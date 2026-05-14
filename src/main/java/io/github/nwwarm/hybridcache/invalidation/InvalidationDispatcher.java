package io.github.nwwarm.hybridcache.invalidation;

import io.github.nwwarm.hybridcache.core.NearCache;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.api.listener.MessageListener;
import org.redisson.client.codec.Codec;
import org.redisson.codec.TypedJsonJacksonCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Single per-node Redis pub/sub subscription that fans invalidation messages
 * out to the right {@link NearCache}. Replaces N listeners across N caches
 * with one listener plus an in-memory routing table.
 *
 * <p>Self-skip and cache-name routing happen here, so individual caches don't
 * need to repeat those checks. The dispatcher is also the single source of
 * truth for {@code nodeId} — caches read it from here when publishing.
 *
 * <h2>Lifecycle</h2>
 *
 * <p>Implements {@link SmartLifecycle} so Spring drives shutdown ordering
 * deterministically. Spring's {@code LifecycleProcessor.onClose()} runs
 * {@link #stop()} <em>before</em> the bean-destruction phase that disposes
 * {@link RedissonClient}. The dispatcher therefore unsubscribes from the
 * Redis topic while Redisson is still alive — without this contract the
 * ordering depended on bean-graph traversal, which is correct in current
 * Spring versions but not formally guaranteed.
 *
 * <p>{@link RedissonClient} is not itself a {@code SmartLifecycle}; it is
 * destroyed via its bean {@code destroyMethod}. {@link #getPhase()} is
 * therefore irrelevant to the Redisson ordering specifically — that ordering
 * is enforced by the lifecycle-vs-destruction split — but a deliberately
 * high phase ({@link #LIFECYCLE_PHASE}) places this dispatcher near the very
 * end of any chain of {@code SmartLifecycle} stops, leaving 1024 phases of
 * headroom below {@code Integer.MAX_VALUE} for users who legitimately need
 * to stop strictly after the cache layer.
 */
public class InvalidationDispatcher implements MessageListener<InvalidationMessage>, SmartLifecycle {

    /**
     * SmartLifecycle phase. Spring stops phases in <em>descending</em> order,
     * so a higher number stops earlier. {@code Integer.MAX_VALUE - 1024} is
     * almost the highest possible value (early stop), with 1024 phases of
     * headroom for any user-supplied {@code SmartLifecycle} that genuinely
     * needs to stop after the cache layer.
     */
    public static final int LIFECYCLE_PHASE = Integer.MAX_VALUE - 1024;

    private static final Logger log = LoggerFactory.getLogger(InvalidationDispatcher.class);
    private static final String INVALIDATION_TOPIC = "cache:invalidate";

    // Dedicated codec for the invalidation topic. The global JsonJacksonCodec
    // activates NON_FINAL default typing on its internal mapper, but reads via
    // Object.class — which then requires a top-level @class on every payload.
    // InvalidationMessage is a record (implicitly final), so default typing
    // skips the @class on serialize, and the decoder fails on read. Pinning the
    // topic to a typed codec sidesteps default typing entirely for pubsub.
    private static final Codec INVALIDATION_CODEC = new TypedJsonJacksonCodec(InvalidationMessage.class);

    private final RedissonClient redisson;
    private final String nodeId;
    private final RTopic topic;
    private final ConcurrentMap<String, InvalidationListener> caches = new ConcurrentHashMap<>();
    private final MeterRegistry meterRegistry;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile Integer listenerId;
    /**
     * Captured at the moment {@link #stop()} runs: was Redisson still alive?
     * Diagnostic field — useful both for ordering verification in tests and
     * for production troubleshooting when a deployment shuts beans down in
     * the wrong order. {@code true} means correct ordering (dispatcher
     * stopped first); {@code false} means Redisson was already gone.
     */
    private volatile Boolean redissonAliveAtStop;

    public InvalidationDispatcher(RedissonClient redisson, String nodeId, MeterRegistry meterRegistry) {
        this(redisson, nodeId, meterRegistry, false);
    }

    /**
     * @param shardedPubsub when {@code true}, subscribes via Redisson's
     *        {@link org.redisson.api.RShardedTopic} ({@code SPUBLISH}/{@code
     *        SSUBSCRIBE}, Redis 7.0+) instead of {@link RTopic}. Cluster-only —
     *        validated upstream by {@code CacheSpecValidator}. The wire format
     *        and routing logic are unchanged; only the channel-to-shard
     *        binding differs.
     */
    public InvalidationDispatcher(RedissonClient redisson, String nodeId,
                           MeterRegistry meterRegistry, boolean shardedPubsub) {
        this.redisson = redisson;
        this.nodeId = nodeId;
        this.meterRegistry = meterRegistry;
        // RShardedTopic extends RTopic, so we can hold either via the same
        // field type. Subscribe/publish/listener-id semantics are identical;
        // the only on-the-wire difference is SPUBLISH vs PUBLISH.
        this.topic = shardedPubsub
                ? redisson.getShardedTopic(INVALIDATION_TOPIC, INVALIDATION_CODEC)
                : redisson.getTopic(INVALIDATION_TOPIC, INVALIDATION_CODEC);
        // Subscribe eagerly. Direct construction in tests then works without a
        // separate start() call; Spring's later start() during refresh is a
        // no-op because isRunning() is already true. Stop semantics are
        // unaffected — Spring still calls stop() during
        // LifecycleProcessor.onClose() before the bean-destruction phase,
        // which is the ordering guarantee we actually need.
        start();
    }

    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            this.listenerId = topic.addListener(InvalidationMessage.class, this);
        }
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        // Snapshot Redisson liveness BEFORE we make any further calls — the
        // act of removing the listener could itself observe a half-shutdown
        // state, and we want the field to reflect "was Redisson up when we
        // were asked to stop", not "did remove succeed".
        boolean alive = !redisson.isShutdown();
        this.redissonAliveAtStop = alive;
        if (!alive) {
            log.warn("InvalidationDispatcher.stop ran AFTER RedissonClient was shut down "
                    + "— bean ordering violated. Phase={}; check whether Redisson was "
                    + "destroyed by non-Spring code or downgraded to a SmartLifecycle "
                    + "with a higher phase.", LIFECYCLE_PHASE);
        }
        Integer id = listenerId;
        listenerId = null;
        if (id != null) {
            try {
                topic.removeListener(id);
            } catch (Exception e) {
                // Failure here is bounded — the bean is going down anyway.
                // Log so an inverted shutdown order is still diagnosable.
                log.warn("Failed to remove invalidation listener on stop", e);
            }
        }
        caches.clear();
    }

    @Override
    public void stop(Runnable callback) {
        try {
            stop();
        } finally {
            // Always invoke the callback, even if stop threw — Spring's
            // LifecycleProcessor uses the callback as the synchronization
            // signal that this bean is done; failing to invoke it would
            // leave the processor waiting on its CountDownLatch forever.
            callback.run();
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public int getPhase() {
        return LIFECYCLE_PHASE;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void register(InvalidationListener cache) {
        InvalidationListener prev = caches.put(cache.getName(), cache);
        if (prev != null && prev != cache) {
            log.warn("Replacing existing cache registration for '{}'", cache.getName());
        }
    }

    public void deregister(String cacheName) {
        caches.remove(cacheName);
    }

    public void publish(InvalidationMessage message) {
        topic.publish(message);
    }

    /**
     * Async publish used by the cache layer's hot write paths (put / evict /
     * clear) so the sync {@link RTopic#publish(Object)} call is never made
     * from a Redisson Netty event-loop thread. Returns the underlying
     * Redisson future's count of receivers; callers compose this into
     * their async chain via {@code thenApply} / {@code handle}.
     */
    public java.util.concurrent.CompletionStage<Long> publishAsync(InvalidationMessage message) {
        return topic.publishAsync(message).toCompletableFuture();
    }

    @Override
    public void onMessage(CharSequence channel, InvalidationMessage msg) {
        if (nodeId.equals(msg.nodeId())) return;
        InvalidationListener cache = caches.get(msg.cacheName());
        if (cache == null) {
            // Forged or deployment drift — a peer published for a cache we
            // don't host. Tracked separately so a real "we missed an
            // invalidation" alert isn't drowned out by configuration drift.
            // Tag cardinality is bounded by the set of misconfigured cache
            // names, which in healthy deployments is empty.
            unknownCounter(msg.cacheName(), msg.op()).increment();
            return;
        }
        receivedCounter(msg.cacheName(), msg.op()).increment();
        // seq is forwarded unconditionally; listeners that don't participate
        // in reconciliation ignore it. 0 is the "no seq" sentinel (publisher
        // has reconciliation disabled, or message came from a 0.4.0 node).
        cache.handleInvalidation(msg.op(), msg.key(), msg.seq());
    }

    /**
     * Diagnostic accessor for shutdown-ordering verification. Returns the
     * value of {@code !redisson.isShutdown()} captured during the most
     * recent {@link #stop()} call, or {@code null} if {@code stop()} has
     * never run. Package-private so tests can assert ordering after
     * context close without exposing this on the public surface.
     */
    Boolean wasRedissonAliveAtStop() {
        return redissonAliveAtStop;
    }

    private Counter receivedCounter(String cacheName, String wireOp) {
        return Counter.builder("cache.invalidations.received")
                .tag("cache", cacheName)
                .tag("op", metricOpFor(wireOp))
                .register(meterRegistry);
    }

    private Counter unknownCounter(String cacheName, String wireOp) {
        return Counter.builder("cache.invalidations.received.unknown")
                .tag("cache", cacheName)
                .tag("op", metricOpFor(wireOp))
                .register(meterRegistry);
    }

    /**
     * Translates the wire op to a metric tag value. The wire format only
     * distinguishes "invalidate" (used by both put and evict) from "clear",
     * so the receiver cannot tag {@code put}/{@code evict} the way the
     * publisher does — a single label "invalidate" covers both. Unknown
     * wire codes are surfaced as-is so dashboards can flag protocol drift.
     */
    private static String metricOpFor(String wireOp) {
        return switch (wireOp) {
            case InvalidationMessage.OP_INVALIDATE -> "invalidate";
            case InvalidationMessage.OP_CLEAR -> "clear";
            default -> wireOp;
        };
    }
}
