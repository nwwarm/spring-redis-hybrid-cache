package io.github.nwwarm;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.api.listener.MessageListener;
import org.redisson.client.codec.Codec;
import org.redisson.codec.TypedJsonJacksonCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Single per-node Redis pub/sub subscription that fans invalidation messages
 * out to the right {@link NearCache}. Replaces N listeners across N caches
 * with one listener plus an in-memory routing table.
 *
 * <p>Self-skip and cache-name routing happen here, so individual caches don't
 * need to repeat those checks. The dispatcher is also the single source of
 * truth for {@code nodeId} — caches read it from here when publishing.
 */
public class InvalidationDispatcher implements MessageListener<InvalidationMessage> {

    private static final Logger log = LoggerFactory.getLogger(InvalidationDispatcher.class);
    private static final String INVALIDATION_TOPIC = "cache:invalidate";

    // Dedicated codec for the invalidation topic. The global JsonJacksonCodec
    // activates NON_FINAL default typing on its internal mapper, but reads via
    // Object.class — which then requires a top-level @class on every payload.
    // InvalidationMessage is a record (implicitly final), so default typing
    // skips the @class on serialize, and the decoder fails on read. Pinning the
    // topic to a typed codec sidesteps default typing entirely for pubsub.
    private static final Codec INVALIDATION_CODEC = new TypedJsonJacksonCodec(InvalidationMessage.class);

    private final String nodeId;
    private final RTopic topic;
    private final int listenerId;
    private final ConcurrentMap<String, InvalidationListener> caches = new ConcurrentHashMap<>();
    private final MeterRegistry meterRegistry;

    InvalidationDispatcher(RedissonClient redisson, String nodeId, MeterRegistry meterRegistry) {
        this.nodeId = nodeId;
        this.meterRegistry = meterRegistry;
        this.topic = redisson.getTopic(INVALIDATION_TOPIC, INVALIDATION_CODEC);
        this.listenerId = topic.addListener(InvalidationMessage.class, this);
    }

    String getNodeId() {
        return nodeId;
    }

    void register(InvalidationListener cache) {
        InvalidationListener prev = caches.put(cache.getName(), cache);
        if (prev != null && prev != cache) {
            log.warn("Replacing existing cache registration for '{}'", cache.getName());
        }
    }

    void deregister(String cacheName) {
        caches.remove(cacheName);
    }

    void publish(InvalidationMessage message) {
        topic.publish(message);
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
        cache.handleInvalidation(msg.op(), msg.key());
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

    @PreDestroy
    void shutdown() {
        try {
            topic.removeListener(listenerId);
        } catch (Exception e) {
            log.warn("Failed to remove invalidation listener", e);
        }
        caches.clear();
    }
}
