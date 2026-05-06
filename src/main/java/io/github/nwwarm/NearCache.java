package io.github.nwwarm;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import jakarta.annotation.Nonnull;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RBucket;
import org.redisson.api.RKeys;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.support.NullValue;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Two-tier cache (Caffeine L1, Redis L2) with topic-based cross-node invalidation.
 *
 * <p>This is the canonical "near-cache" pattern: reads consult L1, fall through
 * to L2, and populate L1 on hit. Writes update both layers and publish an
 * invalidation message; remote nodes evict their L1 copy and lazy-load on next
 * read. The originating node skips its own messages via {@code nodeId}.
 *
 * <h2>Operational properties</h2>
 *
 * <ul>
 *   <li><b>Cross-node coherence on writes:</b> bounded by the latency of pub/sub
 *       delivery (typically &lt; 10ms in a healthy Redis).</li>
 *   <li><b>Single-flight on cold loads:</b> {@link #get(Object, Callable)} acquires
 *       a per-key {@link RLock} so only one node loads from the source.</li>
 *   <li><b>O(1) clear:</b> {@link #clear()} bumps a generation counter rather than
 *       deleting all keys. Old keys orphan and are reclaimed by Redis TTL.</li>
 *   <li><b>Graceful Redis degradation:</b> all L2 calls go through a circuit
 *       breaker. Open breaker → L2 ops are skipped, cache degrades to L1-only.</li>
 * </ul>
 *
 * <h2>Failure semantics</h2>
 *
 * <ul>
 *   <li>L2 read failure → treated as a miss; loader is invoked.</li>
 *   <li>L2 write failure → suppressed; this node's L1 holds a value remote nodes
 *       won't see until their own L1 entry expires. Bounded by TTL.</li>
 *   <li>Pub/sub failure → suppressed; remote nodes serve stale L1 until expiry.</li>
 * </ul>
 *
 * A cache is by definition allowed to lose data. These tradeoffs prioritize
 * availability over coherence during partial Redis outages.
 */
public class NearCache implements HybridCache, InvalidationListener {

    private static final Logger log = LoggerFactory.getLogger(NearCache.class);
    private static final long GENERATION_REFRESH_NANOS = 1_000_000_000L; // 1s

    private final Cache caffeineCache;
    private final String cacheName;
    private final CacheProperties.CacheSpec spec;
    private final RedissonClient redisson;
    private final CircuitBreaker breaker;
    private final InvalidationDispatcher dispatcher;
    private final Codec bucketCodec;
    private final KeyLogFormatter keyLogFormatter;
    private final LoaderGate loaderGate;

    // Generation counter for O(1) clear
    private final RAtomicLong distributedGeneration;
    private final AtomicLong localGeneration = new AtomicLong(0);
    // CAS-guarded: the first thread past the staleness check wins the right
    // to refresh; all others return the cached local generation.
    private final AtomicLong lastRefreshNanos = new AtomicLong(0);

    // Metrics
    private final Counter l2Hits;
    private final Counter l2Misses;
    private final Counter l2Failures;
    private final Counter l2BreakerOpen;
    private final Timer l2GetLatency;
    private final Counter invalidationsSuppressedColdLoad;

    public NearCache(Cache caffeineCache,
                     CacheProperties.CacheSpec spec,
                     Codec bucketCodec,
                     RedissonClient redisson,
                     CircuitBreaker breaker,
                     InvalidationDispatcher dispatcher,
                     MeterRegistry meterRegistry,
                     KeyLogFormatter keyLogFormatter) {
        this.caffeineCache = caffeineCache;
        this.cacheName = caffeineCache.getName();
        this.spec = spec;
        this.redisson = redisson;
        this.breaker = breaker;
        this.dispatcher = dispatcher;
        this.bucketCodec = bucketCodec;
        this.keyLogFormatter = keyLogFormatter;
        this.loaderGate = new LoaderGate(cacheName,
                spec.maxConcurrentLoaders(), spec.loaderAcquireTimeout(), meterRegistry);

        this.distributedGeneration = redisson.getAtomicLong(CacheKeys.generationKey(cacheName));
        initializeGeneration();

        dispatcher.register(this);

        this.l2Hits = Counter.builder("cache.l2.gets")
                .tag("cache", cacheName).tag("result", "hit").register(meterRegistry);
        this.l2Misses = Counter.builder("cache.l2.gets")
                .tag("cache", cacheName).tag("result", "miss").register(meterRegistry);
        this.l2Failures = Counter.builder("cache.l2.failures")
                .tag("cache", cacheName).register(meterRegistry);
        this.l2BreakerOpen = Counter.builder("cache.l2.breaker.open")
                .tag("cache", cacheName).register(meterRegistry);
        this.l2GetLatency = Timer.builder("cache.l2.get.latency")
                .tag("cache", cacheName).register(meterRegistry);
        this.invalidationsSuppressedColdLoad = Counter.builder("cache.invalidations.suppressed.cold_load")
                .tag("cache", cacheName).register(meterRegistry);

        Object native_ = caffeineCache.getNativeCache();
        if (native_ instanceof com.github.benmanes.caffeine.cache.Cache<?, ?> caffeineNative) {
            CaffeineCacheMetrics.monitor(meterRegistry, caffeineNative, cacheName);
        }
    }

    private void initializeGeneration() {
        try {
            breaker.executeRunnable(() -> localGeneration.set(distributedGeneration.get()));
            lastRefreshNanos.set(System.nanoTime());
        } catch (Exception e) {
            log.warn("Failed to initialize generation for cache '{}'; defaulting to 0", cacheName, e);
        }
    }

    // ---------- Spring Cache contract ----------

    @Override
    @Nonnull
    public String getName() {
        return cacheName;
    }

    @Override
    @Nonnull
    public Object getNativeCache() {
        return caffeineCache.getNativeCache();
    }

    @Override
    public ValueWrapper get(@Nonnull Object key) {
        String stringKey = CacheKeys.stringify(cacheName, key);
        ValueWrapper wrapper = caffeineCache.get(stringKey);
        if (wrapper != null) return wrapper;

        Object distValue = readFromL2(stringKey);
        if (distValue == null) return null;

        caffeineCache.put(stringKey, distValue);
        return caffeineCache.get(stringKey);  // delegate so CaffeineCache wraps NullValue correctly
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(@Nonnull Object key, Class<T> type) {
        ValueWrapper wrapper = get(key);
        if (wrapper == null) return null;
        Object value = wrapper.get();
        if (value != null && type != null && !type.isInstance(value)) {
            throw new IllegalStateException(
                    "Cached value [" + value + "] is not of required type [" + type.getName() + "]");
        }
        return (T) value;
    }

    /**
     * Loader-aware get with two-tier single-flight protection.
     *
     * <p>Invoked by Spring's {@code @Cacheable(sync = true)} and by Spring's cache
     * abstraction for some other paths. Two layers of herd protection:
     *
     * <ol>
     *   <li><b>Local single-flight</b> via Caffeine's atomic
     *       {@code Cache.get(key, mappingFunction)}. Guarantees only one thread per
     *       JVM executes the loader for a given key. This protection is unconditional
     *       — it works whether Redis is healthy, slow, or unreachable.</li>
     *   <li><b>Cross-node single-flight</b> via Redisson {@link RLock}. Acquired inside
     *       the local loader function. If Redis is down, lock acquisition fails and
     *       we proceed without it. The local layer alone still prevents per-JVM
     *       pile-up; the worst case is N database loads across N nodes during a
     *       Redis outage, not N × threads-per-node.</li>
     * </ol>
     *
     * <p>Negative caching: if the loader returns {@code null}, the value is stored
     * as {@link NullValue#INSTANCE} so Caffeine's native cache (which does not
     * normally store null) caches the negative result. Subsequent calls return
     * a {@code SimpleValueWrapper} containing null, matching Spring's contract.
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(@Nonnull Object key, @Nonnull Callable<T> valueLoader) {
        String stringKey = CacheKeys.stringify(cacheName, key);

        // Fast path: already cached in L1 or L2.
        ValueWrapper wrapper = caffeineCache.get(stringKey);
        if (wrapper == null) {
            Object distValue = readFromL2(stringKey);
            if (distValue != null) {
                caffeineCache.put(stringKey, distValue);
                wrapper = caffeineCache.get(stringKey);
            }
        }
        if (wrapper != null) return (T) wrapper.get();

        com.github.benmanes.caffeine.cache.Cache<Object, Object> nativeCaffeine =
                (com.github.benmanes.caffeine.cache.Cache<Object, Object>) caffeineCache.getNativeCache();

        try {
            Object stored = nativeCaffeine.get(stringKey, k -> {
                // Caffeine guarantees only one thread per JVM enters this lambda for key k.
                // We may have lost a race with another node since the outer get() — re-check L2.
                Object distValue = readFromL2((String) k);
                if (distValue != null) return distValue;

                Object loaded = loadWithDistributedLock((String) k, valueLoader);
                // Translate null to NullValue so Caffeine actually caches negative results.
                return loaded == null ? NullValue.INSTANCE : loaded;
            });
            return (T) (stored instanceof NullValue ? null : stored);
        } catch (LoaderException e) {
            throw new ValueRetrievalException(key, valueLoader, e.getCause());
        }
    }

    /**
     * Cross-node single-flight via Redis lock. Called from inside Caffeine's
     * compute lambda — the caller is already locally serialized.
     *
     * <p>If lock acquisition fails (Redis unreachable, breaker open, or timeout),
     * we proceed with the load anyway. The local Caffeine compute lock provides
     * the baseline protection.
     */
    private Object loadWithDistributedLock(String key, Callable<?> valueLoader) {
        RLock lock = null;
        boolean acquired = false;
        try {
            lock = redisson.getLock(CacheKeys.lockKey(cacheName, key));
            acquired = lock.tryLock(
                    spec.lockWait().toMillis(),
                    spec.lockLease().toMillis(),
                    TimeUnit.MILLISECONDS);

            if (acquired) {
                // Re-check L2 after acquisition — another node may have just released
                // the lock having populated L2.
                Object distValue = readFromL2(key);
                if (distValue != null) return distValue;
            }
            // If !acquired (Redis slow / breaker open / timeout), fall through.
            // Local Caffeine compute lock still serializes threads on this JVM.
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new LoaderException(e);
        } catch (Exception e) {
            // Redis-side failure during lock acquisition. Local single-flight remains.
            log.debug("Distributed lock acquisition failed for key '{}'; proceeding with local single-flight only", keyLogFormatter.format(key), e);
        }

        try {
            Object value = loaderGate.run(key, valueLoader);
            writeToL2(key, value);
            // Cold-load completion: do NOT publish.
            //
            // Two sub-cases land here:
            //   1. Lock acquired, re-check empty: a true cold load. No peer has
            //      written to L2 yet, so no peer can have a stale L1.
            //   2. Lock acquisition failed (Redis slow, breaker open, tryLock
            //      timeout): we loaded unilaterally. A peer may also be loading
            //      concurrently. If loaders are deterministic, both writes produce
            //      the same value and there's nothing to invalidate. If loaders
            //      are non-deterministic, last-writer-wins on L2 — and a publish
            //      from us would force peers to drop their L1 and reload, only to
            //      potentially get our value or theirs depending on L2 timing.
            //      That's not coherence; it's just churn. Application-level
            //      determinism of loaders is the correct fix for that case, not
            //      cache-level invalidation.
            //
            // In both sub-cases, peer L1 is either empty (lazy-loads on next read
            // from now-warm L2) or holds a value the peer wrote itself. Neither
            // is stale in the sense that requires invalidation.
            //
            // put(...) and evict(...) DO publish — they replace or remove existing
            // state, so remote L1 copies must be invalidated.
            invalidationsSuppressedColdLoad.increment();
            return value;
        } catch (LoaderRejectedException e) {
            // Surface backpressure as itself; do NOT wrap in LoaderException
            // (which the outer get(key, valueLoader) translates into a
            // ValueRetrievalException — the wrong type for callers that want
            // to distinguish "loader rejected" from "loader failed").
            throw e;
        } catch (Throwable t) {
            throw new LoaderException(t);
        } finally {
            if (acquired && lock != null && lock.isHeldByCurrentThread()) {
                try {
                    lock.unlock();
                } catch (Exception e) {
                    log.warn("Failed to unlock cache='{}' key={}", cacheName, keyLogFormatter.format(key), e);
                }
            }
        }
    }

    /** Internal exception so loader failures can propagate through Caffeine's compute. */
    private static class LoaderException extends RuntimeException {
        LoaderException(Throwable cause) { super(cause); }
    }

    @Override
    public void put(@Nonnull Object key, Object value) {
        String stringKey = CacheKeys.stringify(cacheName, key);
        // Order on writes: local first (so this node sees its own write immediately),
        // then remote, then publish so other nodes invalidate and lazy-load.
        caffeineCache.put(stringKey, value);
        writeToL2(stringKey, value);
        publishInvalidation(InvalidationMessage.OP_INVALIDATE, stringKey);
    }

    @Override
    public void evict(@Nonnull Object key) {
        String stringKey = CacheKeys.stringify(cacheName, key);
        // Order: L2 first. If L2 delete fails (breaker open or transient
        // exception), do NOT publish — a publish would tell remote nodes to
        // invalidate when their stale L1 is in fact still consistent with the
        // L2 value that's still present. Always evict the local L1 so this
        // node's L1 ends in a strict-or-equal state vs L2 (cross-node
        // coherence is bounded by TTL when the L2 evict failed).
        boolean l2Deleted = deleteFromL2(stringKey);
        if (l2Deleted) {
            publishInvalidation(InvalidationMessage.OP_INVALIDATE, stringKey);
        }
        caffeineCache.evict(stringKey);
    }

    @Override
    public void clear() {
        caffeineCache.clear();
        bumpGeneration();
        publishInvalidation(InvalidationMessage.OP_CLEAR, null);
    }

    /**
     * Eager clear: bump generation, then SCAN + UNLINK every old-generation
     * value bucket on every reachable shard. Stronger than {@link #clear()} —
     * when this returns, no orphan keys remain in Redis under this cache's
     * prefix. See {@link HybridCache#clearImmediate()} for the rationale.
     *
     * <p>Order is deliberate: the generation bump goes first so concurrent
     * reads switch to the new prefix before any UNLINK runs — the old keys
     * are logically invisible the moment the bump succeeds. Then the
     * invalidation message goes out (peers drop their L1 immediately rather
     * than waiting up to one second for the generation poll). Finally we
     * eagerly UNLINK the old-generation keys via {@link RKeys#unlinkByPattern},
     * which iterates every cluster master and pipelines UNLINK in batches.
     *
     * <p>If the bump fails the exception propagates and no UNLINK is
     * attempted — without a known previous generation we have nothing safe
     * to delete. If the bump succeeds but UNLINK fails mid-iteration the
     * exception still propagates, but the cache is correct: surviving keys
     * are at the old generation and reads will not find them; they expire
     * via TTL just as they would after a regular {@link #clear()}.
     */
    @Override
    public void clearImmediate() {
        caffeineCache.clear();

        long newGen;
        try {
            newGen = breaker.executeSupplier(distributedGeneration::incrementAndGet);
        } catch (RuntimeException e) {
            l2Failures.increment();
            log.warn("clearImmediate generation bump failed for cache '{}'", cacheName, e);
            throw e;
        }
        localGeneration.set(newGen);
        lastRefreshNanos.set(System.nanoTime());
        long oldGen = newGen - 1;

        publishInvalidation(InvalidationMessage.OP_CLEAR, null);

        String pattern = CacheKeys.valueKeyPattern(cacheName, oldGen);
        try {
            redisson.getKeys().unlinkByPattern(pattern);
        } catch (RuntimeException e) {
            l2Failures.increment();
            log.warn("clearImmediate SCAN/UNLINK failed for cache '{}' pattern '{}';"
                    + " surviving old-generation keys will expire via TTL",
                    cacheName, pattern, e);
            throw e;
        }
    }

    // ---------- L2 ops, breaker-wrapped ----------

    private RBucket<Object> bucket(String key) {
        String fullKey = CacheKeys.valueKey(cacheName, key, currentGeneration());
        return bucketCodec == null
                ? redisson.getBucket(fullKey)
                : redisson.getBucket(fullKey, bucketCodec);
    }

    private Object readFromL2(String key) {
        try {
            return breaker.executeSupplier(() -> {
                Timer.Sample sample = Timer.start();
                try {
                    Object value = bucket(key).get();
                    if (value == null) {
                        l2Misses.increment();
                    } else {
                        l2Hits.increment();
                    }
                    return value;
                } finally {
                    sample.stop(l2GetLatency);
                }
            });
        } catch (CallNotPermittedException e) {
            l2BreakerOpen.increment();
            return null;
        } catch (Exception e) {
            l2Failures.increment();
            log.warn("L2 read failed for key '{}'; degrading to local-only", keyLogFormatter.format(key), e);
            return null;
        }
    }

    private void writeToL2(String key, Object value) {
        try {
            breaker.executeRunnable(() -> bucket(key).set(value, spec.ttl()));
        } catch (CallNotPermittedException e) {
            l2BreakerOpen.increment();
            // Local cache holds value; remote nodes won't see it until their TTL.
            // Acceptable for a cache; bounded by TTL.
        } catch (Exception e) {
            l2Failures.increment();
            log.warn("L2 write failed for key '{}'; cross-node incoherence until TTL", keyLogFormatter.format(key), e);
        }
    }

    /**
     * Deletes the L2 entry. Returns whether the delete completed successfully —
     * callers (notably {@link #evict(Object)}) use this to decide whether to
     * publish an invalidation. A failed L2 delete must not publish: telling
     * remote nodes to invalidate would defeat the cross-node coherence
     * guarantee, since their L1 would drop to L2 and find the value still
     * present.
     */
    private boolean deleteFromL2(String key) {
        try {
            breaker.executeRunnable(() -> bucket(key).delete());
            return true;
        } catch (CallNotPermittedException e) {
            l2BreakerOpen.increment();
            log.warn("L2 evict failed for key '{}' (breaker open); cross-node coherence not guaranteed until TTL", keyLogFormatter.format(key));
            return false;
        } catch (Exception e) {
            l2Failures.increment();
            log.warn("L2 evict failed for key '{}'; cross-node coherence not guaranteed until TTL", keyLogFormatter.format(key), e);
            return false;
        }
    }

    // ---------- Generation counter ----------

    private long currentGeneration() {
        long observed = lastRefreshNanos.get();
        long now = System.nanoTime();
        if (now - observed < GENERATION_REFRESH_NANOS) return localGeneration.get();
        // Only the thread that wins the CAS proceeds with the Redis round-trip;
        // all others return the previously-cached generation, which is correct
        // since the refresh hasn't completed yet by definition.
        if (!lastRefreshNanos.compareAndSet(observed, now)) return localGeneration.get();
        try {
            breaker.executeRunnable(() -> localGeneration.set(distributedGeneration.get()));
        } catch (Exception e) {
            // Refresh failed — reset so the next call retries rather than
            // waiting a full interval on stale data. CAS ensures we only
            // reset if nothing else (e.g. bumpGeneration) has written since.
            lastRefreshNanos.compareAndSet(now, observed);
        }
        return localGeneration.get();
    }

    private void bumpGeneration() {
        try {
            Long newGen = breaker.executeSupplier(distributedGeneration::incrementAndGet);
            localGeneration.set(newGen);
            lastRefreshNanos.set(System.nanoTime());
        } catch (Exception e) {
            l2Failures.increment();
            log.warn("Generation bump failed; clear visible only locally on '{}'", cacheName, e);
        }
    }

    // ---------- Pub/sub ----------

    private void publishInvalidation(String op, String key) {
        try {
            breaker.executeRunnable(() ->
                    dispatcher.publish(new InvalidationMessage(dispatcher.getNodeId(), cacheName, op, key)));
        } catch (Exception e) {
            // Suppressed — invalidation failure is bounded by TTL on remote nodes.
        }
    }

    /** Invoked by {@link InvalidationDispatcher} after self-skip and name routing. */
    @Override
    public void handleInvalidation(String op, String key) {
        switch (op) {
            case InvalidationMessage.OP_INVALIDATE -> caffeineCache.evict(key);
            case InvalidationMessage.OP_CLEAR -> {
                caffeineCache.clear();
                lastRefreshNanos.set(0);  // force generation refresh on next read
                currentGeneration();
            }
            default -> log.warn("Unknown invalidation op '{}' on cache '{}'", op, cacheName);
        }
    }

    // ---------- Lifecycle ----------

    /** Called by the resolver during context shutdown. Idempotent. */
    public void shutdown() {
        dispatcher.deregister(cacheName);
    }

    /** Exposed for diagnostic endpoints; not part of the {@link Cache} contract. */
    public CacheStats nativeStats() {
        if (caffeineCache.getNativeCache() instanceof com.github.benmanes.caffeine.cache.Cache<?, ?> native_) {
            return native_.stats();
        }
        return CacheStats.empty();
    }

    void forceRefreshDue() {
        lastRefreshNanos.set(0);
    }

    CircuitBreaker getBreaker() {
        return breaker;
    }
}
