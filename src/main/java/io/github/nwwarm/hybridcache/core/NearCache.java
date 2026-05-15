package io.github.nwwarm.hybridcache.core;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.github.nwwarm.hybridcache.config.CacheProperties;
import io.github.nwwarm.hybridcache.invalidation.InvalidationDispatcher;
import io.github.nwwarm.hybridcache.invalidation.InvalidationListener;
import io.github.nwwarm.hybridcache.invalidation.InvalidationMessage;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import jakarta.annotation.Nonnull;
import org.redisson.api.*;
import org.redisson.client.codec.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.support.NullValue;
import jakarta.annotation.Nullable;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

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
public class NearCache implements HybridCache, InvalidationListener, Reconciler {

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
    /** Async-path loader gate (0.5.0). Constructed unconditionally; short-circuits when {@code max-concurrent-loaders} is unset. */
    private final AsyncLoaderGate asyncLoaderGate;
    /**
     * Per-JVM single-flight map for the async path (0.5.0). Distinct from
     * the Caffeine-backed sync single-flight (which uses Caffeine's atomic
     * {@code get(key, mappingFunction)}); the async path needs an
     * explicit map because Caffeine's atomic compute blocks the caller —
     * unacceptable on the async/reactive path. § 10 / guardrail item 5
     * pins both sync and async to one helper across all tiers; on
     * {@link NearCache} that's two physical structures (Caffeine atomic
     * for sync, this map for async) with the same logical contract:
     * "first inserter runs the loader; the rest wait."
     */
    private final ConcurrentMap<String, CompletableFuture<Object>> asyncInflight =
            new ConcurrentHashMap<>();
    /** Refresh executor for the mandatory loader hop on the async path (guardrail item 2). */
    @Nullable private final Executor refreshExecutorRef;
    private final MeterRegistry meterRegistry;
    /** Non-null only when {@code spec.swr() != null} — SWR opts in via configuration. */
    private final SwrSidecar swrSidecar;
    /**
     * Non-null only when {@code spec.refreshAhead() != null && spec.refreshAhead().enabled()}.
     * RA requires SWR (the validator enforces this); when this field is
     * non-null, {@link #swrSidecar} is also non-null.
     */
    private final RefreshAheadCoordinator refreshAhead;
    /**
     * Per-cache EWMA of measured loader durations. Non-null only when SWR
     * or RA is configured — the EWMA is owned by RA but updated from the
     * SWR refresh path too so RA's predicate sees a current estimate even
     * when only SWR-triggered refreshes have run. Null when neither
     * feature is configured (loader timing is otherwise observed by the
     * existing latency timer).
     */
    private final LoaderRuntimeEwma loaderEwma;

    // Generation counter for O(1) clear
    private final RAtomicLong distributedGeneration;
    private final AtomicLong localGeneration = new AtomicLong(0);
    // CAS-guarded: the first thread past the staleness check wins the right
    // to refresh; all others return the cached local generation.
    private final AtomicLong lastRefreshNanos = new AtomicLong(0);

    // Reconciliation (0.5.0). distributedSeq is null when reconciliation is
    // disabled for this cache; reconciliationEnabled gates the INCR-and-carry
    // path on publish so disabled caches pay zero cost (no Redis op, seq=0
    // on the wire, receivers see no advance).
    private final boolean reconciliationEnabled;
    private final RAtomicLong distributedSeq;
    private final AtomicLong lastObservedSeq = new AtomicLong(0);

    // Metrics
    private final Counter l2Hits;
    private final Counter l2Misses;
    private final Counter l2Failures;
    private final Counter l2BreakerOpen;
    private final Timer l2GetLatency;
    private final Counter invalidationsSuppressedColdLoad;
    // cache.invalidations.published{cache, op} — counts only successful
    // RTopic.publish() calls. The published-vs-received delta on a healthy
    // cluster should be (sources × peers); a divergence flags pub/sub or
    // breaker issues. Three pre-registered counters because tag values are
    // a small fixed set; pre-registration avoids per-publish lookup.
    private final Counter publishedPut;
    private final Counter publishedEvict;
    private final Counter publishedClear;

    public NearCache(Cache caffeineCache,
                     CacheProperties.CacheSpec spec,
                     Codec bucketCodec,
                     RedissonClient redisson,
                     CircuitBreaker breaker,
                     InvalidationDispatcher dispatcher,
                     MeterRegistry meterRegistry,
                     KeyLogFormatter keyLogFormatter) {
        this(caffeineCache, spec, bucketCodec, redisson, breaker, dispatcher,
                meterRegistry, keyLogFormatter, null);
    }

    /**
     * Constructor with SWR support (0.5.0). The {@code swrSidecar} may
     * be {@code null} when {@code spec.swr() == null}; the caller
     * ({@link io.github.nwwarm.hybridcache.core.HybridCacheManager})
     * builds the sidecar before this constructor so the Caffeine
     * {@code RemovalListener} can wire {@link SwrSidecar#onRemoval(String)}
     * directly.
     */
    public NearCache(Cache caffeineCache,
                     CacheProperties.CacheSpec spec,
                     Codec bucketCodec,
                     RedissonClient redisson,
                     CircuitBreaker breaker,
                     InvalidationDispatcher dispatcher,
                     MeterRegistry meterRegistry,
                     KeyLogFormatter keyLogFormatter,
                     SwrSidecar swrSidecar) {
        this(caffeineCache, spec, bucketCodec, redisson, breaker, dispatcher,
                meterRegistry, keyLogFormatter, swrSidecar, null, null);
    }

    /**
     * Full constructor with SWR + RA support (0.5.0). The {@code refreshAhead}
     * coordinator is non-null only when
     * {@code spec.refreshAhead().enabled()}, and in that case {@code swrSidecar}
     * MUST also be non-null (validator E38 enforces RA-requires-SWR at
     * boot). The {@code loaderEwma} parameter is the per-cache estimator
     * shared between SWR and RA refresh paths — both increment it on
     * loader completion so RA's predicate sees a current estimate even
     * when only SWR has fired so far.
     */
    public NearCache(Cache caffeineCache,
                     CacheProperties.CacheSpec spec,
                     Codec bucketCodec,
                     RedissonClient redisson,
                     CircuitBreaker breaker,
                     InvalidationDispatcher dispatcher,
                     MeterRegistry meterRegistry,
                     KeyLogFormatter keyLogFormatter,
                     SwrSidecar swrSidecar,
                     RefreshAheadCoordinator refreshAhead,
                     LoaderRuntimeEwma loaderEwma) {
        this(caffeineCache, spec, bucketCodec, redisson, breaker, dispatcher,
                meterRegistry, keyLogFormatter, swrSidecar, refreshAhead, loaderEwma, null);
    }

    /**
     * Async-aware constructor (0.5.0). The {@code refreshExecutor} is the
     * shared executor the async path uses for the loader hop off
     * Redisson's Netty event loop (guardrail item 2). When null
     * (legacy / test path), the async loader runs on
     * {@link ForkJoinPool#commonPool()} — same execution shape, slightly
     * less control over thread name.
     */
    public NearCache(Cache caffeineCache,
                     CacheProperties.CacheSpec spec,
                     Codec bucketCodec,
                     RedissonClient redisson,
                     CircuitBreaker breaker,
                     InvalidationDispatcher dispatcher,
                     MeterRegistry meterRegistry,
                     KeyLogFormatter keyLogFormatter,
                     SwrSidecar swrSidecar,
                     RefreshAheadCoordinator refreshAhead,
                     LoaderRuntimeEwma loaderEwma,
                     @Nullable Executor refreshExecutor) {
        this.caffeineCache = caffeineCache;
        this.cacheName = caffeineCache.getName();
        this.spec = spec;
        this.redisson = redisson;
        this.breaker = breaker;
        this.dispatcher = dispatcher;
        this.bucketCodec = bucketCodec;
        this.keyLogFormatter = keyLogFormatter;
        this.meterRegistry = meterRegistry;
        this.loaderGate = new LoaderGate(cacheName,
                spec.maxConcurrentLoaders(), spec.loaderAcquireTimeout(), meterRegistry);
        this.asyncLoaderGate = new AsyncLoaderGate(cacheName,
                spec.maxConcurrentLoaders(), spec.loaderAcquireTimeout(), meterRegistry);
        this.refreshExecutorRef = refreshExecutor;
        this.swrSidecar = swrSidecar;
        this.refreshAhead = refreshAhead;
        this.loaderEwma = loaderEwma;

        this.distributedGeneration = redisson.getAtomicLong(CacheKeys.generationKey(cacheName));
        initializeGeneration();

        // Reconciliation: bind the per-cache <cache>:seq counter only when
        // enabled. The Redisson handle is cheap to obtain (no IO until first
        // op), so even when disabled keeping the field non-null would not
        // cost much — but the boolean is what controls the INCR-and-carry
        // hot path on every publish, and a clean null on disabled gives the
        // metrics binding a single source of truth.
        this.reconciliationEnabled = spec.reconciliation() != null
                && spec.reconciliation().enabled();
        this.distributedSeq = reconciliationEnabled
                ? redisson.getAtomicLong(CacheKeys.seqKey(cacheName))
                : null;

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
        this.publishedPut = Counter.builder("cache.invalidations.published")
                .tag("cache", cacheName).tag("op", "put").register(meterRegistry);
        this.publishedEvict = Counter.builder("cache.invalidations.published")
                .tag("cache", cacheName).tag("op", "evict").register(meterRegistry);
        this.publishedClear = Counter.builder("cache.invalidations.published")
                .tag("cache", cacheName).tag("op", "clear").register(meterRegistry);

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
        if (wrapper != null) {
            // SWR classification fires only on L1 hits — past stale-until,
            // Caffeine has physically evicted and we never reach this branch.
            // Plain get(key) (no loader) cannot dispatch a refresh — there
            // is nothing to call. Return the wrapper regardless of fresh /
            // stale; the next loader-aware caller will trigger refresh.
            return wrapper;
        }

        Object distValue = readFromL2(stringKey);
        if (distValue == null) return null;

        // Sidecar must be written BEFORE the value becomes visible —
        // guardrail item 2. A reader landing between the put and the
        // sidecar write would see the value with no fresh deadline,
        // classify as stale, and dispatch a redundant refresh.
        if (swrSidecar != null) swrSidecar.recordWrite(stringKey);
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
        if (wrapper != null) {
            // L1 hit — branch on SWR classification. Inside the fresh
            // window, return synchronously and skip the rest of the
            // method. Inside the stale window, return synchronously and
            // dispatch an async refresh for next time.
            maybeFireSwrRefresh(stringKey, valueLoader);
            return (T) wrapper.get();
        }
        Object distValue = readFromL2(stringKey);
        if (distValue != null) {
            // Sidecar write before put — guardrail item 2.
            if (swrSidecar != null) swrSidecar.recordWrite(stringKey);
            caffeineCache.put(stringKey, distValue);
            wrapper = caffeineCache.get(stringKey);
        }
        if (wrapper != null) return (T) wrapper.get();

        com.github.benmanes.caffeine.cache.Cache<Object, Object> nativeCaffeine =
                (com.github.benmanes.caffeine.cache.Cache<Object, Object>) caffeineCache.getNativeCache();

        try {
            Object stored = nativeCaffeine.get(stringKey, k -> {
                // Caffeine guarantees only one thread per JVM enters this lambda for key k.
                // We may have lost a race with another node since the outer get() — re-check L2.
                Object recheck = readFromL2((String) k);
                if (recheck != null) {
                    // Sidecar deadline pre-write before Caffeine's compute
                    // returns and the value becomes visible (guardrail #2).
                    if (swrSidecar != null) swrSidecar.recordWrite((String) k);
                    return recheck;
                }

                Object loaded = loadWithDistributedLock((String) k, valueLoader);
                if (swrSidecar != null) swrSidecar.recordWrite((String) k);
                // Translate null to NullValue so Caffeine actually caches negative results.
                return loaded == null ? NullValue.INSTANCE : loaded;
            });
            return (T) (stored instanceof NullValue ? null : stored);
        } catch (LoaderException e) {
            throw new ValueRetrievalException(key, valueLoader, e.getCause());
        }
    }

    /**
     * SWR / RA helper: classifies the L1-hit wrapper against the dual
     * deadlines and dispatches a refresh on stale (SWR) or speculatively
     * inside the fresh window (RA). Loader-aware — only the
     * {@code get(key, valueLoader)} path uses this; plain {@code get(key)}
     * has no loader to call.
     *
     * <p>The refresh task body is shared between SWR and RA (per
     * § 10 0.5.0: "RA = SWR with a probabilistic trigger"): same loader
     * gate, same write path through {@link #put(Object, Object)}, same
     * EWMA timing recording. Whichever feature fires first inserts into
     * the in-flight map; the other sees the slot and increments its
     * respective de-dup counter without burning an executor slot.
     */
    private <T> void maybeFireSwrRefresh(String stringKey, Callable<T> valueLoader) {
        if (swrSidecar == null) return;
        SwrSidecar.Classification classification = swrSidecar.classify(stringKey);
        Callable<Object> refreshTask = () -> {
            // loaderGate.run guarantees the per-cache concurrency cap is
            // respected for refresh-driven loads — guardrail item 5. Going
            // through put() (rather than caffeineCache.put directly)
            // ensures L2 + invalidation propagate to peers (guardrail #6).
            // Time the loader so the per-cache EWMA tracks load behavior
            // for RA's XFetch predicate.
            long start = System.nanoTime();
            try {
                Object value = loaderGate.run(stringKey, valueLoader);
                put(stringKey, value);
                return value;
            } finally {
                if (loaderEwma != null) {
                    loaderEwma.record(System.nanoTime() - start);
                }
            }
        };
        if (classification == SwrSidecar.Classification.STALE) {
            swrSidecar.maybeDispatchRefresh(stringKey, refreshTask);
            return;
        }
        // FRESH classification: RA may still fire the loader speculatively
        // via XFetch. RA is enabled only when the coordinator is non-null
        // (validator-checked: RA requires SWR, hence both fields are
        // populated together).
        if (refreshAhead != null) {
            refreshAhead.evaluateAndMaybeDispatch(stringKey, refreshTask);
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

        long loaderStart = System.nanoTime();
        try {
            Object value = loaderGate.run(key, valueLoader);
            if (loaderEwma != null) {
                loaderEwma.record(System.nanoTime() - loaderStart);
            }
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
        // SWR sidecar write goes BEFORE the value becomes visible to readers
        // (guardrail item 2). A reader landing between caffeineCache.put and
        // a subsequent sidecar write would see the new value with no fresh
        // deadline, classify as stale, and dispatch a refresh against a value
        // that was just written and is fresh by definition.
        if (swrSidecar != null) swrSidecar.recordWrite(stringKey);
        // Order on writes: local first (so this node sees its own write immediately),
        // then remote, then publish so other nodes invalidate and lazy-load.
        caffeineCache.put(stringKey, value);
        // L2 and publish go through async Redisson APIs end-to-end —
        // a sync RBucket.set / RTopic.publish here would throw
        // IllegalStateException("Sync methods can't be invoked from
        // async/rx/reactive listeners") when put() runs on a Redisson
        // Netty event-loop thread (Spring's reactive cache integration
        // drives put() from a CompletionStage continuation; under load
        // that continuation can run on the I/O thread). On non-event-loop
        // threads we await the chain to preserve the 1.0.0 sync contract
        // — callers (and many existing tests) expect put() to be
        // observable on this node before it returns. Publish is chained
        // after L2 write so peers reading on invalidate see the new value.
        awaitUnlessOnEventLoop(
                writeToL2Async(stringKey, value)
                        .thenCompose(v -> publishInvalidationAsync(
                                InvalidationMessage.OP_INVALIDATE, "put", stringKey)));
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
        //
        // Async chain for the same reason put() is async — see put() comment.
        awaitUnlessOnEventLoop(
                deleteFromL2Async(stringKey)
                        .thenCompose(l2Deleted -> {
                            if (Boolean.TRUE.equals(l2Deleted)) {
                                return publishInvalidationAsync(
                                        InvalidationMessage.OP_INVALIDATE, "evict", stringKey);
                            }
                            return CompletableFuture.completedFuture(null);
                        }));
        caffeineCache.evict(stringKey);
    }

    @Override
    public void clear() {
        caffeineCache.clear();
        // Async bump + publish — clear() is reactive-reachable via
        // @CacheEvict(allEntries=true) on Mono-returning methods. See
        // put() for the await-vs-fire-and-forget rationale.
        awaitUnlessOnEventLoop(
                bumpGenerationAsync()
                        .thenCompose(v -> publishInvalidationAsync(
                                InvalidationMessage.OP_CLEAR, "clear", null)));
    }

    /**
     * Block on the async chain when invoked from a regular worker thread
     * (preserving the 1.0.0 sync contract on put / evict / clear), but
     * leave it running fire-and-forget when invoked from a Redisson
     * Netty event-loop thread. A {@code .join()} on the event loop would
     * deadlock — the I/O thread must remain available to drive the
     * Redis response that completes the future we'd be waiting on. The
     * thread-name check matches the same threads {@code AsyncBlockHoundIT}
     * marks as non-blocking (Netty I/O workers spawned by Redisson).
     *
     * <p>All call sites compose chains whose handlers swallow their
     * own failures and complete the future normally, so {@code join()}
     * here cannot throw.
     */
    private static void awaitUnlessOnEventLoop(CompletableFuture<?> chain) {
        if (isOnRedissonEventLoop()) return;
        chain.join();
    }

    private static boolean isOnRedissonEventLoop() {
        String name = Thread.currentThread().getName();
        return name.startsWith("redisson-netty-") || name.startsWith("nioEventLoopGroup-");
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

        // Async chain end-to-end: incrementAndGetAsync → set local state →
        // publish OP_CLEAR → unlinkByPatternAsync. Reactive-reachable via
        // @CacheEvict(allEntries=true) on a Mono-returning method whose
        // preceding stage completed on the I/O thread; a sync incrementAndGet
        // / unlinkByPattern there would throw IllegalStateException
        // ("Sync methods can't be invoked from async/rx/reactive listeners").
        // awaitUnlessOnEventLoop preserves the "no orphan keys when this
        // returns" contract on regular worker threads (most callers, all
        // existing tests). Event-loop callers get fire-and-forget — joining
        // would deadlock the Redis response that completes the chain.
        Supplier<CompletionStage<Long>> incrSupplier = () ->
                distributedGeneration.incrementAndGetAsync().toCompletableFuture();
        CompletableFuture<?> chain = breaker.executeCompletionStage(incrSupplier).toCompletableFuture()
                .thenCompose(newGen -> {
                    // Local state advances AFTER the bump lands so a peer
                    // node never observes localGeneration pointing past a
                    // not-yet-persisted Redis value.
                    localGeneration.set(newGen);
                    lastRefreshNanos.set(System.nanoTime());
                    long oldGen = newGen - 1;
                    publishInvalidationAsync(InvalidationMessage.OP_CLEAR, "clear", null);
                    String pattern = CacheKeys.valueKeyPattern(cacheName, oldGen);
                    return redisson.getKeys().unlinkByPatternAsync(pattern)
                            .toCompletableFuture()
                            .handle((deleted, unlinkEx) -> {
                                if (unlinkEx != null) {
                                    Throwable cause = unwrapCompletion(unlinkEx);
                                    l2Failures.increment();
                                    log.warn("clearImmediate SCAN/UNLINK failed for"
                                            + " cache '{}' pattern '{}'; surviving"
                                            + " old-generation keys will expire via TTL",
                                            cacheName, pattern, cause);
                                }
                                return null;
                            });
                })
                .exceptionally(bumpEx -> {
                    Throwable cause = unwrapCompletion(bumpEx);
                    if (cause instanceof CallNotPermittedException) {
                        l2BreakerOpen.increment();
                    } else {
                        l2Failures.increment();
                        log.warn("clearImmediate generation bump failed for cache '{}'",
                                cacheName, cause);
                    }
                    return null;
                });

        awaitUnlessOnEventLoop(chain);
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
     * Async L2 delete used by {@link #evict(Object)}. Returns
     * {@code TRUE} on a successful delete, {@code FALSE} otherwise — the
     * caller uses this to decide whether to publish an invalidation. A
     * failed L2 delete must not publish: telling remote nodes to
     * invalidate would defeat the cross-node coherence guarantee, since
     * their L1 would drop to L2 and find the value still present.
     *
     * <p>The returned future always completes normally (never
     * exceptionally) — failures are caught, metered, and logged so the
     * caller's {@code thenCompose} runs unconditionally.
     */
    private CompletableFuture<Boolean> deleteFromL2Async(String key) {
        Supplier<CompletionStage<Boolean>> deleteSupplier = () ->
                bucket(key).deleteAsync().toCompletableFuture()
                        .thenApply(deleted -> Boolean.TRUE);
        try {
            return breaker.executeCompletionStage(deleteSupplier).toCompletableFuture()
                    .handle((v, ex) -> {
                        if (ex == null) return v;
                        Throwable cause = unwrapCompletion(ex);
                        if (cause instanceof CallNotPermittedException) {
                            l2BreakerOpen.increment();
                            log.warn("L2 evict failed for key '{}' (breaker open);"
                                    + " cross-node coherence not guaranteed until TTL",
                                    keyLogFormatter.format(key));
                        } else {
                            l2Failures.increment();
                            log.warn("L2 evict failed for key '{}'; cross-node coherence"
                                    + " not guaranteed until TTL",
                                    keyLogFormatter.format(key), cause);
                        }
                        return Boolean.FALSE;
                    });
        } catch (CallNotPermittedException e) {
            l2BreakerOpen.increment();
            log.warn("L2 evict failed for key '{}' (breaker open); cross-node coherence"
                    + " not guaranteed until TTL", keyLogFormatter.format(key));
            return CompletableFuture.completedFuture(Boolean.FALSE);
        }
    }

    // ---------- Generation counter ----------

    /**
     * Package-private accessor for the locally cached generation. Used by
     * {@code NearCachePreloader} to stamp the snapshot header without
     * paying for a fresh Redis round-trip on every store. Returns the
     * exact value {@link #currentGeneration()} would return without
     * triggering its refresh-due path.
     */
    public long localGeneration() {
        return localGeneration.get();
    }

    private long currentGeneration() {
        long observed = lastRefreshNanos.get();
        long now = System.nanoTime();
        if (now - observed < GENERATION_REFRESH_NANOS) return localGeneration.get();
        // Only the thread that wins the CAS proceeds with the Redis round-trip;
        // all others return the previously-cached generation, which is correct
        // since the refresh hasn't completed yet by definition.
        if (!lastRefreshNanos.compareAndSet(observed, now)) return localGeneration.get();
        // Eventually-consistent refresh as of 1.0.1: the CAS-winner dispatches
        // getAsync and returns the cached localGeneration immediately. The
        // refreshed value lands on the next caller, not this one. The shift
        // was forced by the sync-on-event-loop bug — currentGeneration is
        // reachable from a Netty event-loop thread via bucket() on the
        // fire-and-forget async write paths, where a sync RAtomicLong.get()
        // would throw IllegalStateException. Worst-case staleness is bounded
        // by GENERATION_REFRESH_NANOS, the same window already accepted by
        // the cached-generation contract; in practice each cache observes
        // at most one extra stale read per ~1s window.
        Supplier<CompletionStage<Long>> getSupplier = () ->
                distributedGeneration.getAsync().toCompletableFuture();
        try {
            breaker.executeCompletionStage(getSupplier).toCompletableFuture()
                    .whenComplete((value, ex) -> {
                        if (ex == null) {
                            localGeneration.set(value);
                            // lastRefreshNanos already set to `now` by the CAS above.
                        } else {
                            // Refresh failed — reset so the next call retries
                            // rather than waiting a full interval on stale data.
                            // CAS ensures we only reset if nothing else (e.g.
                            // bumpGenerationAsync) has written since.
                            lastRefreshNanos.compareAndSet(now, observed);
                        }
                    });
        } catch (CallNotPermittedException e) {
            // Breaker open at submission — same revert path as a future failure.
            lastRefreshNanos.compareAndSet(now, observed);
        }
        return localGeneration.get();
    }

    /**
     * Async generation bump used by {@link #clear()}. Returns a future
     * that always completes normally — failures are metered and logged
     * so the caller's chained publish runs unconditionally. The async
     * shape is required because {@code clear()} is reactive-reachable
     * via {@code @CacheEvict(allEntries=true)} on a Mono-returning
     * method; a sync {@code incrementAndGet} on the Netty event loop
     * would throw {@code IllegalStateException}.
     */
    private CompletableFuture<Void> bumpGenerationAsync() {
        Supplier<CompletionStage<Long>> incrSupplier = () ->
                distributedGeneration.incrementAndGetAsync().toCompletableFuture();
        try {
            return breaker.executeCompletionStage(incrSupplier).toCompletableFuture()
                    .handle((newGen, ex) -> {
                        if (ex != null) {
                            Throwable cause = unwrapCompletion(ex);
                            if (cause instanceof CallNotPermittedException) {
                                l2BreakerOpen.increment();
                            } else {
                                l2Failures.increment();
                                log.warn("Generation bump failed; clear visible only locally on '{}'",
                                        cacheName, cause);
                            }
                            return null;
                        }
                        localGeneration.set(newGen);
                        lastRefreshNanos.set(System.nanoTime());
                        return null;
                    });
        } catch (CallNotPermittedException e) {
            l2BreakerOpen.increment();
            return CompletableFuture.completedFuture(null);
        }
    }

    // ---------- Pub/sub ----------

    /**
     * Publishes an invalidation message and increments
     * {@code cache.invalidations.published{cache, op=metricOp}} only if the
     * publish completed without exception.
     *
     * <p>When reconciliation is enabled for this cache, the order is
     * deliberately <b>SET → INCR → publish(msg{seq: just-INCR'd value})</b>.
     * Item 2 of the {@code # implementation guardrails / ## Reconciliation}
     * block in {@code DESIGN.md} pins this ordering: {@code INCR} before
     * the canonical write would advance the seq for a publish that may
     * never reach peers (false-positive miss); {@code INCR} after publish
     * would carry a stale seq on the wire (guaranteed false-positive miss
     * since {@code lastObservedSeq} would never see the new value while
     * the canonical advanced).
     *
     * @param wireOp   the on-the-wire op code ({@link InvalidationMessage#OP_INVALIDATE}
     *                 or {@link InvalidationMessage#OP_CLEAR})
     * @param metricOp the operation that triggered the publish, used as the
     *                 {@code op} metric tag — one of {@code put}, {@code evict},
     *                 {@code clear}. Distinguishes the two callers that share
     *                 {@code OP_INVALIDATE} on the wire.
     */
    private CompletableFuture<Void> publishInvalidationAsync(String wireOp, String metricOp, String key) {
        // Sentinel: null seq means "skip publish" (INCR failed). When
        // reconciliation is disabled, the supplier completes with 0 so
        // the publish proceeds with the no-seq wire value.
        CompletableFuture<Long> seqStage;
        if (reconciliationEnabled) {
            Supplier<CompletionStage<Long>> incrSupplier = () ->
                    distributedSeq.incrementAndGetAsync().toCompletableFuture();
            try {
                seqStage = breaker.executeCompletionStage(incrSupplier).toCompletableFuture()
                        .handle((seq, ex) -> {
                            if (ex != null) {
                                Throwable cause = unwrapCompletion(ex);
                                if (cause instanceof CallNotPermittedException) {
                                    l2BreakerOpen.increment();
                                } else {
                                    l2Failures.increment();
                                    log.warn("Reconciliation INCR failed for cache '{}'; skipping publish",
                                            cacheName, cause);
                                }
                                return null;  // sentinel — skip publish
                            }
                            // Bump our own watermark to match the just-INCR'd canonical.
                            // The dispatcher self-skips messages we sent, so without this
                            // self-bump our lastObservedSeq would lag behind the canonical
                            // by exactly the number of publishes we've issued — turning
                            // every reconciliation cycle on a publishing node into a
                            // false-positive miss declaration. This is the dual of the
                            // "receivers update on receive" rule: publishers update on
                            // publish.
                            lastObservedSeq.accumulateAndGet(seq, Math::max);
                            return seq;
                        });
            } catch (CallNotPermittedException e) {
                l2BreakerOpen.increment();
                return CompletableFuture.completedFuture(null);
            }
        } else {
            seqStage = CompletableFuture.completedFuture(0L);
        }

        return seqStage.thenCompose(seq -> {
            if (seq == null) return CompletableFuture.completedFuture(null);
            long publishedSeq = seq;
            Supplier<CompletionStage<Long>> publishSupplier = () ->
                    dispatcher.publishAsync(new InvalidationMessage(
                                    dispatcher.getNodeId(), cacheName, wireOp, key, publishedSeq))
                            .toCompletableFuture();
            CompletableFuture<Long> publishStage;
            try {
                publishStage = breaker.executeCompletionStage(publishSupplier).toCompletableFuture();
            } catch (CallNotPermittedException e) {
                l2BreakerOpen.increment();
                return CompletableFuture.completedFuture(null);
            }
            return publishStage.handle((receivers, ex) -> {
                if (ex != null) {
                    // Suppressed — invalidation failure is bounded by TTL on remote nodes,
                    // or by the next reconciliation cycle when reconciliation is enabled.
                    return null;
                }
                // Only after the publish returns normally: the message reached
                // RTopic.publishAsync() and we treat it as published.
                switch (metricOp) {
                    case "put" -> publishedPut.increment();
                    case "evict" -> publishedEvict.increment();
                    case "clear" -> publishedClear.increment();
                    default -> log.warn("Unknown publish metric op '{}' on cache '{}'", metricOp, cacheName);
                }
                return null;
            });
        });
    }

    /** Invoked by {@link InvalidationDispatcher} after self-skip and name routing. */
    @Override
    public void handleInvalidation(String op, String key, long seq) {
        // Update the watermark FIRST: even if the per-op action below throws,
        // we have observed this peer's publish and the local seq must reflect
        // it. accumulateAndGet keeps the watermark monotonic under cluster
        // reroutes (out-of-order delivery is possible) — see guardrail item 3.
        if (seq > 0L) onMessageObserved(seq);
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

    @Override
    public void onMessageObserved(long seq) {
        // Math.max via accumulateAndGet — pub/sub messages can arrive out of
        // order with respect to publish order under cluster reroutes; a plain
        // set() would walk the watermark backwards then forwards then back,
        // generating spurious miss declarations on the next cycle.
        lastObservedSeq.accumulateAndGet(seq, Math::max);
    }

    /**
     * Per-cycle reconciliation work. Single GET on {@code <cache>:seq}, one
     * comparison, one of three outcomes — recorded by the metric tag set
     * declared in §6 of {@code DESIGN.md}:
     * <ul>
     *   <li>{@code redisSeq <= lastObservedSeq + tolerance} → no miss; cycle
     *       counter increments and we return.</li>
     *   <li>{@code redisSeq > lastObservedSeq + tolerance} → miss; clear local
     *       Caffeine, jump {@code lastObservedSeq} to {@code redisSeq} so the
     *       next cycle does not re-fire on the same gap, increment misses
     *       counter, log INFO.</li>
     *   <li>{@code redisSeq < lastObservedSeq} → regression (operator-driven —
     *       counter manually deleted, FLUSHALL, etc.); reset
     *       {@code lastObservedSeq} to {@code redisSeq}, increment regression
     *       counter, log WARN.</li>
     * </ul>
     *
     * <p>Every Redis op is breaker-wrapped. Breaker-open and any other
     * exception increments the appropriate {@code skipped} counter and
     * returns; nothing escapes (item 1 of the reconciliation guardrails).
     */
    @Override
    public void reconcile() {
        if (!reconciliationEnabled) return;
        Counter cyclesCompleted = Counter.builder("cache.reconciliation.cycles.completed")
                .tag("cache", cacheName).register(meterRegistry);
        Timer cycleDuration = Timer.builder("cache.reconciliation.cycle.duration")
                .tag("cache", cacheName).register(meterRegistry);
        Timer.Sample sample = Timer.start();
        try {
            long redisSeq;
            try {
                redisSeq = breaker.executeSupplier(distributedSeq::get);
            } catch (CallNotPermittedException e) {
                Counter.builder("cache.reconciliation.skipped")
                        .tag("cache", cacheName).tag("reason", "breaker-open")
                        .register(meterRegistry).increment();
                return;
            } catch (Exception e) {
                Counter.builder("cache.reconciliation.skipped")
                        .tag("cache", cacheName).tag("reason", "exception")
                        .register(meterRegistry).increment();
                log.warn("Reconciliation cycle exception for cache '{}'; skipping",
                        cacheName, e);
                return;
            }
            long observed = lastObservedSeq.get();
            int tolerance = spec.reconciliation().missTolerance();
            ReconciliationDecision decision =
                    ReconciliationDecision.classify(redisSeq, observed, tolerance);

            switch (decision) {
                case REGRESSION -> {
                    // Operator-driven (manual DEL, FLUSHALL). Reset the
                    // watermark; do NOT treat as a miss — see §10 0.5.0
                    // decision log on regression handling.
                    Counter.builder("cache.reconciliation.seq.regressions")
                            .tag("cache", cacheName).register(meterRegistry).increment();
                    log.warn("Reconciliation seq regressed on cache '{}': redisSeq={}, "
                            + "lastObservedSeq={}. Counter likely deleted by operator;"
                            + " resetting watermark.", cacheName, redisSeq, observed);
                    lastObservedSeq.set(redisSeq);
                }
                case MISS -> {
                    // Clear local L1 and jump the watermark. Do NOT publish.
                    // Item 4 of the guardrails: reconciliation is repair-mode,
                    // not initiation; publishing would force every healthy peer
                    // to also clear when only we missed.
                    Counter.builder("cache.reconciliation.misses.detected")
                            .tag("cache", cacheName).register(meterRegistry).increment();
                    log.info("Reconciliation declared miss on cache '{}': redisSeq={},"
                            + " lastObservedSeq={}, delta={}, tolerance={}. Clearing"
                            + " local L1.", cacheName, redisSeq, observed,
                            redisSeq - observed, tolerance);
                    caffeineCache.clear();
                    lastObservedSeq.set(redisSeq);
                }
                case NO_MISS -> { /* nothing to do */ }
            }
        } catch (Throwable t) {
            // Defense-in-depth — we caught the expected branches above. If
            // anything else escapes, swallow it: the scheduler must keep
            // running. Item 1 of the reconciliation guardrails.
            Counter.builder("cache.reconciliation.skipped")
                    .tag("cache", cacheName).tag("reason", "exception")
                    .register(meterRegistry).increment();
            log.warn("Reconciliation cycle threw unexpected error on cache '{}'", cacheName, t);
        } finally {
            sample.stop(cycleDuration);
            cyclesCompleted.increment();
        }
    }

    /** Test seam: returns the locally observed seq watermark. */
    public long lastObservedSeq() {
        return lastObservedSeq.get();
    }

    /** Test seam: true iff this cache participates in reconciliation. */
    public boolean isReconciliationEnabled() {
        return reconciliationEnabled;
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

    /**
     * Force the next {@link #currentGeneration()} call to refresh from
     * Redis instead of returning the cached value. Public surface as of
     * 0.5.0 — part of the reconciliation recovery contract. Pinned in
     * DESIGN.md §2 / Refactoring sweep and §10 0.5.0 entries.
     */
    public void forceRefreshDue() {
        lastRefreshNanos.set(0);
    }

    public CircuitBreaker getBreaker() {
        return breaker;
    }

    /**
     * Test seam: returns the SWR sidecar when configured, {@code null}
     * otherwise. Tests use this to assert sidecar size, dispatch counts,
     * and the in-flight collapse without poking at private state.
     */
    public SwrSidecar swrSidecar() {
        return swrSidecar;
    }

    /**
     * Test seam: returns the refresh-ahead coordinator when configured,
     * {@code null} otherwise. Tests use this to assert XFetch firing and
     * the de-dup interaction with SWR.
     */
    public RefreshAheadCoordinator refreshAhead() {
        return refreshAhead;
    }

    /** Test seam: per-cache loader-runtime EWMA, or {@code null} when SWR/RA is not configured. */
    public LoaderRuntimeEwma loaderEwma() {
        return loaderEwma;
    }

    // ---------- Async / reactive path (0.5.0) ----------
    //
    // Reactive @Cacheable methods land here via Spring 6.1's
    // CompletableFuture<ValueWrapper>-typed retrieve(...). Defaults in
    // the Cache interface wrap synchronous get(...) in
    // CompletableFuture.supplyAsync(...) — that blocks the calling
    // thread for the full chain and defeats reactivity. The override
    // here keeps the read non-blocking end-to-end:
    //   L1 (synchronous Caffeine read; cheap)
    //     → if hit: completedFuture(value), with SWR/RA dispatch on side
    //     → if miss: L2 via RBucket.getAsync()
    //         → if hit: populate L1 + completedFuture(value)
    //         → if miss: tryLockAsync → re-check L2 → loader hop → setAsync → publish

    /**
     * Async retrieve without a loader. L1 hit returns a completed future;
     * L1 miss chains to {@code RBucket.getAsync()} on the breaker, with
     * an L2 hit populating L1 (and recording the SWR sidecar deadline)
     * before the future completes.
     */
    @Override
    public CompletableFuture<?> retrieve(@Nonnull Object key) {
        String stringKey = CacheKeys.stringify(cacheName, key);
        ValueWrapper wrapper = caffeineCache.get(stringKey);
        if (wrapper != null) {
            // Loader-less retrieve cannot dispatch SWR/RA refresh — same
            // as the sync get(key) path. Return the wrapper directly.
            return CompletableFuture.completedFuture(wrapper);
        }
        return readFromL2Async(stringKey)
                .thenApply(distValue -> {
                    if (distValue == null) return null;
                    if (swrSidecar != null) swrSidecar.recordWrite(stringKey);
                    caffeineCache.put(stringKey, distValue);
                    // Re-fetch via Caffeine so the wrapper has the same
                    // NullValue handling the sync path uses.
                    return caffeineCache.get(stringKey);
                });
    }

    /**
     * Async retrieve with a loader. The headline async surface for
     * NearCache. Mirrors the sync {@link #get(Object, Callable)} chain
     * end-to-end with non-blocking primitives:
     *
     * <ul>
     *   <li>L1 hit → completedFuture(value); SWR/RA may dispatch refresh
     *       through the existing coordinators.</li>
     *   <li>L1 miss → {@code RBucket.getAsync()} via the breaker.</li>
     *   <li>L2 hit → populate L1 + completedFuture(value).</li>
     *   <li>L2 miss → cross-JVM single-flight via the shared async
     *       in-flight map; cross-node single-flight via {@code RLock.tryLockAsync};
     *       loader runs on the refresh executor (mandatory loader hop —
     *       guardrail item 2); on success: {@code setAsync → publishAsync}.</li>
     * </ul>
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> CompletableFuture<T> retrieve(@Nonnull Object key,
                                              @Nonnull Supplier<CompletableFuture<T>> valueLoader) {
        String stringKey = CacheKeys.stringify(cacheName, key);
        ValueWrapper wrapper = caffeineCache.get(stringKey);
        if (wrapper != null) {
            // L1 hit. Build the async-aware refresh task body and dispatch
            // through SWR (STALE) or RA (FRESH + predicate) — matches the
            // sync read-path's classification branching.
            if (swrSidecar != null) {
                Callable<Object> refreshTask = buildAsyncRefreshTaskBody(key, stringKey, valueLoader);
                SwrSidecar.Classification c = swrSidecar.classify(stringKey);
                if (c == SwrSidecar.Classification.STALE) {
                    swrSidecar.maybeDispatchRefresh(stringKey, refreshTask);
                } else if (refreshAhead != null) {
                    refreshAhead.evaluateAndMaybeDispatch(stringKey, refreshTask);
                }
            }
            T value = (T) wrapper.get();
            return CompletableFuture.completedFuture(value);
        }
        return readFromL2Async(stringKey)
                .thenCompose(distValue -> {
                    if (distValue != null) {
                        // Sidecar pre-write before L1 visibility (guardrail #2 of SWR).
                        if (swrSidecar != null) swrSidecar.recordWrite(stringKey);
                        caffeineCache.put(stringKey, distValue);
                        @SuppressWarnings("unchecked")
                        T cast = (T) (distValue instanceof NullValue ? null : distValue);
                        return CompletableFuture.completedFuture(cast);
                    }
                    return loadAsyncWithSingleFlight(key, stringKey, valueLoader);
                });
    }

    private CompletableFuture<Object> readFromL2Async(String key) {
        Timer.Sample sample = Timer.start();
        Supplier<CompletionStage<Object>> readSupplier = () -> {
            // bucket() reads the generation counter — safe from any
            // thread, and the Redisson async API takes it from there.
            return bucket(key).getAsync().toCompletableFuture();
        };
        CompletableFuture<Object> chained;
        try {
            chained = breaker.executeCompletionStage(readSupplier).toCompletableFuture();
        } catch (CallNotPermittedException e) {
            l2BreakerOpen.increment();
            sample.stop(l2GetLatency);
            return CompletableFuture.completedFuture(null);
        }
        return chained.handle((value, ex) -> {
            sample.stop(l2GetLatency);
            if (ex != null) {
                Throwable cause = unwrapCompletion(ex);
                if (cause instanceof CallNotPermittedException) {
                    l2BreakerOpen.increment();
                } else {
                    l2Failures.increment();
                    log.warn("L2 async read failed for key '{}'; degrading to local-only",
                            keyLogFormatter.format(key), cause);
                }
                return null;
            }
            if (value == null) {
                l2Misses.increment();
            } else {
                l2Hits.increment();
            }
            return value;
        });
    }

    @SuppressWarnings("unchecked")
    private <T> CompletableFuture<T> loadAsyncWithSingleFlight(
            Object key, String stringKey, Supplier<CompletableFuture<T>> valueLoader) {
        // computeIfAbsent makes the "is there an in-flight loader, or do
        // I create one" decision atomic. The previous putIfAbsent shape
        // could race against a concurrent retrieve whose L2 miss landed
        // after the first caller's loader had already completed, populated
        // L1, and removed itself from the registry — that retrieve would
        // not re-check L1 (the outer retrieve checked it only at entry)
        // and would start a second loader. Re-checking L1 inside the
        // atomic remap closes that window: by the time we are here, an
        // earlier loader may have populated L1, in which case we publish
        // its already-completed value through the registry instead of
        // starting fresh. Equivalent to the Mono.cache() pattern in the
        // reactive shape: subscribers share one resolution, never
        // re-trigger the work.
        CompletableFuture<Object> shared = asyncInflight.computeIfAbsent(stringKey, k -> {
            ValueWrapper warmed = caffeineCache.get(stringKey);
            if (warmed != null) {
                return CompletableFuture.completedFuture(warmed.get());
            }
            CompletableFuture<Object> mine = new CompletableFuture<>();
            Executor hop = refreshExecutorRef != null ? refreshExecutorRef : ForkJoinPool.commonPool();
            long start = System.nanoTime();
            loadCrossNodeAsync(key, stringKey, valueLoader, hop)
                    .whenComplete((value, ex) -> {
                        if (ex != null) {
                            mine.completeExceptionally(ex);
                            return;
                        }
                        // Populate L1 via the cache layer (NullValue
                        // handling), record sidecar deadline first
                        // (guardrail #2 of SWR).
                        if (swrSidecar != null) swrSidecar.recordWrite(stringKey);
                        caffeineCache.put(stringKey, value);
                        if (loaderEwma != null) {
                            loaderEwma.record(System.nanoTime() - start);
                        }
                        // No publish on cold load — same rule the sync
                        // path follows; see the long comment in
                        // loadWithDistributedLock.
                        invalidationsSuppressedColdLoad.increment();
                        mine.complete(value);
                    });
            // Free the registry slot only on terminal signal so concurrent
            // arrivals during the inflight window all get the same future.
            // Compare-and-remove (remove(k, mine)) so a slot already
            // replaced by a later compute cycle is not clobbered.
            mine.whenComplete((v, ex) -> asyncInflight.remove(k, mine));
            return mine;
        });
        return (CompletableFuture<T>) shared.handle((v, ex) -> {
            if (ex != null) throw asUncheckedAsyncFailure(key, valueLoader, ex);
            return v;
        });
    }

    private <T> CompletableFuture<Object> loadCrossNodeAsync(
            Object key, String stringKey,
            Supplier<CompletableFuture<T>> valueLoader, Executor hop) {
        if (spec.lockWait().isZero()) {
            return runAsyncLoaderAndWriteThrough(key, stringKey, valueLoader, hop, null);
        }
        RLock lock = redisson.getLock(CacheKeys.lockKey(cacheName, stringKey));
        return lock.tryLockAsync(
                        spec.lockWait().toMillis(),
                        spec.lockLease().toMillis(),
                        TimeUnit.MILLISECONDS)
                .toCompletableFuture()
                .handle((acquired, ex) -> {
                    if (ex != null) {
                        log.debug("Distributed async lock acquisition failed for key '{}';"
                                + " proceeding with local single-flight only",
                                keyLogFormatter.format(stringKey), ex);
                        return Boolean.FALSE;
                    }
                    return acquired;
                })
                .thenCompose(acquired -> {
                    if (Boolean.TRUE.equals(acquired)) {
                        return readFromL2Async(stringKey)
                                .thenCompose(existing -> {
                                    if (existing != null) {
                                        return releaseLockAsync(lock).thenApply(v -> existing);
                                    }
                                    return runAsyncLoaderAndWriteThrough(
                                            key, stringKey, valueLoader, hop, lock);
                                });
                    }
                    return runAsyncLoaderAndWriteThrough(
                            key, stringKey, valueLoader, hop, null);
                });
    }

    private <T> CompletableFuture<Object> runAsyncLoaderAndWriteThrough(
            Object key, String stringKey,
            Supplier<CompletableFuture<T>> valueLoader, Executor hop,
            @Nullable RLock lockToReleaseOnCompletion) {
        return CompletableFuture.completedFuture((Void) null)
                .thenComposeAsync(v -> asyncLoaderGate.run(key, () -> {
                    CompletableFuture<T> raw;
                    try {
                        raw = valueLoader.get();
                    } catch (Throwable t) {
                        CompletableFuture<Object> failed = new CompletableFuture<>();
                        failed.completeExceptionally(t);
                        return failed;
                    }
                    if (raw == null) raw = CompletableFuture.completedFuture(null);
                    return raw.thenApply(o -> (Object) o);
                }), hop)
                .thenCompose(value ->
                        writeToL2Async(stringKey, value).thenApply(ignored -> value))
                .whenComplete((v, ex) -> {
                    if (lockToReleaseOnCompletion != null) {
                        releaseLockAsync(lockToReleaseOnCompletion);
                    }
                });
    }

    private CompletableFuture<Void> releaseLockAsync(RLock lock) {
        try {
            return lock.unlockAsync().toCompletableFuture()
                    .handle((v, ex) -> {
                        if (ex != null) {
                            log.warn("Async unlock failed cache='{}'", cacheName, ex);
                        }
                        return null;
                    });
        } catch (Exception e) {
            log.warn("Async unlock invocation failed cache='{}'", cacheName, e);
            return CompletableFuture.completedFuture(null);
        }
    }

    private CompletableFuture<Void> writeToL2Async(String key, Object value) {
        Supplier<CompletionStage<Void>> writeSupplier = () -> {
            return bucket(key).setAsync(value, spec.ttl()).toCompletableFuture()
                    .thenApply(unused -> (Void) null);
        };
        try {
            return breaker.executeCompletionStage(writeSupplier).toCompletableFuture()
                    .handle((v, ex) -> {
                        if (ex != null) {
                            Throwable cause = unwrapCompletion(ex);
                            if (cause instanceof CallNotPermittedException) {
                                l2BreakerOpen.increment();
                            } else {
                                l2Failures.increment();
                                log.warn("L2 async write failed for key '{}';"
                                        + " cross-node incoherence until TTL",
                                        keyLogFormatter.format(key), cause);
                            }
                        }
                        return null;
                    });
        } catch (CallNotPermittedException e) {
            l2BreakerOpen.increment();
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * Builds an SWR / RA refresh task body for use from the async
     * read-path on L1 hit. The same shape the sync path uses (loader
     * through async gate, write through {@link #put(Object, Object)} so
     * cross-node invalidation propagates), invoked from inside the SWR
     * executor's worker thread.
     *
     * <p>The dispatch contract on {@code SwrSidecar} / {@code RefreshAheadCoordinator}
     * is {@code Callable<Object>} — synchronous. We adapt by joining the
     * async loader future inside the callable, which is acceptable
     * because the callable runs on the SWR/RA refresh-executor thread,
     * not on a Netty event loop or Reactor thread.
     */
    private <T> Callable<Object> buildAsyncRefreshTaskBody(
            Object key, String stringKey, Supplier<CompletableFuture<T>> valueLoader) {
        return () -> {
            long start = System.nanoTime();
            try {
                final CompletableFuture<T> userFuture;
                try {
                    CompletableFuture<T> raw = valueLoader.get();
                    userFuture = raw == null ? CompletableFuture.completedFuture(null) : raw;
                } catch (Throwable t) {
                    throw t instanceof Exception ? (Exception) t : new RuntimeException(t);
                }
                Object value;
                try {
                    value = asyncLoaderGate.run(key, () -> userFuture.thenApply(v -> (Object) v))
                            .toCompletableFuture().join();
                } catch (CompletionException ce) {
                    Throwable cause = ce.getCause() != null ? ce.getCause() : ce;
                    if (cause instanceof Exception ex) throw ex;
                    throw new RuntimeException(cause);
                }
                // Refresh writes through put(): updates L1, L2, and
                // publishes the invalidation. Same as the sync refresh
                // task body — SWR guardrail item 6.
                put(stringKey, value);
                return value;
            } finally {
                if (loaderEwma != null) {
                    loaderEwma.record(System.nanoTime() - start);
                }
            }
        };
    }

    private RuntimeException asUncheckedAsyncFailure(
            Object key, Supplier<?> loader, Throwable ex) {
        Throwable cause = unwrapCompletion(ex);
        if (cause instanceof LoaderRejectedException lr) return lr;
        if (cause instanceof RuntimeException re
                && re.getClass() == Cache.ValueRetrievalException.class) {
            return re;
        }
        Callable<?> loaderAdapter = loader::get;
        return new Cache.ValueRetrievalException(key, loaderAdapter, cause);
    }

    private static Throwable unwrapCompletion(Throwable t) {
        Throwable cur = t;
        while (cur instanceof CompletionException
                || cur instanceof ExecutionException) {
            Throwable c = cur.getCause();
            if (c == null || c == cur) break;
            cur = c;
        }
        return cur;
    }
}
