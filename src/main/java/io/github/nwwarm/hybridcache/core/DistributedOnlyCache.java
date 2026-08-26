package io.github.nwwarm.hybridcache.core;

import io.github.nwwarm.hybridcache.config.CacheProperties;
import io.github.nwwarm.hybridcache.invalidation.InvalidationDispatcher;
import io.github.nwwarm.hybridcache.invalidation.InvalidationListener;
import io.github.nwwarm.hybridcache.invalidation.InvalidationMessage;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.Nonnull;
import org.redisson.api.*;
import org.redisson.client.codec.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;
import jakarta.annotation.Nullable;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Redis-only cache with no L1.
 *
 * <p>Every read crosses the network. Use when working set is too large to
 * replicate per-node, when consistency matters more than latency, or when the
 * cached values are inherently shared across nodes (sessions, idempotency
 * keys, distributed coordination state).
 *
 * <h2>Single-flight</h2>
 *
 * Two-tier herd protection on the loader path ({@link #get(Object, Callable)}):
 *
 * <ol>
 *   <li><b>Local single-flight</b> via an in-flight {@link CompletableFuture} map.
 *       Only one thread per JVM executes the loader for a given key; other threads
 *       wait on the future. Works whether Redis is healthy, slow, or unreachable.</li>
 *   <li><b>Cross-node single-flight</b> via Redisson {@link RLock}. Acquired inside
 *       the loader. If Redis is down, lock acquisition fails and we proceed with
 *       local-only protection — the worst case is N database loads across N nodes,
 *       not N × threads-per-node.</li>
 * </ol>
 *
 * <p>Generation counter (same trick as {@link NearCache}) gives O(1) {@link #clear()}.
 *
 * <h2>Failure semantics</h2>
 *
 * <p>When the circuit breaker is open every read returns null and every write is
 * suppressed — callers fall through to the source. A cache must not cause outages;
 * it must fail open. Source systems behind a distributed-only cache must handle
 * full unfiltered load during a Redis incident.
 */
public class DistributedOnlyCache implements HybridCache, InvalidationListener, Reconciler {

    private static final Logger log = LoggerFactory.getLogger(DistributedOnlyCache.class);
    private static final long GENERATION_REFRESH_NANOS = 1_000_000_000L; // 1s

    private final String cacheName;
    private final CacheProperties.CacheSpec spec;
    private final RedissonClient redisson;
    private final CircuitBreaker breaker;
    private final Codec bucketCodec;
    private final KeyLogFormatter keyLogFormatter;
    private final InvalidationDispatcher dispatcher;
    private final MeterRegistry meterRegistry;

    private final RAtomicLong distributedGeneration;
    private final AtomicLong localGeneration = new AtomicLong(0);
    private final AtomicLong lastRefreshNanos = new AtomicLong(0);

    // Reconciliation (0.5.0). Same shape as NearCache — see that class's
    // field comment. The recovery action differs: there is no L1 to clear,
    // so the cache forces a generation-pointer refresh, which is the same
    // path handleInvalidation(OP_CLEAR, ...) already takes.
    private final boolean reconciliationEnabled;
    private final RAtomicLong distributedSeq;
    private final AtomicLong lastObservedSeq = new AtomicLong(0);

    /** In-flight loads, used for local single-flight when there is no L1 to lean on. */
    private final ConcurrentMap<Object, CompletableFuture<Object>> inflight = new ConcurrentHashMap<>();

    private final Counter hits;
    private final Counter misses;
    private final Counter failures;
    private final Counter breakerOpen;
    private final Timer getLatency;
    private final LoaderGate loaderGate;
    /**
     * cache.invalidations.published{cache, op=clear}. DistributedOnlyCache
     * publishes only on clear/clearImmediate — there is no L1 to keep
     * coherent on per-key writes, so put/evict do not publish and have no
     * counter. The metric name and tag scheme match NearCache so dashboards
     * can union across tiers.
     */
    private final Counter publishedClear;
    /**
     * Async-path loader gate (0.5.0). Constructed unconditionally — when
     * {@code max-concurrent-loaders} is unset, both sync and async gates
     * short-circuit to direct loader calls (matches {@link LoaderGate}).
     */
    private final AsyncLoaderGate asyncLoaderGate;
    /**
     * Refresh executor for the mandatory loader hop on the async path.
     * When null (legacy/test construction), the async loader runs on
     * {@link ForkJoinPool#commonPool()} — same execution shape, slightly
     * less control over thread name. The hop is mandatory: a blocking
     * loader on Redisson's Netty event loop would stall every other
     * Redisson IO operation in the JVM (guardrail item 2).
     */
    @Nullable private final Executor refreshExecutor;

    public DistributedOnlyCache(String cacheName,
                                CacheProperties.CacheSpec spec,
                                Codec bucketCodec,
                                RedissonClient redisson,
                                CircuitBreaker breaker,
                                InvalidationDispatcher dispatcher,
                                MeterRegistry meterRegistry,
                                KeyLogFormatter keyLogFormatter) {
        this(cacheName, spec, bucketCodec, redisson, breaker, dispatcher,
                meterRegistry, keyLogFormatter, null);
    }

    /**
     * Async-aware constructor (0.5.0). The {@code refreshExecutor} is the
     * same shared executor used by SWR / RA / async-loader hops, plumbed
     * here so {@link #retrieve(Object, Supplier)} can hand the loader to
     * a non-Netty thread (guardrail item 2).
     */
    public DistributedOnlyCache(String cacheName,
                                CacheProperties.CacheSpec spec,
                                Codec bucketCodec,
                                RedissonClient redisson,
                                CircuitBreaker breaker,
                                InvalidationDispatcher dispatcher,
                                MeterRegistry meterRegistry,
                                KeyLogFormatter keyLogFormatter,
                                @Nullable Executor refreshExecutor) {
        this.cacheName = cacheName;
        this.spec = spec;
        this.redisson = redisson;
        this.breaker = breaker;
        this.bucketCodec = bucketCodec;
        this.keyLogFormatter = keyLogFormatter;
        this.dispatcher = dispatcher;
        this.meterRegistry = meterRegistry;
        this.distributedGeneration = redisson.getAtomicLong(CacheKeys.generationKey(cacheName));

        try {
            breaker.executeRunnable(() -> localGeneration.set(distributedGeneration.get()));
            lastRefreshNanos.set(System.nanoTime());
        } catch (Exception e) {
            log.warn("Failed to initialize generation for cache '{}'", cacheName, e);
        }

        this.reconciliationEnabled = spec.reconciliation() != null
                && spec.reconciliation().enabled();
        this.distributedSeq = reconciliationEnabled
                ? redisson.getAtomicLong(CacheKeys.seqKey(cacheName))
                : null;

        dispatcher.register(this);

        this.hits = Counter.builder("cache.distributed.gets")
                .tag("cache", cacheName).tag("result", "hit").register(meterRegistry);
        this.misses = Counter.builder("cache.distributed.gets")
                .tag("cache", cacheName).tag("result", "miss").register(meterRegistry);
        this.failures = Counter.builder("cache.distributed.failures")
                .tag("cache", cacheName).register(meterRegistry);
        this.breakerOpen = Counter.builder("cache.distributed.breaker.open")
                .tag("cache", cacheName).register(meterRegistry);
        this.getLatency = Timer.builder("cache.distributed.get.latency")
                .tag("cache", cacheName).register(meterRegistry);
        this.loaderGate = new LoaderGate(cacheName,
                spec.maxConcurrentLoaders(), spec.loaderAcquireTimeout(), meterRegistry);
        this.asyncLoaderGate = new AsyncLoaderGate(cacheName,
                spec.maxConcurrentLoaders(), spec.loaderAcquireTimeout(), meterRegistry);
        this.refreshExecutor = refreshExecutor;
        this.publishedClear = Counter.builder("cache.invalidations.published")
                .tag("cache", cacheName).tag("op", "clear").register(meterRegistry);
    }

    @Override
    @Nonnull
    public String getName() {
        return cacheName;
    }

    @Override
    @Nonnull
    public Object getNativeCache() {
        return redisson;
    }

    @Override
    public ValueWrapper get(@Nonnull Object key) {
        String stringKey = CacheKeys.stringify(cacheName, key);
        Object value = readFromRedis(stringKey);
        return value == null ? null : new SimpleValueWrapper(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(@Nonnull Object key, Class<T> type) {
        String stringKey = CacheKeys.stringify(cacheName, key);
        Object value = readFromRedis(stringKey);
        if (value != null && type != null && !type.isInstance(value)) {
            throw new IllegalStateException(
                    "Cached value [" + value + "] is not of required type [" + type.getName() + "]");
        }
        return (T) value;
    }

    /**
     * Loader-aware get with two-tier single-flight protection.
     *
     * <ol>
     *   <li><b>Local single-flight</b> via {@link #inflight} map. Concurrent requests
     *       on this JVM for the same key collapse onto a single {@link CompletableFuture}
     *       — only the first thread executes the loader path; others wait. Works whether
     *       Redis is up or down.</li>
     *   <li><b>Cross-node single-flight</b> via {@link RLock} inside the loader path.
     *       Falls through if Redis is unreachable; local single-flight remains.</li>
     * </ol>
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(@Nonnull Object key, @Nonnull Callable<T> valueLoader) {
        String stringKey = CacheKeys.stringify(cacheName, key);

        // Fast path: already in Redis.
        Object existing = readFromRedis(stringKey);
        if (existing != null) return (T) existing;

        CompletableFuture<Object> mine = new CompletableFuture<>();
        CompletableFuture<Object> theirs = inflight.putIfAbsent(stringKey, mine);

        if (theirs != null) {
            // Another thread on this JVM is already loading. Wait for its result.
            try {
                long timeoutMs = spec.lockWait().toMillis() + spec.lockLease().toMillis();
                Object value = theirs.get(timeoutMs, TimeUnit.MILLISECONDS);
                return (T) value;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ValueRetrievalException(key, valueLoader, e);
            } catch (ExecutionException | TimeoutException e) {
                Throwable cause = e.getCause();
                if (cause instanceof LoaderRejectedException lr) throw lr;
                throw new ValueRetrievalException(key, valueLoader,
                        cause != null ? cause : e);
            }
        }

        // We're the first; do the load.
        try {
            Object value = doLoadWithCrossNodeSingleFlight(stringKey, valueLoader);
            mine.complete(value);
            return (T) value;
        } catch (RuntimeException t) {
            mine.completeExceptionally(t);
            throw t;
        } catch (Throwable t) {
            mine.completeExceptionally(t);
            throw new ValueRetrievalException(key, valueLoader, t);
        } finally {
            inflight.remove(stringKey, mine);
        }
    }

    /**
     * Performs the load behind a Redis lock when possible. Falls through to
     * loader-only if Redis is unreachable — local inflight map already handles
     * the per-JVM herd.
     */
    private Object doLoadWithCrossNodeSingleFlight(String key, Callable<?> valueLoader) {
        RLock lock = null;
        boolean acquired = false;
        try {
            if (!spec.lockWait().isZero()) {
                lock = redisson.getLock(CacheKeys.lockKey(cacheName, key));
                acquired = lock.tryLock(
                        spec.lockWait().toMillis(),
                        spec.lockLease().toMillis(),
                        TimeUnit.MILLISECONDS);

                if (acquired) {
                    Object existing = readFromRedis(key);
                    if (existing != null) return existing;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ValueRetrievalException(key, valueLoader, e);
        } catch (Exception e) {
            log.debug("Distributed lock acquisition failed for key '{}'; proceeding with local single-flight only", keyLogFormatter.format(key), e);
        }

        try {
            Object value = loaderGate.run(key, valueLoader);
            // Bypass put()'s key validation: we already stringified above.
            writeToRedis(key, value);
            return value;
        } catch (LoaderRejectedException e) {
            // Backpressure: do not wrap as ValueRetrievalException — callers
            // distinguish rejection (no loader call made) from failure
            // (loader ran and threw).
            throw e;
        } catch (Throwable t) {
            throw new ValueRetrievalException(key, valueLoader, t);
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

    @Override
    public void put(@Nonnull Object key, Object value) {
        String stringKey = CacheKeys.stringify(cacheName, key);
        // Async write end-to-end. A sync RBucket.set here would throw
        // IllegalStateException when put() is invoked from a Redisson
        // Netty event-loop thread (Spring's reactive cache integration
        // drives put() from inside a CompletionStage continuation;
        // under load that continuation may run on the I/O thread).
        // On non-event-loop threads we await the chain to preserve the
        // 1.0.0 sync contract — callers expect put() to be observable
        // before it returns.
        awaitUnlessOnEventLoop(writeToRedisAsync(stringKey, value));
    }

    private void writeToRedis(String key, Object value) {
        try {
            breaker.executeRunnable(() -> bucket(key).set(value, spec.ttl()));
        } catch (CallNotPermittedException e) {
            breakerOpen.increment();
            // Distributed-only + Redis down = no caching this call. Caller hits source on next read.
        } catch (Exception e) {
            failures.increment();
            log.warn("Distributed put failed for key '{}'", keyLogFormatter.format(key), e);
        }
    }

    @Override
    public void evict(@Nonnull Object key) {
        String stringKey = CacheKeys.stringify(cacheName, key);
        awaitUnlessOnEventLoop(deleteFromRedisAsync(stringKey));
    }

    private CompletableFuture<Void> deleteFromRedisAsync(String key) {
        Supplier<CompletionStage<Void>> deleteSupplier = () ->
                bucket(key).deleteAsync().toCompletableFuture()
                        .thenApply(deleted -> (Void) null);
        try {
            return breaker.executeCompletionStage(deleteSupplier).toCompletableFuture()
                    .handle((v, ex) -> {
                        if (ex != null) {
                            Throwable cause = unwrapCompletion(ex);
                            if (cause instanceof CallNotPermittedException) {
                                breakerOpen.increment();
                            } else {
                                failures.increment();
                                log.warn("Distributed evict failed for key '{}'",
                                        keyLogFormatter.format(key), cause);
                            }
                        }
                        return null;
                    });
        } catch (CallNotPermittedException e) {
            breakerOpen.increment();
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public void clear() {
        // Async chain: bump generation, then publish OP_CLEAR. clear() is
        // reactive-reachable via @CacheEvict(allEntries=true) — sync
        // RAtomicLong.incrementAndGet / RTopic.publish here would throw
        // IllegalStateException on the Netty event loop. On non-event-loop
        // threads we await so the 1.0.0 sync contract holds.
        awaitUnlessOnEventLoop(
                bumpGenerationAsync()
                        .thenCompose(v -> publishClearAsync()));
    }

    /**
     * Block on the async chain when invoked from a regular worker thread;
     * leave it running fire-and-forget when on a Redisson Netty event-loop
     * thread. Mirrors {@link NearCache#awaitUnlessOnEventLoop}.
     */
    private static void awaitUnlessOnEventLoop(CompletableFuture<?> chain) {
        if (isOnRedissonEventLoop()) return;
        chain.join();
    }

    private static boolean isOnRedissonEventLoop() {
        String name = Thread.currentThread().getName();
        return name.startsWith("redisson-netty-") || name.startsWith("nioEventLoopGroup-");
    }

    private CompletableFuture<Void> bumpGenerationAsync() {
        Supplier<CompletionStage<Long>> incrSupplier = () ->
                distributedGeneration.incrementAndGetAsync().toCompletableFuture();
        try {
            return breaker.executeCompletionStage(incrSupplier).toCompletableFuture()
                    .handle((newGen, ex) -> {
                        if (ex != null) {
                            Throwable cause = unwrapCompletion(ex);
                            if (cause instanceof CallNotPermittedException) {
                                breakerOpen.increment();
                            } else {
                                failures.increment();
                                log.warn("Distributed clear (generation bump) failed for cache '{}'",
                                        cacheName, cause);
                            }
                            return null;
                        }
                        localGeneration.set(newGen);
                        lastRefreshNanos.set(System.nanoTime());
                        return null;
                    });
        } catch (CallNotPermittedException e) {
            breakerOpen.increment();
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * Eager clear: bump generation, then SCAN + UNLINK every old-generation
     * value bucket on every reachable shard. Stronger than {@link #clear()} —
     * when this returns, no orphan keys remain in Redis under this cache's
     * prefix. See {@link HybridCache#clearImmediate()} for the rationale.
     *
     * <p>Order matches {@code NearCache.clearImmediate}: bump generation
     * first (concurrent reads switch to the new prefix immediately), publish
     * OP_CLEAR (peers refresh their generation pointer ASAP), then UNLINK
     * old-generation keys via {@link RKeys#unlinkByPattern}, which iterates
     * every cluster master and pipelines UNLINK in batches.
     *
     * <p>Failure handling: if the bump fails the exception propagates and
     * no UNLINK is attempted. If UNLINK fails mid-iteration the exception
     * still propagates, but correctness is preserved — the generation is
     * already bumped, so reads land on a fresh prefix and surviving keys
     * orphan and decay via TTL.
     */
    @Override
    public void clearImmediate() {
        // Async chain end-to-end: incrementAndGetAsync → set local state →
        // publish OP_CLEAR → unlinkByPatternAsync. Reactive-reachable via
        // @CacheEvict(allEntries=true) on a Mono-returning method. Mirrors
        // the NearCache.clearImmediate fix; see that method's comment for
        // the threading rationale. awaitUnlessOnEventLoop preserves the
        // "no orphan keys when this returns" contract on regular worker
        // threads; event-loop callers fire-and-forget to avoid deadlocking
        // the I/O thread that completes the chain.
        Supplier<CompletionStage<Long>> incrSupplier = () ->
                distributedGeneration.incrementAndGetAsync().toCompletableFuture();
        CompletableFuture<?> chain = breaker.executeCompletionStage(incrSupplier).toCompletableFuture()
                .thenCompose(newGen -> {
                    // Local state advance happens-after the bump lands so peers
                    // never see localGeneration ahead of persisted Redis state.
                    localGeneration.set(newGen);
                    lastRefreshNanos.set(System.nanoTime());
                    long oldGen = newGen - 1;
                    publishClearAsync();
                    String pattern = CacheKeys.valueKeyPattern(cacheName, oldGen);
                    return redisson.getKeys().unlinkByPatternAsync(pattern)
                            .toCompletableFuture()
                            .handle((deleted, unlinkEx) -> {
                                if (unlinkEx != null) {
                                    Throwable cause = unwrapCompletion(unlinkEx);
                                    failures.increment();
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
                        breakerOpen.increment();
                    } else {
                        failures.increment();
                        log.warn("clearImmediate generation bump failed for cache '{}'",
                                cacheName, cause);
                    }
                    return null;
                });

        awaitUnlessOnEventLoop(chain);
    }

    private CompletableFuture<Void> publishClearAsync() {
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
                                    breakerOpen.increment();
                                } else {
                                    failures.increment();
                                    log.warn("Reconciliation INCR failed on clear for cache '{}'; skipping publish",
                                            cacheName, cause);
                                }
                                return null;
                            }
                            // Self-bump local watermark — dispatcher self-skips our own
                            // message so without this our lastObservedSeq would lag.
                            lastObservedSeq.accumulateAndGet(seq, Math::max);
                            return seq;
                        });
            } catch (CallNotPermittedException e) {
                breakerOpen.increment();
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
                                    dispatcher.getNodeId(), cacheName,
                                    InvalidationMessage.OP_CLEAR, null, publishedSeq))
                            .toCompletableFuture();
            CompletableFuture<Long> publishStage;
            try {
                publishStage = breaker.executeCompletionStage(publishSupplier).toCompletableFuture();
            } catch (CallNotPermittedException e) {
                breakerOpen.increment();
                return CompletableFuture.completedFuture(null);
            }
            return publishStage.handle((receivers, ex) -> {
                if (ex == null) publishedClear.increment();
                // Failures suppressed — peers will catch up via the 1s
                // generation poll or the next reconciliation cycle.
                return null;
            });
        });
    }

    /**
     * Invoked by {@link InvalidationDispatcher} after self-skip and name routing.
     * Only {@code OP_CLEAR} drives any action: a clear from a peer means the
     * generation counter has advanced, so we force a refresh on the next read
     * instead of waiting for the 1s poll. {@code OP_INVALIDATE} carries no
     * action here — there is no L1 to evict, and the peer's L2 delete already
     * removed the entry from L2.
     */
    @Override
    public void handleInvalidation(String op, String key, long seq) {
        // Update watermark before per-op action (see NearCache.handleInvalidation
        // for the rationale — even if the action below throws, we have observed
        // the publish and the local seq must reflect it).
        if (seq > 0L) onMessageObserved(seq);
        switch (op) {
            case InvalidationMessage.OP_CLEAR -> {
                lastRefreshNanos.set(0);
                currentGeneration();
            }
            case InvalidationMessage.OP_INVALIDATE -> log.trace(
                    "Ignoring per-key invalidation on distributed-only cache '{}' key={}",
                    cacheName, keyLogFormatter.format(key));
            default -> log.warn("Unknown invalidation op '{}' on cache '{}'", op, cacheName);
        }
    }

    @Override
    public void onMessageObserved(long seq) {
        lastObservedSeq.accumulateAndGet(seq, Math::max);
    }

    /**
     * Per-cycle reconciliation work for distributed-only caches. Same shape
     * as {@code NearCache.reconcile()}: read {@code <cache>:seq}, compare,
     * one of three outcomes. The recovery action differs — there is no L1
     * to clear, so on a miss the cache forces a generation-pointer refresh,
     * the same path {@code handleInvalidation(OP_CLEAR, ...)} already takes.
     * That repairs the locally-cached generation, which is the actual state
     * a missed {@code OP_CLEAR} would have left stale.
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
            // Two watermark samples bracketing the GET — see
            // NearCache.reconcile() and NearCache.classifyBracketed for the
            // full rationale. Same rule, same guarantee: a sample taken
            // before the GET was issued cannot legitimately exceed what the
            // GET returns, so REGRESSION measured against it is genuine.
            long observedBefore = lastObservedSeq.get();
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
            long observedAfter = lastObservedSeq.get();
            int tolerance = spec.reconciliation().missTolerance();
            ReconciliationDecision decision =
                    classifyBracketed(redisSeq, observedBefore, observedAfter, tolerance);

            if (decision == ReconciliationDecision.REGRESSION) {
                // Second line of defence, retained from 1.0.2. The bracketed
                // read removes the single-master race that used to reach
                // here; a lagging-replica read (custom RedissonClient with
                // ReadMode.SLAVE) can still show a transient dip. Re-read
                // through the same breaker to disambiguate from a genuine
                // counter-deleted regression. See NearCache.reconcile()
                // for the full mechanism.
                long redisSeqRecheck;
                try {
                    redisSeqRecheck = breaker.executeSupplier(distributedSeq::get);
                } catch (CallNotPermittedException e) {
                    Counter.builder("cache.reconciliation.skipped")
                            .tag("cache", cacheName).tag("reason", "breaker-open")
                            .register(meterRegistry).increment();
                    return;
                } catch (Exception e) {
                    Counter.builder("cache.reconciliation.skipped")
                            .tag("cache", cacheName).tag("reason", "exception")
                            .register(meterRegistry).increment();
                    log.warn("Reconciliation recheck exception for cache '{}'; skipping",
                            cacheName, e);
                    return;
                }
                // Bracket the recheck GET too — see NearCache.reconcile(). Without
                // this sample the re-classification measured a fresh
                // redisSeqRecheck against a watermark snapshot taken before the
                // recheck round-trip, so publishes landing during that round-trip
                // read as missed and every suppressed regression became a miss.
                long recheckObservedAfter = lastObservedSeq.get();
                if (redisSeqRecheck >= observedBefore) {
                    Counter.builder("cache.reconciliation.seq.regression_recheck_resolved")
                            .tag("cache", cacheName).register(meterRegistry).increment();
                    redisSeq = redisSeqRecheck;
                    // Adopt the recheck's pair so the MISS branch logs the delta
                    // it decided on. observedBefore stays the REGRESSION anchor,
                    // and the branch condition means this call cannot return
                    // REGRESSION — a resolved regression must not re-open as one.
                    observedAfter = recheckObservedAfter;
                    decision = classifyBracketed(
                            redisSeq, observedBefore, observedAfter, tolerance);
                }
            }

            switch (decision) {
                case REGRESSION -> {
                    Counter.builder("cache.reconciliation.seq.regressions")
                            .tag("cache", cacheName).register(meterRegistry).increment();
                    log.warn("Reconciliation seq regressed on cache '{}': redisSeq={}, "
                            + "lastObservedSeq={}. Counter likely deleted by operator;"
                            + " resetting watermark.", cacheName, redisSeq, observedBefore);
                    lastObservedSeq.set(redisSeq);
                }
                case MISS -> {
                    Counter.builder("cache.reconciliation.misses.detected")
                            .tag("cache", cacheName).register(meterRegistry).increment();
                    log.info("Reconciliation declared miss on cache '{}': redisSeq={},"
                            + " lastObservedSeq={}, delta={}, tolerance={}. Forcing"
                            + " generation refresh.", cacheName, redisSeq, observedAfter,
                            redisSeq - observedAfter, tolerance);
                    // No L1 to clear; force generation refresh — repairs a
                    // possibly-stale locally-cached generation.
                    lastRefreshNanos.set(0);
                    currentGeneration();
                    // accumulateAndGet, not set — see NearCache's MISS branch:
                    // set() would discard a publisher self-bump that landed
                    // after the redisSeq read, walking the watermark backwards
                    // into a spurious MISS on the following cycle.
                    lastObservedSeq.accumulateAndGet(redisSeq, Math::max);
                }
                case NO_MISS -> { /* nothing to do */ }
            }
        } catch (Throwable t) {
            Counter.builder("cache.reconciliation.skipped")
                    .tag("cache", cacheName).tag("reason", "exception")
                    .register(meterRegistry).increment();
            log.warn("Reconciliation cycle threw unexpected error on cache '{}'", cacheName, t);
        } finally {
            sample.stop(cycleDuration);
            cyclesCompleted.increment();
        }
    }

    /**
     * Two-sample classification (1.0.3). Identical rule to
     * {@code NearCache.classifyBracketed} — REGRESSION is measured against
     * the pre-GET sample (the only one that can prove genuine counter loss),
     * MISS / NO_MISS against the post-GET sample (the only one that reflects
     * publishes this cycle actually covered), and the window between them is
     * benign by construction.
     */
    private static ReconciliationDecision classifyBracketed(
            long redisSeq, long observedBefore, long observedAfter, int tolerance) {
        if (ReconciliationDecision.classify(redisSeq, observedBefore, tolerance)
                == ReconciliationDecision.REGRESSION) {
            return ReconciliationDecision.REGRESSION;
        }
        ReconciliationDecision decision =
                ReconciliationDecision.classify(redisSeq, observedAfter, tolerance);
        return decision == ReconciliationDecision.REGRESSION
                ? ReconciliationDecision.NO_MISS
                : decision;
    }

    /** Test seam: returns the locally observed seq watermark. */
    public long lastObservedSeq() {
        return lastObservedSeq.get();
    }

    /** Test seam: true iff this cache participates in reconciliation. */
    public boolean isReconciliationEnabled() {
        return reconciliationEnabled;
    }

    /** Called by the cache manager during context shutdown. Idempotent. */
    public void shutdown() {
        dispatcher.deregister(cacheName);
    }

    private Object readFromRedis(String key) {
        try {
            return breaker.executeSupplier(() -> {
                Timer.Sample sample = Timer.start();
                try {
                    Object value = bucket(key).get();
                    if (value == null) {
                        misses.increment();
                    } else {
                        hits.increment();
                    }
                    return value;
                } finally {
                    sample.stop(getLatency);
                }
            });
        } catch (CallNotPermittedException e) {
            breakerOpen.increment();
            return null;
        } catch (Exception e) {
            failures.increment();
            log.warn("Distributed read failed for key '{}'", keyLogFormatter.format(key), e);
            return null;
        }
    }

    private RBucket<Object> bucket(String key) {
        String fullKey = CacheKeys.valueKey(cacheName, key, currentGeneration());
        return bucketCodec == null
                ? redisson.getBucket(fullKey)
                : redisson.getBucket(fullKey, bucketCodec);
    }

    private long currentGeneration() {
        long observed = lastRefreshNanos.get();
        long now = System.nanoTime();
        if (now - observed < GENERATION_REFRESH_NANOS) return localGeneration.get();
        if (!lastRefreshNanos.compareAndSet(observed, now)) return localGeneration.get();
        // Eventually-consistent refresh as of 1.0.1: dispatch getAsync and
        // return the cached localGeneration immediately. The next caller
        // observes the refreshed value, not this one. Forced by the
        // sync-on-event-loop bug — currentGeneration is reachable from a
        // Netty event-loop thread via bucket() on the async read/write paths.
        // Worst-case staleness is bounded by GENERATION_REFRESH_NANOS, which
        // already governs the cached-generation contract. Mirror of
        // NearCache.currentGeneration.
        Supplier<CompletionStage<Long>> getSupplier = () ->
                distributedGeneration.getAsync().toCompletableFuture();
        try {
            breaker.executeCompletionStage(getSupplier).toCompletableFuture()
                    .whenComplete((value, ex) -> {
                        if (ex == null) {
                            localGeneration.set(value);
                        } else {
                            // Refresh failed — reset so the next call retries
                            // rather than waiting a full interval on stale data.
                            lastRefreshNanos.compareAndSet(now, observed);
                        }
                    });
        } catch (CallNotPermittedException e) {
            lastRefreshNanos.compareAndSet(now, observed);
        }
        return localGeneration.get();
    }

    /**
     * Force the next {@link #currentGeneration()} call to refresh from
     * Redis instead of returning the cached value. Public surface as of
     * 0.5.0 — part of the reconciliation recovery contract. The
     * {@link Reconciler} calls this on a detected miss; the same path
     * {@code handleInvalidation(OP_CLEAR, ...)} already takes.
     */
    public void forceRefreshDue() {
        lastRefreshNanos.set(0);
    }

    /**
     * Return the locally cached generation snapshot. Public surface as
     * of 0.5.0 — used by the {@link Reconciler} to compare against the
     * canonical {@code <cache>:generation} value during a cycle.
     */
    public long localGeneration() {
        return localGeneration.get();
    }

    CircuitBreaker getBreaker() {
        return breaker;
    }

    // ---------- Async / reactive path (0.5.0) ----------
    //
    // Implements Spring 6.1's CompletableFuture<ValueWrapper>-typed
    // retrieve(...) methods so the cache layer is non-blocking when an
    // @Cacheable method has a Mono / Flux / CompletableFuture return type.
    // The defaults in the Cache interface wrap the synchronous get(...) in
    // CompletableFuture.supplyAsync(...) — that blocks the calling thread
    // and defeats the whole point of overriding here.

    /**
     * Async retrieve without a loader. Chains:
     * {@code RBucket.getAsync → wrap → CompletableFuture<ValueWrapper>}.
     * No thread switch — the chain stays on Redisson's Netty event loop
     * (no loader to invoke, so no hop needed). Hits and misses both
     * complete normally; a breaker-open / Redis-unreachable surfaces as
     * a {@code completedFuture(null)} (matches sync-path "L2 read failure
     * is treated as a miss").
     */
    @Override
    public CompletableFuture<?> retrieve(@Nonnull Object key) {
        String stringKey = CacheKeys.stringify(cacheName, key);
        return readFromRedisAsync(stringKey)
                .thenApply(value -> value == null ? null : new SimpleValueWrapper(value));
    }

    /**
     * Async retrieve with a loader. Chains:
     * {@code RBucket.getAsync → if hit: completedFuture(value);
     *  else: tryLockAsync → recheck-getAsync → loader-on-refreshExecutor → setAsync → publish}.
     *
     * <p>Single-flight via the shared {@link #inflight} map (guardrail
     * item 5): a sync caller and an async caller racing for the same key
     * converge on the same in-flight entry.
     *
     * <p>The returned future:
     * <ul>
     *   <li>Completes with the cached value on L2 hit OR with the loader
     *       result after the loader runs;</li>
     *   <li>Completes exceptionally with {@link Cache.ValueRetrievalException}
     *       wrapping the loader's cause on loader failure (matches
     *       sync-path exception type — guardrail item 3);</li>
     *   <li>Completes exceptionally with {@link LoaderRejectedException}
     *       (NOT wrapped) on async-loader-gate timeout.</li>
     * </ul>
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> CompletableFuture<T> retrieve(@Nonnull Object key,
                                              @Nonnull Supplier<CompletableFuture<T>> valueLoader) {
        String stringKey = CacheKeys.stringify(cacheName, key);
        return readFromRedisAsync(stringKey)
                .thenCompose(existing -> {
                    if (existing != null) return CompletableFuture.completedFuture((T) existing);
                    return loadAsyncWithSingleFlight(key, stringKey, valueLoader);
                });
    }

    /**
     * Async L2 read wrapped in {@link CircuitBreaker#executeCompletionStage}.
     * On breaker-open or Redis failure, the returned future completes with
     * {@code null} (matches sync-path "L2 read failure → treated as a miss"
     * — see § 4 failure-modes table). Metrics increment identically to the
     * sync path: {@code cache.distributed.gets{result=hit|miss}},
     * {@code cache.distributed.failures}, {@code cache.distributed.breaker.open}.
     */
    private CompletableFuture<Object> readFromRedisAsync(String key) {
        Timer.Sample sample = Timer.start();
        Supplier<CompletionStage<Object>> readSupplier = () -> {
            // bucket() reads currentGeneration() — must run on a thread
            // safe to call AtomicLong CAS on, which is any thread. The
            // Redisson async API is non-blocking from here on out.
            RBucket<Object> b = bucket(key);
            return b.getAsync().toCompletableFuture();
        };
        CompletableFuture<Object> chained;
        try {
            chained = breaker.executeCompletionStage(readSupplier).toCompletableFuture();
        } catch (CallNotPermittedException e) {
            // executeCompletionStage throws CallNotPermitted synchronously
            // when the breaker is open at submission time — convert to
            // a completed-with-null future so the read-as-miss contract
            // holds.
            breakerOpen.increment();
            sample.stop(getLatency);
            return CompletableFuture.completedFuture(null);
        }
        return chained
                .handle((value, ex) -> {
                    sample.stop(getLatency);
                    if (ex != null) {
                        Throwable cause = unwrapCompletion(ex);
                        if (cause instanceof CallNotPermittedException) {
                            breakerOpen.increment();
                        } else {
                            failures.increment();
                            log.warn("Distributed read failed for key '{}'",
                                    keyLogFormatter.format(key), cause);
                        }
                        return null;
                    }
                    if (value == null) {
                        misses.increment();
                    } else {
                        hits.increment();
                    }
                    return value;
                });
    }

    /**
     * Async cold-load path with two-tier single-flight (per-JVM via
     * {@link #inflight}, cross-node via {@link RLock#tryLockAsync}).
     * Falls through to local-only protection if Redis is slow / down /
     * breaker-open — matches sync-path semantics.
     */
    @SuppressWarnings("unchecked")
    private <T> CompletableFuture<T> loadAsyncWithSingleFlight(
            Object key, String stringKey, Supplier<CompletableFuture<T>> valueLoader) {
        CompletableFuture<Object> mine = new CompletableFuture<>();
        CompletableFuture<Object> theirs = inflight.putIfAbsent(stringKey, mine);
        if (theirs != null) {
            // Another thread (sync or async) is already loading. Wait on
            // its future; translate exceptions to match the async-path
            // exception contract.
            return (CompletableFuture<T>) theirs.handle((v, ex) -> {
                if (ex != null) {
                    throw asUncheckedAsyncFailure(key, valueLoader, ex);
                }
                return v;
            });
        }

        // We are the winner. Cross-node single-flight via tryLockAsync,
        // then loader hop, then setAsync write-through.
        Executor hop = refreshExecutor != null ? refreshExecutor : ForkJoinPool.commonPool();
        return loadCrossNodeAsync(key, stringKey, valueLoader, hop)
                .handle((value, ex) -> {
                    try {
                        if (ex != null) {
                            mine.completeExceptionally(ex);
                            throw asUncheckedAsyncFailure(key, valueLoader, ex);
                        }
                        mine.complete(value);
                        @SuppressWarnings("unchecked")
                        T cast = (T) value;
                        return cast;
                    } finally {
                        inflight.remove(stringKey, mine);
                    }
                });
    }

    private <T> CompletableFuture<Object> loadCrossNodeAsync(
            Object key, String stringKey,
            Supplier<CompletableFuture<T>> valueLoader, Executor hop) {
        if (spec.lockWait().isZero()) {
            // Lock disabled by configuration — go straight to the loader
            // hop. Local in-flight already protects per-JVM.
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
                        log.debug("Distributed lock acquisition failed for key '{}';"
                                + " proceeding with local single-flight only",
                                keyLogFormatter.format(stringKey), ex);
                        return Boolean.FALSE;
                    }
                    return acquired;
                })
                .thenCompose(acquired -> {
                    if (Boolean.TRUE.equals(acquired)) {
                        // Re-check after acquisition: another node may
                        // have populated L2 while we waited for the lock.
                        return readFromRedisAsync(stringKey)
                                .thenCompose(existing -> {
                                    if (existing != null) {
                                        return releaseLockAsync(lock)
                                                .thenApply(v -> existing);
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
        // The mandatory loader hop. thenComposeAsync(loader, hop) keeps
        // the loader off the Redisson Netty event loop (guardrail item 2).
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
                        // Async write-through. setAsync is breaker-wrapped
                        // via executeCompletionStage so failures count
                        // toward the breaker the same as sync writes.
                        writeToRedisAsync(stringKey, value).thenApply(ignored -> value))
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
                            log.warn("Failed to async-unlock cache='{}'", cacheName, ex);
                        }
                        return null;
                    });
        } catch (Exception e) {
            log.warn("Failed to invoke async-unlock for cache='{}'", cacheName, e);
            return CompletableFuture.completedFuture(null);
        }
    }

    private CompletableFuture<Void> writeToRedisAsync(String key, Object value) {
        Supplier<CompletionStage<Void>> writeSupplier = () -> {
            RBucket<Object> b = bucket(key);
            return b.setAsync(value, spec.ttl()).toCompletableFuture()
                    .thenApply(unused -> (Void) null);
        };
        try {
            return breaker.executeCompletionStage(writeSupplier).toCompletableFuture()
                    .handle((v, ex) -> {
                        if (ex != null) {
                            Throwable cause = unwrapCompletion(ex);
                            if (cause instanceof CallNotPermittedException) {
                                breakerOpen.increment();
                            } else {
                                failures.increment();
                                log.warn("Distributed async put failed for key '{}'",
                                        keyLogFormatter.format(key), cause);
                            }
                        }
                        return null;
                    });
        } catch (CallNotPermittedException e) {
            breakerOpen.increment();
            return CompletableFuture.completedFuture(null);
        }
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
