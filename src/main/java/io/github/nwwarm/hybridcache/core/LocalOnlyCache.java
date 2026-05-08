package io.github.nwwarm.hybridcache.core;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import jakarta.annotation.Nonnull;
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * Caffeine-only cache with no distributed layer.
 *
 * <p>Use for caches that are inherently per-node and do not need cross-node
 * coherence — per-instance rate limiters, ephemeral lookups, request-scoped
 * memoization. The library treats this as a thin pass-through to the underlying
 * Spring {@link Cache}; the wrapper exists so per-cache metrics labels are
 * applied uniformly across all three tiers.
 */
public class LocalOnlyCache implements HybridCache {

    private final Cache delegate;
    private final LoaderGate loaderGate;
    private final AsyncLoaderGate asyncLoaderGate;
    /**
     * Per-JVM single-flight map shared between sync and async loader paths
     * (0.5.0 async). § 10 binding: a sync caller and an async caller racing
     * for the same key on the same JVM converge on the same in-flight
     * entry, so only one runs the loader. Splitting this map for "type
     * purity" (one for sync, one for async) re-introduces the doubled-loader
     * hazard the helper exists to close.
     */
    private final ConcurrentMap<String, CompletableFuture<Object>> inflight =
            new ConcurrentHashMap<>();
    /** Refresh executor for the async-path loader hop. May be null in tests/back-compat. */
    private final Executor refreshExecutor;
    /** Non-null only when {@code spec.swr() != null}. */
    private final SwrSidecar swrSidecar;
    /**
     * Non-null only when {@code spec.refreshAhead().enabled()}. When
     * non-null, {@link #swrSidecar} is also non-null (validator E38).
     */
    private final RefreshAheadCoordinator refreshAhead;
    /** Per-cache EWMA shared between SWR and RA refresh paths. Null when neither feature is configured. */
    private final LoaderRuntimeEwma loaderEwma;

    public LocalOnlyCache(Cache delegate, MeterRegistry meterRegistry) {
        this(delegate, null, null, meterRegistry, null);
    }

    public LocalOnlyCache(Cache delegate,
                          Integer maxConcurrentLoaders,
                          java.time.Duration loaderAcquireTimeout,
                          MeterRegistry meterRegistry) {
        this(delegate, maxConcurrentLoaders, loaderAcquireTimeout, meterRegistry, null);
    }

    /**
     * Constructor with SWR support (0.5.0). The {@code swrSidecar} may
     * be {@code null} when SWR is not configured for this cache.
     * {@link io.github.nwwarm.hybridcache.core.HybridCacheManager} builds
     * the sidecar before this constructor so the Caffeine
     * {@code RemovalListener} on {@code delegate}'s underlying native
     * cache can wire {@link SwrSidecar#onRemoval(String)} directly.
     */
    public LocalOnlyCache(Cache delegate,
                          Integer maxConcurrentLoaders,
                          java.time.Duration loaderAcquireTimeout,
                          MeterRegistry meterRegistry,
                          SwrSidecar swrSidecar) {
        this(delegate, maxConcurrentLoaders, loaderAcquireTimeout, meterRegistry,
                swrSidecar, null, null);
    }

    /**
     * Full constructor with SWR + RA support (0.5.0). RA is enabled when
     * {@code refreshAhead != null}; in that case {@code swrSidecar} MUST
     * also be non-null (validator E38: RA requires SWR). The shared
     * {@code loaderEwma} estimator is updated on every loader completion
     * (sync path and SWR/RA refresh paths) so RA's predicate sees a
     * current estimate regardless of which trigger has fired so far.
     */
    public LocalOnlyCache(Cache delegate,
                          Integer maxConcurrentLoaders,
                          java.time.Duration loaderAcquireTimeout,
                          MeterRegistry meterRegistry,
                          SwrSidecar swrSidecar,
                          RefreshAheadCoordinator refreshAhead,
                          LoaderRuntimeEwma loaderEwma) {
        this(delegate, maxConcurrentLoaders, loaderAcquireTimeout,
                meterRegistry, swrSidecar, refreshAhead, loaderEwma, null);
    }

    /**
     * Async-aware constructor (0.5.0). The {@code refreshExecutor} is used
     * for the loader hop on {@link #retrieve(Object, Supplier)}; without
     * it, the async path's loader would run on whatever thread the
     * caller's chain landed on (typically the calling thread for a
     * cold-load on this tier, since LOCAL_ONLY has no L2 to chain off of —
     * but pinning the hop matches the binding async pattern from § 2 /
     * § 10 and matches the {@link NearCache} / {@link DistributedOnlyCache}
     * implementations).
     */
    public LocalOnlyCache(Cache delegate,
                          Integer maxConcurrentLoaders,
                          java.time.Duration loaderAcquireTimeout,
                          MeterRegistry meterRegistry,
                          SwrSidecar swrSidecar,
                          RefreshAheadCoordinator refreshAhead,
                          LoaderRuntimeEwma loaderEwma,
                          Executor refreshExecutor) {
        this.delegate = delegate;
        this.loaderGate = new LoaderGate(delegate.getName(),
                maxConcurrentLoaders, loaderAcquireTimeout, meterRegistry);
        this.asyncLoaderGate = new AsyncLoaderGate(delegate.getName(),
                maxConcurrentLoaders, loaderAcquireTimeout, meterRegistry);
        this.swrSidecar = swrSidecar;
        this.refreshAhead = refreshAhead;
        this.loaderEwma = loaderEwma;
        this.refreshExecutor = refreshExecutor;
        Object native_ = delegate.getNativeCache();
        if (native_ instanceof com.github.benmanes.caffeine.cache.Cache<?, ?> caffeineNative) {
            CaffeineCacheMetrics.monitor(meterRegistry, caffeineNative, delegate.getName());
        }
    }

    @Override
    @Nonnull
    public String getName() {
        return delegate.getName();
    }

    @Override
    @Nonnull
    public Object getNativeCache() {
        return delegate.getNativeCache();
    }

    @Override
    public ValueWrapper get(@Nonnull Object key) {
        // No loader on this code path: even on a stale L1 hit we cannot
        // dispatch a refresh (nothing to call). Return the wrapper as-is;
        // a subsequent loader-aware caller will trigger refresh.
        return delegate.get(key);
    }

    @Override
    public <T> T get(@Nonnull Object key, Class<T> type) {
        return delegate.get(key, type);
    }

    @Override
    public <T> T get(@Nonnull Object key, @Nonnull Callable<T> valueLoader) {
        // SWR/RA fast path: an L1 hit can be fresh (synchronous return; RA
        // may speculatively refresh) or stale (synchronous return AND SWR
        // dispatches async refresh). The wrapper inspection happens before
        // delegate.get(..., valueLoader) so the loader does not run on the
        // synchronous path.
        if (swrSidecar != null) {
            ValueWrapper wrapper = delegate.get(key);
            if (wrapper != null) {
                String sk = stringKey(key);
                Callable<Object> refreshTask = () -> {
                    long start = System.nanoTime();
                    try {
                        // Loader through the same gate the sync path uses
                        // (guardrail item 5). Refresh writes back via
                        // put() so the SWR sidecar deadline is updated and
                        // visible before the cached value (guardrail item 2).
                        Object value = loaderGate.run(key, valueLoader);
                        put(key, value);
                        return value;
                    } finally {
                        if (loaderEwma != null) {
                            loaderEwma.record(System.nanoTime() - start);
                        }
                    }
                };
                SwrSidecar.Classification classification = swrSidecar.classify(sk);
                if (classification == SwrSidecar.Classification.STALE) {
                    swrSidecar.maybeDispatchRefresh(sk, refreshTask);
                } else if (refreshAhead != null) {
                    // FRESH classification + RA enabled: XFetch may fire a
                    // speculative refresh. The shared in-flight map ensures
                    // a single key never has both SWR and RA refreshes in
                    // flight at once.
                    refreshAhead.evaluateAndMaybeDispatch(sk, refreshTask);
                }
                @SuppressWarnings("unchecked")
                T cast = (T) wrapper.get();
                return cast;
            }
        }
        // Wrap the loader so the per-cache concurrency cap gates Caffeine's
        // compute lambda. When the gate is unconfigured, LoaderGate.run is a
        // direct loader.call() — same control flow as the pre-feature
        // pass-through.
        //
        // Spring's CaffeineCache.LoadFunction wraps any Throwable from the
        // inner Callable in Cache.ValueRetrievalException. Unwrap on the way
        // out so a rejection surfaces as itself — callers want to distinguish
        // "loader rejected" from "loader ran and failed."
        try {
            return delegate.get(key, () -> {
                long start = System.nanoTime();
                try {
                    Object loaded = loaderGate.run(key, valueLoader);
                    // Sidecar pre-write before Caffeine's compute populates
                    // the entry (guardrail item 2). For LOCAL_ONLY, no L2 /
                    // peers — the loader-completion path writes the deadline
                    // and Caffeine writes the value atomically.
                    if (swrSidecar != null) swrSidecar.recordWrite(stringKey(key));
                    @SuppressWarnings("unchecked")
                    T typed = (T) loaded;
                    return typed;
                } finally {
                    if (loaderEwma != null) {
                        loaderEwma.record(System.nanoTime() - start);
                    }
                }
            });
        } catch (Cache.ValueRetrievalException e) {
            if (e.getCause() instanceof LoaderRejectedException lr) throw lr;
            throw e;
        }
    }

    /** Test seam: returns the RA coordinator when configured. */
    public RefreshAheadCoordinator refreshAhead() {
        return refreshAhead;
    }

    /** Test seam: returns the per-cache loader-runtime EWMA. */
    public LoaderRuntimeEwma loaderEwma() {
        return loaderEwma;
    }

    // ---------- Async / reactive path (0.5.0) ----------
    //
    // Spring 6.1's CompletableFuture<ValueWrapper>-typed retrieve(...) hooks
    // are the entry points Spring's CacheInterceptor calls when an
    // @Cacheable method has a Mono / Flux / CompletableFuture return type.
    // The defaults in the Cache interface wrap the synchronous get(...) in
    // CompletableFuture.supplyAsync(...), which blocks the calling thread
    // for the whole load — the whole point of overriding here is to NOT do
    // that. See # implementation guardrails / ## Async/reactive.
    //
    // For LOCAL_ONLY there is no L2 to chain off of, so the async surface
    // is "L1 hit returns completed future; L1 miss runs the loader on
    // refreshExecutor." Single-flight goes through the shared inflight
    // map so a sync caller and an async caller racing for the same key
    // collapse onto one loader.

    /**
     * Async retrieve without a loader. L1 hit returns a completed future
     * holding the cached value; miss returns a completed future of
     * {@code null}. No thread switch — Caffeine reads are synchronous and
     * cheap, and the calling thread already has the answer.
     *
     * <p>Per § 10 / guardrail item 6, an L1 entry whose value is
     * {@code null} (a cached negative result) returns a future with the
     * same wrapper Spring's interceptor expects, so {@code unless = "#result == null"}
     * SpEL clauses see "cached null" rather than "no cached value."
     */
    @Override
    public CompletableFuture<?> retrieve(@Nonnull Object key) {
        ValueWrapper wrapper = delegate.get(key);
        if (wrapper == null) return CompletableFuture.completedFuture(null);
        // SWR / RA refresh dispatch on hit (loader-less retrieve cannot
        // refresh — same as the sync get(key) path). The wrapper is the
        // synchronous answer.
        return CompletableFuture.completedFuture(wrapper);
    }

    /**
     * Async retrieve with a loader. L1 hit returns a completed future;
     * L1 miss runs the loader on the shared refresh executor (mandatory
     * loader hop per guardrail item 2 — even on LOCAL_ONLY, where there
     * is no Netty event loop, the hop keeps the loader off the calling
     * thread which may be a Reactor thread).
     *
     * <p>Single-flight via the shared {@link #inflight} map (guardrail
     * item 5): a sync and async caller racing for the same key converge
     * on one in-flight entry.
     *
     * <p>The returned future:
     * <ul>
     *   <li>Completes with the cached value (a {@link ValueWrapper})
     *       on L1 hit OR after the loader runs;</li>
     *   <li>Completes exceptionally with {@link Cache.ValueRetrievalException}
     *       wrapping the loader's cause on loader failure (matches
     *       sync-path exception type — guardrail item 3);</li>
     *   <li>Completes exceptionally with {@link LoaderRejectedException}
     *       (NOT wrapped) on async-loader-gate timeout (guardrail item 3).</li>
     * </ul>
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> CompletableFuture<T> retrieve(@Nonnull Object key,
                                              @Nonnull Supplier<CompletableFuture<T>> valueLoader) {
        ValueWrapper wrapper = delegate.get(key);
        if (wrapper != null) {
            String sk = stringKey(key);
            // Build the same shared refresh task body the sync path uses,
            // adapted for async execution. SWR (STALE) and RA (FRESH +
            // predicate) dispatch through their existing coordinators —
            // same metrics, same in-flight collapse.
            if (swrSidecar != null) {
                Callable<Object> refreshTask = buildAsyncRefreshTaskBody(key, sk, valueLoader);
                SwrSidecar.Classification c = swrSidecar.classify(sk);
                if (c == SwrSidecar.Classification.STALE) {
                    swrSidecar.maybeDispatchRefresh(sk, refreshTask);
                } else if (refreshAhead != null) {
                    refreshAhead.evaluateAndMaybeDispatch(sk, refreshTask);
                }
            }
            // Wrap the value in a future. Cached null returns a future
            // that resolves to the wrapper itself (which Spring's
            // CacheInterceptor will distinguish from "no cached value").
            T value = (T) wrapper.get();
            return CompletableFuture.completedFuture(value);
        }
        // Cold-load. Single-flight via the shared inflight map; per § 10
        // sync and async callers converge on the same entry.
        String sk = stringKey(key);
        CompletableFuture<Object> mine = new CompletableFuture<>();
        CompletableFuture<Object> theirs = inflight.putIfAbsent(sk, mine);
        if (theirs != null) {
            // Another thread is already loading. Chain onto its future
            // and translate any exception to match the async-path
            // exception contract.
            return (CompletableFuture<T>) theirs.handle((v, ex) -> {
                if (ex != null) {
                    throw asUncheckedAsyncFailure(key, valueLoader, ex);
                }
                // The winner stored the raw value (or NullValue) in the
                // shared inflight future. Materialize it via delegate so
                // we observe the same SimpleValueWrapper contract.
                return v;
            });
        }
        // We are the winner. Run the loader off the calling thread and
        // populate the L1 cache + complete the inflight future.
        long start = System.nanoTime();
        Supplier<CompletableFuture<Object>> gatedLoader = () -> {
            CompletableFuture<T> userFuture;
            try {
                userFuture = valueLoader.get();
                if (userFuture == null) {
                    userFuture = CompletableFuture.completedFuture(null);
                }
            } catch (Throwable t) {
                CompletableFuture<Object> failed = new CompletableFuture<>();
                failed.completeExceptionally(t);
                return failed;
            }
            // Erase to Object so the gate can manage permits uniformly.
            return userFuture.thenApply(v -> (Object) v);
        };
        // Loader hop is mandatory: thenComposeAsync(gatedLoader,
        // refreshExecutor) keeps the loader off the calling thread.
        // When refreshExecutor is null (test back-compat path), fall back
        // to ForkJoinPool.commonPool — same execution shape, slightly
        // less control over thread name.
        Executor hop = refreshExecutor != null ? refreshExecutor : java.util.concurrent.ForkJoinPool.commonPool();
        return CompletableFuture.completedFuture((Void) null)
                .thenComposeAsync(v -> asyncLoaderGate.run(key, gatedLoader), hop)
                .handle((value, ex) -> {
                    try {
                        if (ex != null) {
                            mine.completeExceptionally(ex);
                            // Match sync-path exception types: bare
                            // LoaderRejectedException flows through;
                            // anything else wraps in ValueRetrievalException.
                            throw asUncheckedAsyncFailure(key, valueLoader, ex);
                        }
                        // Sidecar pre-write before the value becomes
                        // visible (guardrail item 2 of SWR section).
                        if (swrSidecar != null) swrSidecar.recordWrite(sk);
                        delegate.put(key, value);
                        if (loaderEwma != null) {
                            loaderEwma.record(System.nanoTime() - start);
                        }
                        mine.complete(value);
                        @SuppressWarnings("unchecked")
                        T cast = (T) value;
                        return cast;
                    } finally {
                        inflight.remove(sk, mine);
                    }
                });
    }

    /**
     * Builds an async refresh task body suitable for SWR / RA dispatch
     * from the async path. Same shape as the sync-path refresh task
     * (loader through gate, write through put(), update EWMA), but the
     * loader is invoked through the async path so the SWR/RA executor
     * thread doesn't block on a synchronous loader.
     *
     * <p>Note: the SwrSidecar / RefreshAheadCoordinator dispatch contract
     * is {@code Callable<Object>} — a synchronous callable. We adapt by
     * having the callable join the loader future. This is acceptable
     * because the callable runs on the SWR/RA refresh-executor thread,
     * not on a Netty event loop or a Reactor thread; blocking that thread
     * is exactly what it's there for. The "loader hop" guardrail concerns
     * the calling thread for {@link #retrieve(Object, Supplier)}, not
     * the refresh-task body.
     */
    private <T> Callable<Object> buildAsyncRefreshTaskBody(
            Object key, String stringKey, Supplier<CompletableFuture<T>> valueLoader) {
        return () -> {
            long start = System.nanoTime();
            try {
                // Reuse the same async gate so the per-cache concurrency
                // cap applies to refresh-driven loads too.
                final CompletableFuture<T> userFuture;
                try {
                    CompletableFuture<T> raw = valueLoader.get();
                    userFuture = raw == null ? CompletableFuture.completedFuture(null) : raw;
                } catch (Throwable t) {
                    throw t instanceof Exception ? (Exception) t : new RuntimeException(t);
                }
                // SWR / RA refresh dispatches expect a synchronous
                // result; the executor thread blocks on the future here.
                Object value;
                try {
                    value = asyncLoaderGate.run(key, () -> userFuture.thenApply(v -> (Object) v))
                            .toCompletableFuture().join();
                } catch (java.util.concurrent.CompletionException ce) {
                    Throwable cause = ce.getCause() != null ? ce.getCause() : ce;
                    if (cause instanceof Exception ex) throw ex;
                    throw new RuntimeException(cause);
                }
                put(key, value);
                return value;
            } finally {
                if (loaderEwma != null) {
                    loaderEwma.record(System.nanoTime() - start);
                }
            }
        };
    }

    /**
     * Translates an exception from the async loader chain into the
     * sync-path-equivalent type so {@code @Cacheable} callers catch on
     * the same type whether they used a sync or async return type.
     * <ul>
     *   <li>{@link LoaderRejectedException} flows through unwrapped
     *       (matches sync path: gate rejection is a distinct failure mode
     *       from loader failure).</li>
     *   <li>Anything else wraps in {@link Cache.ValueRetrievalException}.</li>
     * </ul>
     */
    private RuntimeException asUncheckedAsyncFailure(
            Object key, Supplier<?> loader, Throwable ex) {
        Throwable cause = unwrapCompletion(ex);
        if (cause instanceof LoaderRejectedException lr) return lr;
        // ValueRetrievalException's constructor expects a Callable; we
        // adapt the Supplier with a stub that surfaces the same loader
        // identity in any toString — operators see the same object class
        // either way. The Callable is never actually invoked by the
        // exception (it's stored for diagnostics), so wrapping with
        // a Supplier-to-Callable adapter is safe.
        Callable<?> loaderAdapter = loader::get;
        return new Cache.ValueRetrievalException(key, loaderAdapter, cause);
    }

    private static Throwable unwrapCompletion(Throwable t) {
        Throwable cur = t;
        while (cur instanceof java.util.concurrent.CompletionException
                || cur instanceof java.util.concurrent.ExecutionException) {
            Throwable c = cur.getCause();
            if (c == null || c == cur) break;
            cur = c;
        }
        return cur;
    }

    @Override
    public void put(@Nonnull Object key, Object value) {
        // SWR sidecar write before the value becomes visible (guardrail
        // item 2). Same ordering as NearCache.put.
        if (swrSidecar != null) swrSidecar.recordWrite(stringKey(key));
        delegate.put(key, value);
    }

    /**
     * LOCAL_ONLY does not stringify-and-validate the way the L2 tiers do
     * (no Redis key to collide with the {@code ':'} separator). For SWR
     * sidecar lookup, an internally-consistent string representation is
     * sufficient; we use {@link Object#toString()} the same way Caffeine
     * computes equality keys.
     */
    private static String stringKey(Object key) {
        return key == null ? "_null" : key.toString();
    }

    @Override
    public void evict(@Nonnull Object key) {
        delegate.evict(key);
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public void clearImmediate() {
        // No distributed state to reconcile — Caffeine.invalidateAll() already
        // drops every entry synchronously, so there are no orphans to chase.
        delegate.clear();
    }
}
