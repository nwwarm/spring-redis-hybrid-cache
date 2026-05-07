package io.github.nwwarm.hybridcache.core;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import jakarta.annotation.Nonnull;
import org.springframework.cache.Cache;

import java.util.concurrent.Callable;

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
    /** Non-null only when {@code spec.swr() != null}. */
    private final SwrSidecar swrSidecar;

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
        this.delegate = delegate;
        this.loaderGate = new LoaderGate(delegate.getName(),
                maxConcurrentLoaders, loaderAcquireTimeout, meterRegistry);
        this.swrSidecar = swrSidecar;
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
        // SWR fast path: an L1 hit can either be fresh (return synchronously,
        // no extra work) or stale (return synchronously AND dispatch async
        // refresh). The wrapper inspection happens before delegate.get(...,
        // valueLoader) so the loader does not run on the synchronous path.
        if (swrSidecar != null) {
            ValueWrapper wrapper = delegate.get(key);
            if (wrapper != null) {
                if (swrSidecar.classify(stringKey(key)) == SwrSidecar.Classification.STALE) {
                    swrSidecar.maybeDispatchRefresh(stringKey(key), () -> {
                        // Loader through the same gate the sync path uses
                        // (guardrail item 5). Refresh writes back via
                        // put() so the SWR sidecar deadline is updated and
                        // visible before the cached value (guardrail item 2).
                        Object value = loaderGate.run(key, valueLoader);
                        put(key, value);
                        return value;
                    });
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
                Object loaded = loaderGate.run(key, valueLoader);
                // Sidecar pre-write before Caffeine's compute populates
                // the entry (guardrail item 2). For LOCAL_ONLY, no L2 /
                // peers — the loader-completion path writes the deadline
                // and Caffeine writes the value atomically.
                if (swrSidecar != null) swrSidecar.recordWrite(stringKey(key));
                @SuppressWarnings("unchecked")
                T typed = (T) loaded;
                return typed;
            });
        } catch (Cache.ValueRetrievalException e) {
            if (e.getCause() instanceof LoaderRejectedException lr) throw lr;
            throw e;
        }
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
