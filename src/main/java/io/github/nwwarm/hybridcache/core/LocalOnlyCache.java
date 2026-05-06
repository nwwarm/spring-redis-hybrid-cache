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

    public LocalOnlyCache(Cache delegate, MeterRegistry meterRegistry) {
        this(delegate, null, null, meterRegistry);
    }

    public LocalOnlyCache(Cache delegate,
                          Integer maxConcurrentLoaders,
                          java.time.Duration loaderAcquireTimeout,
                          MeterRegistry meterRegistry) {
        this.delegate = delegate;
        this.loaderGate = new LoaderGate(delegate.getName(),
                maxConcurrentLoaders, loaderAcquireTimeout, meterRegistry);
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
        return delegate.get(key);
    }

    @Override
    public <T> T get(@Nonnull Object key, Class<T> type) {
        return delegate.get(key, type);
    }

    @Override
    public <T> T get(@Nonnull Object key, @Nonnull Callable<T> valueLoader) {
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
            return delegate.get(key, () -> loaderGate.run(key, valueLoader));
        } catch (Cache.ValueRetrievalException e) {
            if (e.getCause() instanceof LoaderRejectedException lr) throw lr;
            throw e;
        }
    }

    @Override
    public void put(@Nonnull Object key, Object value) {
        delegate.put(key, value);
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
