package io.github.nwwarm;

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
public class LocalOnlyCache implements Cache {

    private final Cache delegate;

    public LocalOnlyCache(Cache delegate, MeterRegistry meterRegistry) {
        this.delegate = delegate;
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
        return delegate.get(key, valueLoader);
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
}
