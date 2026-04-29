package io.github.nwwarm;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.cache.caffeine.CaffeineCacheManager;

/**
 * Caffeine cache manager that consults {@link CacheProperties} to build a
 * per-name {@link com.github.benmanes.caffeine.cache.Cache} with cache-specific
 * TTL and {@code maximumSize}.
 *
 * <p>Statistics recording is enabled on every cache so Micrometer's
 * {@code CaffeineCacheMetrics} binding produces non-zero values.
 */
public class PerNameCaffeineCacheManager extends CaffeineCacheManager {

    private final CacheProperties properties;

    public PerNameCaffeineCacheManager(CacheProperties properties) {
        this.properties = properties;
        setAllowNullValues(true);
    }

    @Override
    protected com.github.benmanes.caffeine.cache.Cache<Object, Object> createNativeCaffeineCache(String name) {
        CacheProperties.CacheSpec spec = properties.specFor(name);
        return Caffeine.newBuilder()
                .expireAfterWrite(spec.ttl())
                .maximumSize(spec.maximumSize())
                .recordStats()
                .build();
    }
}
