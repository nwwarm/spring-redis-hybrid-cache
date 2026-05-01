package io.github.nwwarm;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.Nonnull;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.cache.Cache;
import org.springframework.cache.caffeine.CaffeineCache;
import org.springframework.cache.support.AbstractCacheManager;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Tier-aware {@link org.springframework.cache.CacheManager} that dispatches each
 * cache name to {@link LocalOnlyCache}, {@link DistributedOnlyCache}, or
 * {@link NearCache} based on {@link CacheProperties.CacheSpec#tier()}.
 *
 * <p>Caches are built lazily on first access via
 * {@link #getMissingCache(String)}; {@link AbstractCacheManager} handles
 * thread-safe caching of {@link Cache} instances per name. {@code @Cacheable}
 * resolves through Spring's default {@code SimpleCacheResolver}, which calls
 * this manager — so a single bean replaces the previous
 * {@code CacheResolver} + {@code CacheManager} pair.
 */
public class HybridCacheManager extends AbstractCacheManager implements DisposableBean {

    private final CacheProperties properties;
    private final RedissonClient redisson;
    private final CircuitBreaker breaker;
    private final InvalidationDispatcher dispatcher;
    private final MeterRegistry meterRegistry;
    private final CodecResolver codecResolver;

    private final List<NearCache> nearCaches = new CopyOnWriteArrayList<>();

    public HybridCacheManager(CacheProperties properties,
                              RedissonClient redisson,
                              CircuitBreaker breaker,
                              InvalidationDispatcher dispatcher,
                              MeterRegistry meterRegistry,
                              CodecResolver codecResolver) {
        // Fail fast on configured cache names that contain ':'. Names that
        // fall through to defaultSpec (i.e., not listed in cache.caches.*)
        // are validated lazily in getMissingCache.
        properties.caches().keySet().forEach(CacheKeys::validateCacheName);
        this.properties = properties;
        this.redisson = redisson;
        this.breaker = breaker;
        this.dispatcher = dispatcher;
        this.meterRegistry = meterRegistry;
        this.codecResolver = codecResolver;
    }

    @Override
    @Nonnull
    protected Collection<? extends Cache> loadCaches() {
        // Caches are built lazily via getMissingCache on first @Cacheable lookup;
        // we can't preload because cache names are discovered from annotations
        // at runtime, not from CacheProperties (which only carries per-name
        // overrides — names not listed there fall back to defaultSpec).
        return Collections.emptyList();
    }

    @Override
    protected Cache getMissingCache(@Nonnull String name) {
        CacheKeys.validateCacheName(name);
        CacheProperties.CacheSpec spec = properties.specFor(name);
        org.redisson.client.codec.Codec bucketCodec = codecResolver.resolve(spec.codec());
        return switch (spec.tier()) {
            case LOCAL_ONLY -> new LocalOnlyCache(buildCaffeineCache(name, spec), meterRegistry);
            case DISTRIBUTED_ONLY ->
                    new DistributedOnlyCache(name, spec, bucketCodec, redisson, breaker, meterRegistry);
            case NEAR_CACHE -> {
                NearCache near = new NearCache(
                        buildCaffeineCache(name, spec),
                        spec, bucketCodec, redisson, breaker, dispatcher, meterRegistry);
                nearCaches.add(near);
                yield near;
            }
        };
    }

    private CaffeineCache buildCaffeineCache(String name, CacheProperties.CacheSpec spec) {
        com.github.benmanes.caffeine.cache.Cache<Object, Object> nativeCache = Caffeine.newBuilder()
                .expireAfterWrite(spec.ttl())
                .maximumSize(spec.maximumSize())
                .recordStats()
                .build();
        return new CaffeineCache(name, nativeCache, true);
    }

    @Override
    public void destroy() {
        nearCaches.forEach(NearCache::shutdown);
        nearCaches.clear();
    }
}
