package io.github.nwwarm;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.Nonnull;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.interceptor.CacheOperationInvocationContext;
import org.springframework.cache.interceptor.CacheResolver;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves cache names to the appropriate tier-specific {@link Cache} implementation
 * based on {@link CacheProperties}. Caches are constructed lazily on first use and
 * cached in {@link #cacheMap} for the lifetime of the application context.
 *
 * <p>Tier dispatch happens here rather than in application code, so the same
 * {@code @Cacheable("foo")} annotation produces tier-appropriate behavior based
 * solely on YAML configuration.
 */
public class LocalCacheResolver implements CacheResolver, DisposableBean {

    private final CacheManager cacheManager;
    private final CacheProperties properties;
    private final RedissonClient redisson;
    private final CircuitBreaker breaker;
    private final InvalidationDispatcher dispatcher;
    private final MeterRegistry meterRegistry;

    private final Map<String, Cache> cacheMap = new ConcurrentHashMap<>();

    public LocalCacheResolver(CacheManager cacheManager,
                              CacheProperties properties,
                              RedissonClient redisson,
                              CircuitBreaker breaker,
                              InvalidationDispatcher dispatcher,
                              MeterRegistry meterRegistry) {
        this.cacheManager = cacheManager;
        this.properties = properties;
        this.redisson = redisson;
        this.breaker = breaker;
        this.dispatcher = dispatcher;
        this.meterRegistry = meterRegistry;
    }

    @Override
    @Nonnull
    public Collection<? extends Cache> resolveCaches(
            @Nonnull CacheOperationInvocationContext<?> context) {
        return context.getOperation().getCacheNames().stream()
                .map(this::resolve)
                .filter(Objects::nonNull)
                .toList();
    }

    private Cache resolve(String name) {
        return cacheMap.computeIfAbsent(name, this::build);
    }

    private Cache build(String name) {
        CacheProperties.CacheSpec spec = properties.specFor(name);

        return switch (spec.tier()) {
            case LOCAL_ONLY -> {
                Cache caffeineCache = cacheManager.getCache(name);
                if (caffeineCache == null) {
                    throw new IllegalStateException("CacheManager produced null for '" + name + "'");
                }
                yield new LocalOnlyCache(caffeineCache, meterRegistry);
            }
            case DISTRIBUTED_ONLY ->
                    new DistributedOnlyCache(name, spec, redisson, breaker, meterRegistry);
            case NEAR_CACHE -> {
                Cache caffeineCache = cacheManager.getCache(name);
                if (caffeineCache == null) {
                    throw new IllegalStateException("CacheManager produced null for '" + name + "'");
                }
                yield new NearCache(caffeineCache, spec, redisson, breaker, dispatcher, meterRegistry);
            }
        };
    }

    @Override
    public void destroy() {
        cacheMap.values().forEach(c -> {
            if (c instanceof NearCache near) {
                near.shutdown();
            }
        });
        cacheMap.clear();
    }

    /** Exposed for diagnostics; identifies this JVM in invalidation messages. */
    public String getNodeId() {
        return dispatcher.getNodeId();
    }
}
