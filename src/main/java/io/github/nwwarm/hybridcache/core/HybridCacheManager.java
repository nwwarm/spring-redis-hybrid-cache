package io.github.nwwarm.hybridcache.core;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.nwwarm.hybridcache.config.CacheProperties;
import io.github.nwwarm.hybridcache.invalidation.InvalidationDispatcher;
import io.github.nwwarm.hybridcache.preloader.PreloaderCoordinator;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
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
 *
 * <p>Each cache resolves its own {@link CircuitBreaker} from the shared
 * {@link CircuitBreakerRegistry} at construction time so a noisy cache trips
 * only its own breaker, leaving every other cache's L2 path open.
 */
public class HybridCacheManager extends AbstractCacheManager implements DisposableBean {

    private final CacheProperties properties;
    private final RedissonClient redisson;
    private final CircuitBreakerFactory breakerFactory;
    private final InvalidationDispatcher dispatcher;
    private final MeterRegistry meterRegistry;
    private final CodecResolver codecResolver;
    private final KeyLogFormatter keyLogFormatter;
    private final PreloaderCoordinator preloaderCoordinator;

    private final List<NearCache> nearCaches = new CopyOnWriteArrayList<>();
    private final List<DistributedOnlyCache> distributedCaches = new CopyOnWriteArrayList<>();

    public HybridCacheManager(CacheProperties properties,
                              RedissonClient redisson,
                              CircuitBreakerRegistry circuitBreakerRegistry,
                              InvalidationDispatcher dispatcher,
                              MeterRegistry meterRegistry,
                              CodecResolver codecResolver,
                              KeyLogFormatter keyLogFormatter) {
        this(properties, redisson, circuitBreakerRegistry, dispatcher,
                meterRegistry, codecResolver, keyLogFormatter, null);
    }

    public HybridCacheManager(CacheProperties properties,
                              RedissonClient redisson,
                              CircuitBreakerRegistry circuitBreakerRegistry,
                              InvalidationDispatcher dispatcher,
                              MeterRegistry meterRegistry,
                              CodecResolver codecResolver,
                              KeyLogFormatter keyLogFormatter,
                              PreloaderCoordinator preloaderCoordinator) {
        // Fail fast on configured cache names that contain ':'. Names that
        // fall through to defaultSpec (i.e., not listed in cache.caches.*)
        // are validated lazily in getMissingCache.
        properties.caches().keySet().forEach(CacheKeys::validateCacheName);
        this.properties = properties;
        this.redisson = redisson;
        this.breakerFactory = new CircuitBreakerFactory(circuitBreakerRegistry);
        this.dispatcher = dispatcher;
        this.meterRegistry = meterRegistry;
        this.codecResolver = codecResolver;
        this.keyLogFormatter = keyLogFormatter;
        this.preloaderCoordinator = preloaderCoordinator;
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
            case LOCAL_ONLY -> new LocalOnlyCache(buildCaffeineCache(name, spec),
                    spec.maxConcurrentLoaders(), spec.loaderAcquireTimeout(), meterRegistry);
            case DISTRIBUTED_ONLY -> {
                CircuitBreaker breaker = breakerFactory.resolve(name, spec.circuitBreaker());
                DistributedOnlyCache distributed = new DistributedOnlyCache(
                        name, spec, bucketCodec, redisson, breaker, dispatcher, meterRegistry, keyLogFormatter);
                distributedCaches.add(distributed);
                yield distributed;
            }
            case NEAR_CACHE -> {
                CircuitBreaker breaker = breakerFactory.resolve(name, spec.circuitBreaker());
                NearCache near = new NearCache(
                        buildCaffeineCache(name, spec),
                        spec, bucketCodec, redisson, breaker, dispatcher, meterRegistry, keyLogFormatter);
                nearCaches.add(near);
                // Preloader registration runs *after* the cache is fully
                // built and the dispatcher has registered the listener
                // (NearCache constructor does the latter). The order
                // matters: any invalidation messages received during
                // prefetch land on a cache whose dispatcher subscription
                // is already in place. See design doc §4 ("Lifecycle").
                if (preloaderCoordinator != null
                        && spec.preloader() != null
                        && spec.preloader().enabled()) {
                    preloaderCoordinator.register(name, near, spec.preloader());
                }
                yield near;
            }
        };
    }

    private CaffeineCache buildCaffeineCache(String name, CacheProperties.CacheSpec spec) {
        Caffeine<Object, Object> builder = Caffeine.newBuilder()
                .maximumSize(spec.maximumSize())
                .recordStats();
        // Four-way branch over (ttlJitterRatio, maxIdle):
        //   neither       → expireAfterWrite(ttl)               (pre-0.4.0 byte-identical)
        //   maxIdle only  → expireAfterWrite(ttl) + expireAfterAccess(maxIdle)
        //   jitter only   → expireAfter(JitteredExpiry)
        //   both          → expireAfter(JitteredMaxIdleExpiry) + removalListener
        //
        // Caffeine forbids combining expireAfter(Expiry) with either
        // expireAfterWrite(Duration) or expireAfterAccess(Duration), and
        // the order of method calls on the builder doesn't change that —
        // the choice has to be made up-front per spec.
        boolean jittering = spec.ttlJitterRatio() > 0.0;
        boolean maxIdling = spec.maxIdle() != null;
        if (jittering && maxIdling) {
            JitteredMaxIdleExpiry expiry = new JitteredMaxIdleExpiry(
                    spec.ttl().toNanos(),
                    spec.ttlJitterRatio(),
                    spec.maxIdle().toNanos());
            // Skip REPLACED: expireAfterUpdate has just written a fresh
            // deadline, and removing it here would race that write. Every
            // other cause (EXPIRED, EXPLICIT, SIZE, COLLECTED) leaves no
            // further write to the sidecar, so cleanup is unconditional.
            builder.expireAfter(expiry)
                    .removalListener((k, v, cause) -> {
                        if (k != null && cause != com.github.benmanes.caffeine.cache.RemovalCause.REPLACED) {
                            expiry.onRemoval(k);
                        }
                    });
        } else if (jittering) {
            builder.expireAfter(new JitteredExpiry(
                    spec.ttl().toNanos(), spec.ttlJitterRatio()));
        } else if (maxIdling) {
            builder.expireAfterWrite(spec.ttl())
                    .expireAfterAccess(spec.maxIdle());
        } else {
            builder.expireAfterWrite(spec.ttl());
        }
        com.github.benmanes.caffeine.cache.Cache<Object, Object> nativeCache = builder.build();
        return new CaffeineCache(name, nativeCache, true);
    }

    @Override
    public void destroy() {
        nearCaches.forEach(NearCache::shutdown);
        nearCaches.clear();
        distributedCaches.forEach(DistributedOnlyCache::shutdown);
        distributedCaches.clear();
    }
}
