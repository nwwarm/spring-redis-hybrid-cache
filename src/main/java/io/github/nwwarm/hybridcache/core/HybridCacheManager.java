package io.github.nwwarm.hybridcache.core;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
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
    private final ReconciliationCoordinator reconciliationCoordinator;
    private final RefreshExecutor refreshExecutor;

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
                meterRegistry, codecResolver, keyLogFormatter, null, null);
    }

    public HybridCacheManager(CacheProperties properties,
                              RedissonClient redisson,
                              CircuitBreakerRegistry circuitBreakerRegistry,
                              InvalidationDispatcher dispatcher,
                              MeterRegistry meterRegistry,
                              CodecResolver codecResolver,
                              KeyLogFormatter keyLogFormatter,
                              PreloaderCoordinator preloaderCoordinator) {
        this(properties, redisson, circuitBreakerRegistry, dispatcher,
                meterRegistry, codecResolver, keyLogFormatter,
                preloaderCoordinator, null);
    }

    public HybridCacheManager(CacheProperties properties,
                              RedissonClient redisson,
                              CircuitBreakerRegistry circuitBreakerRegistry,
                              InvalidationDispatcher dispatcher,
                              MeterRegistry meterRegistry,
                              CodecResolver codecResolver,
                              KeyLogFormatter keyLogFormatter,
                              PreloaderCoordinator preloaderCoordinator,
                              ReconciliationCoordinator reconciliationCoordinator) {
        this(properties, redisson, circuitBreakerRegistry, dispatcher,
                meterRegistry, codecResolver, keyLogFormatter,
                preloaderCoordinator, reconciliationCoordinator, null);
    }

    public HybridCacheManager(CacheProperties properties,
                              RedissonClient redisson,
                              CircuitBreakerRegistry circuitBreakerRegistry,
                              InvalidationDispatcher dispatcher,
                              MeterRegistry meterRegistry,
                              CodecResolver codecResolver,
                              KeyLogFormatter keyLogFormatter,
                              PreloaderCoordinator preloaderCoordinator,
                              ReconciliationCoordinator reconciliationCoordinator,
                              RefreshExecutor refreshExecutor) {
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
        this.reconciliationCoordinator = reconciliationCoordinator;
        this.refreshExecutor = refreshExecutor;
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
            case LOCAL_ONLY -> {
                // LOCAL_ONLY caches do not have a circuit breaker (no L2
                // to guard); pass null to the sidecar — refresh dispatch
                // proceeds without the breaker-open gate.
                SwrSidecar localSidecar = (spec.swr() != null)
                        ? new SwrSidecar(name,
                                spec.swr().freshFor().toNanos(),
                                refreshExecutor,
                                null,
                                meterRegistry)
                        : null;
                yield new LocalOnlyCache(
                        buildCaffeineCache(name, spec, localSidecar),
                        spec.maxConcurrentLoaders(), spec.loaderAcquireTimeout(),
                        meterRegistry, localSidecar);
            }
            case DISTRIBUTED_ONLY -> {
                CircuitBreaker breaker = breakerFactory.resolve(name, spec.circuitBreaker());
                DistributedOnlyCache distributed = new DistributedOnlyCache(
                        name, spec, bucketCodec, redisson, breaker, dispatcher, meterRegistry, keyLogFormatter);
                distributedCaches.add(distributed);
                if (reconciliationCoordinator != null
                        && spec.reconciliation() != null
                        && spec.reconciliation().enabled()) {
                    reconciliationCoordinator.register(distributed, spec.reconciliation());
                }
                yield distributed;
            }
            case NEAR_CACHE -> {
                CircuitBreaker breaker = breakerFactory.resolve(name, spec.circuitBreaker());
                // Construct SWR sidecar BEFORE the Caffeine cache so the
                // RemovalListener can call its onRemoval callback directly.
                // Validator already enforced spec.swr() preconditions; we
                // only need to translate freshFor to nanos here.
                SwrSidecar swrSidecar = (spec.swr() != null)
                        ? new SwrSidecar(name,
                                spec.swr().freshFor().toNanos(),
                                refreshExecutor,
                                breaker,
                                meterRegistry)
                        : null;
                NearCache near = new NearCache(
                        buildCaffeineCache(name, spec, swrSidecar),
                        spec, bucketCodec, redisson, breaker, dispatcher,
                        meterRegistry, keyLogFormatter, swrSidecar);
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
                // Reconciliation registration mirrors preloader registration
                // — runs after dispatcher.register so any in-flight messages
                // observed during the cycle land on the listener already.
                if (reconciliationCoordinator != null
                        && spec.reconciliation() != null
                        && spec.reconciliation().enabled()) {
                    reconciliationCoordinator.register(near, spec.reconciliation());
                }
                yield near;
            }
        };
    }

    private CaffeineCache buildCaffeineCache(String name, CacheProperties.CacheSpec spec) {
        return buildCaffeineCache(name, spec, null);
    }

    /**
     * Builds the underlying Caffeine cache. Branches on which of
     * (jitter, maxIdle, swr) are configured. SWR is layered as the
     * highest-precedence physical-eviction control: when the spec opts
     * in, {@code expireAfterWrite} is set to {@code swr.stale-for}
     * regardless of the jitter ratio, and the SWR fresh-until deadline
     * is exact (the integration test pins this — jitter must not skew
     * SWR deadlines).
     *
     * <p>The {@link com.github.benmanes.caffeine.cache.RemovalListener}
     * is composed: it fires both {@link JitteredMaxIdleExpiry#onRemoval}
     * (when present) and {@link SwrSidecar#onRemoval} (when present),
     * skipping {@code REPLACED} for both reasons (the sidecar write the
     * caller has just performed must not be cleaned up — guardrail
     * item 1 of the SWR section + the existing jittered-max-idle race).
     */
    private CaffeineCache buildCaffeineCache(String name,
                                             CacheProperties.CacheSpec spec,
                                             SwrSidecar swrSidecar) {
        Caffeine<Object, Object> builder = Caffeine.newBuilder()
                .maximumSize(spec.maximumSize())
                .recordStats();
        boolean jittering = spec.ttlJitterRatio() > 0.0;
        boolean maxIdling = spec.maxIdle() != null;
        boolean swrEnabled = swrSidecar != null;

        // SWR overrides the L1 expiry: stale-for is the physical eviction
        // boundary, regardless of jitter or max-idle. Jitter is
        // intentionally inert in SWR mode (the test pins this — fresh-for
        // and stale-for stay exact).
        JitteredMaxIdleExpiry jmiExpiry = null;
        if (swrEnabled) {
            builder.expireAfterWrite(spec.swr().staleFor());
        } else if (jittering && maxIdling) {
            jmiExpiry = new JitteredMaxIdleExpiry(
                    spec.ttl().toNanos(),
                    spec.ttlJitterRatio(),
                    spec.maxIdle().toNanos());
            builder.expireAfter(jmiExpiry);
        } else if (jittering) {
            builder.expireAfter(new JitteredExpiry(
                    spec.ttl().toNanos(), spec.ttlJitterRatio()));
        } else if (maxIdling) {
            builder.expireAfterWrite(spec.ttl())
                    .expireAfterAccess(spec.maxIdle());
        } else {
            builder.expireAfterWrite(spec.ttl());
        }

        // Composite removal listener: fires the jittered-max-idle cleanup
        // and the SWR sidecar cleanup, both skipping REPLACED. The two
        // are independent — REPLACED races a fresh deadline write in both
        // cases, every other cause is a strict eviction with no further
        // write to clean up.
        //
        // Caffeine dispatches removal notifications via its executor, which
        // defaults to ForkJoinPool.commonPool() (async). We force synchronous
        // dispatch with executor(Runnable::run) so SWR's reconciliation
        // contract — "caffeineCache.clear() drains the sidecar before it
        // returns" (DESIGN §2 SWR / Reconciliation interaction) — actually
        // holds. There is no AsyncLoadingCache in use, so synchronous
        // executor has no downside on the cache's hot path.
        if (jmiExpiry != null || swrEnabled) {
            final JitteredMaxIdleExpiry jmiFinal = jmiExpiry;
            builder.executor(Runnable::run)
                    .removalListener((k, v, cause) -> {
                        if (k == null || cause == RemovalCause.REPLACED) return;
                        if (jmiFinal != null) jmiFinal.onRemoval(k);
                        if (swrSidecar != null) swrSidecar.onRemoval(k.toString());
                    });
        }

        com.github.benmanes.caffeine.cache.Cache<Object, Object> nativeCache = builder.build();
        return new CaffeineCache(name, nativeCache, true);
    }

    @Override
    public void destroy() {
        if (reconciliationCoordinator != null) {
            nearCaches.forEach(c -> reconciliationCoordinator.deregister(c.getName()));
            distributedCaches.forEach(c -> reconciliationCoordinator.deregister(c.getName()));
        }
        nearCaches.forEach(NearCache::shutdown);
        nearCaches.clear();
        distributedCaches.forEach(DistributedOnlyCache::shutdown);
        distributedCaches.clear();
    }
}
