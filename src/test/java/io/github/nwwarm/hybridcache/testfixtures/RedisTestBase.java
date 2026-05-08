package io.github.nwwarm.hybridcache.testfixtures;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.nwwarm.hybridcache.config.CacheProperties;
import io.github.nwwarm.hybridcache.core.DistributedOnlyCache;
import io.github.nwwarm.hybridcache.core.KeyLogFormatter;
import io.github.nwwarm.hybridcache.core.LocalOnlyCache;
import io.github.nwwarm.hybridcache.core.NearCache;
import io.github.nwwarm.hybridcache.invalidation.InvalidationDispatcher;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.client.RedisException;
import org.redisson.client.codec.Codec;
import org.redisson.codec.JsonJacksonCodec;
import org.redisson.codec.Kryo5Codec;
import org.redisson.config.Config;
import org.springframework.cache.caffeine.CaffeineCache;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.net.ConnectException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * Shared Testcontainers Redis fixture for integration tests.
 *
 * <p>One Redis container per JVM (started by JUnit's @Testcontainers extension);
 * each test gets fresh RedissonClient/CircuitBreaker/NearCache instances so the
 * global generation counters and circuit-breaker state don't bleed between tests.
 */
@Testcontainers
public abstract class RedisTestBase {

    @Container
    protected static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    // ---------- Redisson clients ----------

    protected RedissonClient newRedisson() {
        // Generous timeouts for the happy-path tests.
        return newRedisson(Duration.ofSeconds(3), Duration.ofSeconds(10), 3);
    }

    /**
     * Fail-fast client for the graceful-degradation test. Short timeouts let a
     * paused container surface failures within hundreds of milliseconds rather
     * than tens of seconds.
     */
    protected RedissonClient newFailFastRedisson() {
        return newRedisson(Duration.ofMillis(500), Duration.ofMillis(500), 1);
    }

    protected RedissonClient newRedisson(Duration connectTimeout,
                                         Duration readTimeout,
                                         int retryAttempts) {
        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule());
        Config cfg = new Config();
        cfg.setCodec(new JsonJacksonCodec(mapper));
        cfg.useSingleServer()
                .setAddress("redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379))
                .setConnectTimeout((int) connectTimeout.toMillis())
                .setTimeout((int) readTimeout.toMillis())
                .setRetryAttempts(retryAttempts)
                .setRetryInterval(100);
        return Redisson.create(cfg);
    }

    // ---------- Circuit breakers ----------

    /** Permissive breaker — won't trip during normal happy-path tests. */
    protected CircuitBreaker defaultBreaker() {
        return CircuitBreakerRegistry.ofDefaults().circuitBreaker("test-" + System.nanoTime());
    }

    /** Trips fast under load and recovers quickly — used by the degradation test. */
    protected CircuitBreaker fastFailBreaker() {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(5)
                .minimumNumberOfCalls(3)
                .failureRateThreshold(50.0f)
                .slowCallDurationThreshold(Duration.ofMillis(800))
                .slowCallRateThreshold(80.0f)
                .waitDurationInOpenState(Duration.ofSeconds(2))
                .permittedNumberOfCallsInHalfOpenState(2)
                .recordExceptions(
                        RedisException.class,
                        TimeoutException.class,
                        ConnectException.class,
                        org.redisson.client.RedisTimeoutException.class,
                        org.redisson.client.RedisConnectionException.class)
                .build();
        return CircuitBreakerRegistry.of(config).circuitBreaker("test-fast-" + System.nanoTime());
    }

    // ---------- Cache builders ----------

    protected NearCache newNearCache(String name,
                                     RedissonClient redisson,
                                     CircuitBreaker breaker,
                                     String nodeId) {
        return newNearCache(name, redisson, breaker, nodeId,
                CacheProperties.Codec.JSON, new SimpleMeterRegistry());
    }

    protected NearCache newNearCache(String name,
                                     RedissonClient redisson,
                                     CircuitBreaker breaker,
                                     String nodeId,
                                     CacheProperties.Codec codec,
                                     MeterRegistry meterRegistry) {
        return newNearCache(name, redisson, breaker, nodeId, codec, meterRegistry,
                Duration.ofMinutes(10), Duration.ofSeconds(2), Duration.ofSeconds(10));
    }

    protected NearCache newNearCache(String name,
                                     RedissonClient redisson,
                                     CircuitBreaker breaker,
                                     String nodeId,
                                     CacheProperties.Codec codec,
                                     MeterRegistry meterRegistry,
                                     Duration ttl,
                                     Duration lockWait,
                                     Duration lockLease) {
        return newNearCache(name, redisson, breaker, nodeId, codec, meterRegistry,
                ttl, lockWait, lockLease, null);
    }

    protected NearCache newNearCache(String name,
                                     RedissonClient redisson,
                                     CircuitBreaker breaker,
                                     String nodeId,
                                     CacheProperties.Codec codec,
                                     MeterRegistry meterRegistry,
                                     Duration ttl,
                                     Duration lockWait,
                                     Duration lockLease,
                                     CacheProperties.Reconciliation reconciliation) {
        CacheProperties.CacheSpec spec = new CacheProperties.CacheSpec(
                CacheProperties.Tier.NEAR_CACHE,
                ttl, 10_000, lockWait, lockLease, codec, null, null, null, 0.0, null, null,
                reconciliation, null, null);

        com.github.benmanes.caffeine.cache.Cache<Object, Object> caffeineNative = Caffeine.newBuilder()
                .expireAfterWrite(ttl)
                .maximumSize(spec.maximumSize())
                .recordStats()
                .build();
        CaffeineCache springCache = new CaffeineCache(name, caffeineNative, true);
        InvalidationDispatcher dispatcher = new InvalidationDispatcher(redisson, nodeId, meterRegistry);
        return new NearCache(springCache, spec, resolveTestCodec(codec), redisson,
                breaker, dispatcher, meterRegistry, new KeyLogFormatter(false, "test"));
    }

    /**
     * Same shape as the simpler {@link #newNearCache(String, RedissonClient, CircuitBreaker, String)}
     * helper, but with reconciliation pre-enabled at the supplied
     * interval and miss-tolerance. Used by the reconciliation IT suite.
     */
    protected NearCache newReconcilingNearCache(String name,
                                                RedissonClient redisson,
                                                CircuitBreaker breaker,
                                                String nodeId,
                                                MeterRegistry meterRegistry,
                                                Duration interval,
                                                int missTolerance) {
        return newNearCache(name, redisson, breaker, nodeId,
                CacheProperties.Codec.JSON, meterRegistry,
                Duration.ofMinutes(10),       // ttl — long, so TTL doesn't mask reconciliation effects
                Duration.ofSeconds(2),         // lockWait
                Duration.ofSeconds(10),        // lockLease
                new CacheProperties.Reconciliation(true, interval, missTolerance));
    }

    /**
     * SWR-enabled near cache for integration tests. Builds the Caffeine
     * cache with {@code expireAfterWrite(staleFor)} (no jitter), wires the
     * {@code RemovalListener} to the sidecar (skipping {@code REPLACED}
     * per SWR guardrail item 1), and returns a {@link NearCache} with the
     * sidecar passed in.
     *
     * <p>Mirrors {@code HybridCacheManager.buildCaffeineCache} — the test
     * fixture must not drift from the production wiring, otherwise the SWR
     * IT suite would test a parallel implementation.
     */
    protected NearCache newSwrNearCache(String name,
                                        RedissonClient redisson,
                                        CircuitBreaker breaker,
                                        String nodeId,
                                        MeterRegistry meterRegistry,
                                        io.github.nwwarm.hybridcache.core.RefreshExecutor refreshExecutor,
                                        Duration ttl,
                                        Duration freshFor,
                                        Duration staleFor,
                                        CacheProperties.Reconciliation reconciliation) {
        CacheProperties.CacheSpec spec = new CacheProperties.CacheSpec(
                CacheProperties.Tier.NEAR_CACHE,
                ttl, 10_000,
                Duration.ofSeconds(2), Duration.ofSeconds(10),
                CacheProperties.Codec.JSON, null, null, null, 0.0, null, null,
                reconciliation,
                new CacheProperties.Swr(freshFor, staleFor),
                null);

        io.github.nwwarm.hybridcache.core.SwrSidecar sidecar =
                new io.github.nwwarm.hybridcache.core.SwrSidecar(name,
                        freshFor.toNanos(), refreshExecutor, breaker, meterRegistry);

        com.github.benmanes.caffeine.cache.Cache<Object, Object> caffeineNative = Caffeine.newBuilder()
                .expireAfterWrite(staleFor)
                .maximumSize(spec.maximumSize())
                .recordStats()
                // Synchronous removal — mirrors HybridCacheManager so the
                // SWR sidecar drains before clear()/evict() returns. Without
                // this, Caffeine fires removalListener via its async
                // executor and tests would race the cleanup.
                .executor(Runnable::run)
                .removalListener((k, v, cause) -> {
                    if (k != null
                            && cause != com.github.benmanes.caffeine.cache.RemovalCause.REPLACED) {
                        sidecar.onRemoval(k.toString());
                    }
                })
                .build();
        CaffeineCache springCache = new CaffeineCache(name, caffeineNative, true);
        InvalidationDispatcher dispatcher = new InvalidationDispatcher(redisson, nodeId, meterRegistry);
        return new NearCache(springCache, spec, resolveTestCodec(spec.codec()), redisson,
                breaker, dispatcher, meterRegistry,
                new KeyLogFormatter(false, "test"), sidecar);
    }

    /**
     * SWR + RA-enabled near cache for integration tests. Builds the same
     * wiring as {@link #newSwrNearCache(String, RedissonClient, CircuitBreaker, String, MeterRegistry, io.github.nwwarm.hybridcache.core.RefreshExecutor, Duration, Duration, Duration, CacheProperties.Reconciliation)}
     * plus a {@link io.github.nwwarm.hybridcache.core.RefreshAheadCoordinator}
     * and a {@link io.github.nwwarm.hybridcache.core.LoaderRuntimeEwma}
     * shared between the SWR refresh path and RA. Mirrors
     * {@code HybridCacheManager} so the RA IT suite isn't testing a parallel
     * implementation.
     *
     * @param beta XFetch β; β > 0 (validator E39 enforces this in production).
     */
    protected NearCache newRaNearCache(String name,
                                        RedissonClient redisson,
                                        CircuitBreaker breaker,
                                        String nodeId,
                                        MeterRegistry meterRegistry,
                                        io.github.nwwarm.hybridcache.core.RefreshExecutor refreshExecutor,
                                        Duration ttl,
                                        Duration freshFor,
                                        Duration staleFor,
                                        double beta) {
        return newRaNearCache(name, redisson, breaker, nodeId, meterRegistry,
                refreshExecutor, ttl, freshFor, staleFor, beta, null);
    }

    /**
     * Same as the simpler RA fixture but with a caller-supplied
     * {@link java.util.random.RandomGenerator} so tests can fix the XFetch
     * sample seed for deterministic predicate firing.
     */
    protected NearCache newRaNearCache(String name,
                                        RedissonClient redisson,
                                        CircuitBreaker breaker,
                                        String nodeId,
                                        MeterRegistry meterRegistry,
                                        io.github.nwwarm.hybridcache.core.RefreshExecutor refreshExecutor,
                                        Duration ttl,
                                        Duration freshFor,
                                        Duration staleFor,
                                        double beta,
                                        java.util.random.RandomGenerator rng) {
        CacheProperties.CacheSpec spec = new CacheProperties.CacheSpec(
                CacheProperties.Tier.NEAR_CACHE,
                ttl, 10_000,
                Duration.ofSeconds(2), Duration.ofSeconds(10),
                CacheProperties.Codec.JSON, null, null, null, 0.0, null, null,
                null,
                new CacheProperties.Swr(freshFor, staleFor),
                new CacheProperties.RefreshAhead(true, beta));

        io.github.nwwarm.hybridcache.core.SwrSidecar sidecar =
                new io.github.nwwarm.hybridcache.core.SwrSidecar(name,
                        freshFor.toNanos(), refreshExecutor, breaker, meterRegistry);
        io.github.nwwarm.hybridcache.core.LoaderRuntimeEwma ewma =
                new io.github.nwwarm.hybridcache.core.LoaderRuntimeEwma();
        io.github.nwwarm.hybridcache.core.RefreshAheadCoordinator ra =
                new io.github.nwwarm.hybridcache.core.RefreshAheadCoordinator(name,
                        freshFor.toNanos(), beta, sidecar, refreshExecutor,
                        breaker, ewma, meterRegistry, rng);

        com.github.benmanes.caffeine.cache.Cache<Object, Object> caffeineNative = Caffeine.newBuilder()
                .expireAfterWrite(staleFor)
                .maximumSize(spec.maximumSize())
                .recordStats()
                .executor(Runnable::run)
                .removalListener((k, v, cause) -> {
                    if (k != null
                            && cause != com.github.benmanes.caffeine.cache.RemovalCause.REPLACED) {
                        sidecar.onRemoval(k.toString());
                    }
                })
                .build();
        CaffeineCache springCache = new CaffeineCache(name, caffeineNative, true);
        InvalidationDispatcher dispatcher = new InvalidationDispatcher(redisson, nodeId, meterRegistry);
        return new NearCache(springCache, spec, resolveTestCodec(spec.codec()), redisson,
                breaker, dispatcher, meterRegistry,
                new KeyLogFormatter(false, "test"), sidecar, ra, ewma);
    }

    protected DistributedOnlyCache newDistributedCache(String name,
                                                       RedissonClient redisson,
                                                       CircuitBreaker breaker) {
        return newDistributedCache(name, redisson, breaker, "test-node-" + System.nanoTime());
    }

    protected DistributedOnlyCache newDistributedCache(String name,
                                                       RedissonClient redisson,
                                                       CircuitBreaker breaker,
                                                       String nodeId) {
        CacheProperties.CacheSpec spec = new CacheProperties.CacheSpec(
                CacheProperties.Tier.DISTRIBUTED_ONLY,
                Duration.ofMinutes(10), 10_000,
                Duration.ofSeconds(2), Duration.ofSeconds(10),
                CacheProperties.Codec.JSON, null, null, null, 0.0, null, null, null, null, null);
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        InvalidationDispatcher dispatcher = new InvalidationDispatcher(redisson, nodeId, meterRegistry);
        return new DistributedOnlyCache(name, spec, resolveTestCodec(spec.codec()),
                redisson, breaker, dispatcher, meterRegistry,
                new KeyLogFormatter(false, "test"));
    }

    /**
     * Resolves the per-cache codec for direct test construction. Tests that
     * use {@link CacheProperties.Codec#KRYO} get a default {@link Kryo5Codec}
     * with no registration — the production-side registration validator
     * lives in {@code CacheConfig}, which these tests bypass on purpose.
     */
    protected Codec resolveTestCodec(CacheProperties.Codec choice) {
        return switch (choice) {
            case JSON -> null;
            case KRYO -> new Kryo5Codec();
        };
    }

    protected LocalOnlyCache newLocalCache(String name) {
        com.github.benmanes.caffeine.cache.Cache<Object, Object> caffeineNative = Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofMinutes(10))
                .maximumSize(10_000)
                .recordStats()
                .build();
        CaffeineCache springCache = new CaffeineCache(name, caffeineNative, true);
        return new LocalOnlyCache(springCache, new SimpleMeterRegistry());
    }

    /** Direct access to the underlying Caffeine cache for L1-only assertions. */
    @SuppressWarnings("unchecked")
    protected static com.github.benmanes.caffeine.cache.Cache<Object, Object> nativeOf(NearCache cache) {
        return (com.github.benmanes.caffeine.cache.Cache<Object, Object>) cache.getNativeCache();
    }
}
