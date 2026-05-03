package io.github.nwwarm;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.redisson.client.RedisException;
import org.springframework.cache.caffeine.CaffeineCache;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Per-cache circuit-breaker isolation. Verifies the four acceptance criteria
 * for the registry refactor:
 *
 * <ol>
 *   <li>Two caches with different names hold distinct {@link CircuitBreaker}
 *       instances (identity comparison) — confirms the per-cache lookup
 *       happens at construction time and the cache stores its own breaker
 *       rather than a shared global.</li>
 *   <li>A flood of failing L2 ops on cache A trips A's breaker but leaves
 *       cache B's breaker {@code CLOSED} — the actual operational guarantee
 *       this refactor exists to deliver.</li>
 *   <li>Per-cache YAML override
 *       {@code cache.caches.foo.circuit-breaker.failure-rate-threshold=10}
 *       produces a breaker whose config reflects 10, while default-configured
 *       caches reflect the global default of 50.</li>
 *   <li>Pre-registering a breaker at startup and then constructing a cache
 *       under the same name yields the same breaker instance — paranoia
 *       check on the lookup-vs-creation contract.</li>
 * </ol>
 *
 * <p>All tests run without Redis. {@link RedissonClient} is mocked with deep
 * stubs so the cache's distributed-generation read and initial L2 calls
 * resolve without hitting a real broker; failure injection on
 * {@code getBucket(...).get()} drives the breaker through real L2 op
 * wrapping, not synthetic {@code breaker.onError(...)} calls.
 */
class CircuitBreakerPerCacheTest {

    private RedissonClient redisson;
    private InvalidationDispatcher dispatcher;

    @BeforeEach
    void setup() {
        redisson = mock(RedissonClient.class, Answers.RETURNS_DEEP_STUBS);
        // The dispatcher is package-private and used by NearCache. With deep
        // stubs, redisson.getTopic(...).addListener(...) returns 0 (int default)
        // — sufficient for these tests since we never publish.
        dispatcher = new InvalidationDispatcher(redisson, "test-node");
    }

    @Test
    void distinctCacheNames_haveDistinctBreakerInstances() {
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.ofDefaults();
        CircuitBreakerFactory factory = new CircuitBreakerFactory(registry);

        NearCache a = newNearCache("a", factory.resolve("a", null));
        NearCache b = newNearCache("b", factory.resolve("b", null));

        assertThat(a.getBreaker()).isNotSameAs(b.getBreaker());
        assertThat(a.getBreaker().getName()).isEqualTo("cache:a");
        assertThat(b.getBreaker().getName()).isEqualTo("cache:b");
    }

    @Test
    void failingL2OpsOnCacheA_doNotTripCacheBBreaker() {
        // Tight breaker window so a small number of failures is enough to
        // trip — keeps the test fast without sacrificing the realism of going
        // through readFromL2 / writeToL2.
        CacheProperties.CircuitBreaker tight = new CacheProperties.CircuitBreaker(
                5,        // slidingWindowSize
                5,        // minimumNumberOfCalls
                50.0f,    // failureRateThreshold
                null,
                null,
                Duration.ofSeconds(30),
                3);
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.ofDefaults();
        CircuitBreakerFactory factory = new CircuitBreakerFactory(registry);

        CircuitBreaker breakerA = factory.resolve("a", tight);
        CircuitBreaker breakerB = factory.resolve("b", tight);

        // Wire all bucket reads to throw RedisException so cache A's L2 reads
        // record failures against its breaker.
        @SuppressWarnings("unchecked")
        RBucket<Object> failingBucket = (RBucket<Object>) mock(RBucket.class);
        when(failingBucket.get()).thenThrow(new RedisException("simulated"));
        when(redisson.<Object>getBucket(anyString())).thenReturn(failingBucket);
        when(redisson.<Object>getBucket(anyString(), any())).thenReturn(failingBucket);

        NearCache a = newNearCache("a", breakerA);
        NearCache b = newNearCache("b", breakerB);

        // Drive cache A through enough failing reads to satisfy the
        // minimumNumberOfCalls + failure-rate threshold and open the breaker.
        for (int i = 0; i < 10; i++) {
            a.get("key-" + i);
        }

        assertThat(breakerA.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        // Cache B has done zero L2 ops; its window is empty, so the breaker
        // remains in its initial CLOSED state.
        assertThat(breakerB.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }

    @Test
    void perCacheOverride_appliesToSingleCacheOnly() {
        // Build CacheProperties as Spring would: caches.foo has an override of
        // failure-rate-threshold=10; bar inherits the default (50).
        CacheProperties.CircuitBreaker fooOverride = new CacheProperties.CircuitBreaker(
                null, null, 10.0f, null, null, null, null);
        CacheProperties.CacheSpec fooSpec = new CacheProperties.CacheSpec(
                CacheProperties.Tier.NEAR_CACHE,
                Duration.ofMinutes(10), 1000,
                Duration.ofSeconds(2), Duration.ofSeconds(10),
                CacheProperties.Codec.JSON,
                fooOverride);
        CacheProperties.CacheSpec barSpec = new CacheProperties.CacheSpec(
                CacheProperties.Tier.NEAR_CACHE,
                Duration.ofMinutes(10), 1000,
                Duration.ofSeconds(2), Duration.ofSeconds(10),
                CacheProperties.Codec.JSON,
                null);
        CacheProperties props = new CacheProperties(
                new CacheProperties.Server(
                        CacheProperties.Mode.SINGLE, "redis://localhost:6379",
                        null, null, null, null),
                Map.of("foo", fooSpec, "bar", barSpec),
                null,
                "test-node",
                java.util.List.of("io.github.nwwarm."),
                null,
                null,
                null,
                false, null);

        CircuitBreakerRegistry registry = new CacheConfig().redisCacheCircuitBreakerRegistry(props);

        CircuitBreaker fooBreaker = registry.find("cache:foo").orElseThrow();
        CircuitBreaker barBreaker = registry.find("cache:bar").orElseThrow();

        assertThat(fooBreaker.getCircuitBreakerConfig().getFailureRateThreshold())
                .isEqualTo(10.0f);
        assertThat(barBreaker.getCircuitBreakerConfig().getFailureRateThreshold())
                .isEqualTo(50.0f);
    }

    /**
     * The user-requested paranoia check on the lookup contract: a cache that
     * is pre-registered at startup AND resolved later at construction time
     * must hold the same breaker instance, not a fresh one. If
     * {@link CircuitBreakerRegistry#circuitBreaker(String,
     * io.github.resilience4j.circuitbreaker.CircuitBreakerConfig)} were not
     * idempotent, dashboards would show two breakers per cache and the
     * library would be silently wrong.
     */
    @Test
    void preRegisteredBreaker_isSameInstanceAsCacheBreaker() {
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.ofDefaults();
        CircuitBreakerFactory factory = new CircuitBreakerFactory(registry);

        CircuitBreaker preRegistered = factory.resolve("foo", null);
        NearCache cache = newNearCache("foo", factory.resolve("foo", null));

        assertThat(cache.getBreaker()).isSameAs(preRegistered);
    }

    private NearCache newNearCache(String name, CircuitBreaker breaker) {
        com.github.benmanes.caffeine.cache.Cache<Object, Object> nativeCache =
                Caffeine.newBuilder().maximumSize(100).recordStats().build();
        CaffeineCache springCache = new CaffeineCache(name, nativeCache, true);
        CacheProperties.CacheSpec spec = new CacheProperties.CacheSpec(
                CacheProperties.Tier.NEAR_CACHE,
                Duration.ofMinutes(10), 100,
                Duration.ofSeconds(2), Duration.ofSeconds(10),
                CacheProperties.Codec.JSON,
                null);
        return new NearCache(springCache, spec, null, redisson, breaker, dispatcher,
                new SimpleMeterRegistry(), new KeyLogFormatter(false, "test"));
    }
}
