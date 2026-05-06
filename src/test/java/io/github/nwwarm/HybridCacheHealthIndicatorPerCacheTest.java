package io.github.nwwarm;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.redisson.api.RedissonClient;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.cache.caffeine.CaffeineCache;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * The health indicator must report breaker state per cache, not a single
 * aggregate detail. This is the dashboards-care-which-cache-is-broken
 * guarantee that lets operators see "auth-tokens has lost L2" without
 * inspecting every breaker by hand.
 */
class HybridCacheHealthIndicatorPerCacheTest {

    @Test
    void details_reportBreakerStatePerCache() {
        RedissonClient redisson = mock(RedissonClient.class, Answers.RETURNS_DEEP_STUBS);
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.ofDefaults();
        CircuitBreakerFactory factory = new CircuitBreakerFactory(registry);

        // Two caches with their own breakers.
        InvalidationDispatcher dispatcher = new InvalidationDispatcher(redisson, "test-node", new SimpleMeterRegistry());
        NearCache aCache = newNearCache("a", redisson, factory.resolve("a", null), dispatcher);
        NearCache bCache = newNearCache("b", redisson, factory.resolve("b", null), dispatcher);

        // Trip cache A's breaker without touching B's.
        aCache.getBreaker().transitionToOpenState();

        // CacheManager-shaped stub that returns the two caches by name. Pure
        // anonymous subclass to avoid pulling in Spring's full AbstractCacheManager
        // wiring just to expose a getCacheNames() and getCache() pair.
        HybridCacheManager cacheManager = new HybridCacheManager(
                new CacheProperties(
                        new CacheProperties.Server(
                                CacheProperties.Mode.SINGLE, "redis://localhost:6379",
                                null, null, null, null),
                        Map.of(),
                        null, "test-node",
                        java.util.List.of("io.github.nwwarm."),
                        null, null, null, false, null, null, null, null),
                redisson,
                registry,
                dispatcher,
                new SimpleMeterRegistry(),
                new CodecResolver(null),
                new KeyLogFormatter(false, "test")) {
            @Override
            public java.util.Collection<String> getCacheNames() {
                return java.util.List.of("a", "b");
            }

            @Override
            public org.springframework.cache.Cache getCache(String name) {
                return switch (name) {
                    case "a" -> aCache;
                    case "b" -> bCache;
                    default -> null;
                };
            }
        };

        HybridCacheHealthIndicator indicator = new HybridCacheHealthIndicator(
                redisson, registry, cacheManager, Duration.ofMillis(50));
        Health health = indicator.health();

        // Aggregate status reflects the worst breaker — A is OPEN.
        assertThat(health.getStatus()).isEqualTo(Status.OUT_OF_SERVICE);

        @SuppressWarnings("unchecked")
        Map<String, Object> caches = (Map<String, Object>) health.getDetails().get("caches");
        @SuppressWarnings("unchecked")
        Map<String, Object> aDetails = (Map<String, Object>) caches.get("a");
        @SuppressWarnings("unchecked")
        Map<String, Object> bDetails = (Map<String, Object>) caches.get("b");

        assertThat(aDetails.get("breakerState")).isEqualTo("OPEN");
        assertThat(bDetails.get("breakerState")).isEqualTo("CLOSED");
    }

    private NearCache newNearCache(String name, RedissonClient redisson,
                                   CircuitBreaker breaker,
                                   InvalidationDispatcher dispatcher) {
        com.github.benmanes.caffeine.cache.Cache<Object, Object> nativeCache =
                Caffeine.newBuilder().maximumSize(100).recordStats().build();
        CaffeineCache springCache = new CaffeineCache(name, nativeCache, true);
        CacheProperties.CacheSpec spec = new CacheProperties.CacheSpec(
                CacheProperties.Tier.NEAR_CACHE,
                Duration.ofMinutes(10), 100,
                Duration.ofSeconds(2), Duration.ofSeconds(10),
                CacheProperties.Codec.JSON,
                null, null, null, 0.0, null, null);
        return new NearCache(springCache, spec, null, redisson, breaker, dispatcher,
                new SimpleMeterRegistry(), new KeyLogFormatter(false, "test"));
    }
}
