package io.github.nwwarm.hybridcache.it;

import io.github.nwwarm.hybridcache.core.HybridCacheManager;
import io.github.nwwarm.hybridcache.metrics.HybridCacheHealthIndicator;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.cache.CacheManager;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Health-indicator behavior under healthy and degraded Redis. The indicator
 * is exercised through the bean Spring registers from {@code CacheConfig},
 * not constructed directly — that catches wiring regressions where the
 * {@code @ConditionalOnClass(HealthIndicator.class)} guard accidentally
 * suppresses the bean.
 *
 * <p>The OUT_OF_SERVICE test uses a private registry rather than the
 * autowired one so the global circuit-breaker state isn't disturbed —
 * other tests sharing this Spring context (notably
 * {@link EndToEndWiringIT}) would be brittle if we transitioned a shared
 * breaker through OPEN/CLOSED inside another test method.
 */
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class HealthIndicatorIT extends EndToEndTestBase {

    @Autowired
    private HybridCacheHealthIndicator healthIndicator;

    @Autowired
    private CacheManager cacheManager;

    @Autowired
    private RedissonClient autowiredRedisson;

    @Autowired
    private HybridCacheManager hybridCacheManager;

    @Test
    void healthIndicator_isUp_whenRedisReachable() {
        // Touch a cache so the cache details map has at least one entry, the
        // per-cache breaker is created, and the breaker registers at least
        // one buffered Redis call (the indicator's "Redis is reachable"
        // signal — see HybridCacheHealthIndicator's "Why no ping?" javadoc).
        productService.findById(1L);

        Health health = healthIndicator.health();
        // Surface the indicator's own details map on failure so a future
        // flake self-diagnoses. The indicator no longer issues a Redis
        // ping; the only DOWN path is "no cache activity AND Redisson
        // client shut down", which an autowired-context test cannot
        // legitimately hit.
        assertThat(health.getStatus())
                .as("health.getDetails()=%s", health.getDetails())
                .isEqualTo(Status.UP);
        assertThat(health.getDetails()).containsKeys("redisStatus", "caches");
        assertThat(health.getDetails().get("redisStatus"))
                .isEqualTo("reachable via active caches");

        @SuppressWarnings("unchecked")
        java.util.Map<String, Object> caches =
                (java.util.Map<String, Object>) health.getDetails().get("caches");
        assertThat(caches).containsKey("products");

        @SuppressWarnings("unchecked")
        java.util.Map<String, Object> products =
                (java.util.Map<String, Object>) caches.get("products");
        assertThat(products.get("breakerState")).isEqualTo("CLOSED");
    }

    @Test
    void healthIndicator_isOutOfService_whenBreakerOpen() {
        // Construct a private registry + indicator so we don't disturb the
        // shared registry used by NearCache instances in other tests sharing
        // this context. Pre-register a breaker that's open so the aggregate
        // status flips to OUT_OF_SERVICE.
        CircuitBreakerRegistry privateRegistry = CircuitBreakerRegistry.ofDefaults();
        CircuitBreaker privateBreaker =
                privateRegistry.circuitBreaker("cache:health-it-" + System.nanoTime());
        privateBreaker.transitionToOpenState();

        HybridCacheHealthIndicator scoped = new HybridCacheHealthIndicator(
                autowiredRedisson,
                privateRegistry,
                hybridCacheManager,
                Duration.ofMillis(500));

        Health health = scoped.health();
        assertThat(health.getStatus()).isEqualTo(Status.OUT_OF_SERVICE);
    }

    @Test
    void healthIndicator_includesPerCacheStats_underName() {
        // Populate the cache so size and hitRate are non-zero on read.
        productService.findById(7L);
        productService.findById(7L);
        productService.findById(7L);

        Health health = healthIndicator.health();

        @SuppressWarnings("unchecked")
        java.util.Map<String, Object> caches =
                (java.util.Map<String, Object>) health.getDetails().get("caches");
        assertThat(caches).containsKey("products");

        @SuppressWarnings("unchecked")
        java.util.Map<String, Object> products =
                (java.util.Map<String, Object>) caches.get("products");
        assertThat(products).containsKeys("size", "hitRate", "hitCount", "missCount");
    }
}
