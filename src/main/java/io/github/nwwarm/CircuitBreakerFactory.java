package io.github.nwwarm;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

import java.time.Duration;

/**
 * Resolves the per-cache {@link CircuitBreaker} from the shared
 * {@link CircuitBreakerRegistry}, applying any per-cache overlay declared in
 * {@code cache.caches.<name>.circuit-breaker.*} on top of the registry's
 * default configuration.
 *
 * <p>The breaker name is {@code "cache:" + cacheName} so per-cache breakers
 * sit under a stable, predictable namespace in {@code TaggedCircuitBreakerMetrics}.
 *
 * <p>Resilience4j's {@link CircuitBreakerRegistry#circuitBreaker(String,
 * CircuitBreakerConfig)} is idempotent on repeated calls with the same name —
 * the supplied config is only used at creation time. Caches that are
 * pre-registered at startup and resolved later via {@link HybridCacheManager}
 * therefore receive the same instance.
 */
final class CircuitBreakerFactory {

    private final CircuitBreakerRegistry registry;

    CircuitBreakerFactory(CircuitBreakerRegistry registry) {
        this.registry = registry;
    }

    static String breakerName(String cacheName) {
        return "cache:" + cacheName;
    }

    /**
     * Returns the breaker for {@code cacheName}, registering it with the
     * overlaid config the first time it is requested.
     */
    CircuitBreaker resolve(String cacheName, CacheProperties.CircuitBreaker overlay) {
        CircuitBreakerConfig config = buildConfig(overlay);
        return registry.circuitBreaker(breakerName(cacheName), config);
    }

    /**
     * Builds the overlaid {@link CircuitBreakerConfig} without registering it.
     * Used at startup to validate that per-cache overrides parse cleanly
     * (Resilience4j's builder throws on out-of-range values).
     */
    CircuitBreakerConfig buildConfig(CacheProperties.CircuitBreaker overlay) {
        CircuitBreakerConfig defaults = registry.getDefaultConfig();
        CircuitBreakerConfig.Builder builder = CircuitBreakerConfig.from(defaults);
        if (overlay == null) return builder.build();

        Integer slidingWindowSize = overlay.slidingWindowSize();
        if (slidingWindowSize != null) builder.slidingWindowSize(slidingWindowSize);

        Integer minCalls = overlay.minimumNumberOfCalls();
        if (minCalls != null) builder.minimumNumberOfCalls(minCalls);

        Float failureRate = overlay.failureRateThreshold();
        if (failureRate != null) builder.failureRateThreshold(failureRate);

        Duration slowCallDuration = overlay.slowCallDurationThreshold();
        if (slowCallDuration != null) builder.slowCallDurationThreshold(slowCallDuration);

        Float slowCallRate = overlay.slowCallRateThreshold();
        if (slowCallRate != null) builder.slowCallRateThreshold(slowCallRate);

        Duration waitInOpen = overlay.waitDurationInOpenState();
        if (waitInOpen != null) builder.waitDurationInOpenState(waitInOpen);

        Integer halfOpenPermitted = overlay.permittedNumberOfCallsInHalfOpenState();
        if (halfOpenPermitted != null) builder.permittedNumberOfCallsInHalfOpenState(halfOpenPermitted);

        return builder.build();
    }
}
