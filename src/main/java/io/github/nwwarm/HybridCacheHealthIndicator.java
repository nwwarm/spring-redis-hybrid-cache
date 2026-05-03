package io.github.nwwarm;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.cache.Cache;

import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Spring Boot Actuator {@link HealthIndicator} for the hybrid cache.
 *
 * <p>Reports the operational state of the L2 path: per-cache circuit breaker
 * state (one entry per cache name), Redis reachability, and a per-cache
 * snapshot of L1 stats.
 *
 * <h2>Status mapping</h2>
 *
 * <p>Top-level status is the aggregate over every breaker registered for a
 * cache:
 * <ul>
 *   <li>Any breaker {@code OPEN} or {@code FORCED_OPEN} → {@link Status#OUT_OF_SERVICE}.</li>
 *   <li>Else any breaker {@code HALF_OPEN} → {@link Status#UNKNOWN}.</li>
 *   <li>Else all breakers {@code CLOSED}/{@code METRICS_ONLY}/{@code DISABLED}
 *       and Redis ping success → {@link Status#UP}.</li>
 *   <li>All breakers nominally CLOSED but Redis ping fails →
 *       {@link Status#DOWN}. Either Redis is briefly unavailable but no
 *       breaker has tripped yet, or the connection is too slow to complete
 *       inside the configured ping timeout.</li>
 * </ul>
 *
 * <p>Each cache also reports its own breaker state under
 * {@code details.caches.<name>.breakerState}, so dashboards can show which
 * caches have lost L2 access without having to inspect every breaker
 * separately.
 *
 * <p>The Redis ping is a single asynchronous {@code EXISTS} on a sentinel
 * key, bounded by the configured timeout (default 500ms). The timeout
 * matters: {@code /actuator/health} is hit by Kubernetes liveness probes,
 * load balancers, and dashboards — it must not block on a Redis incident
 * for tens of seconds. If the ping doesn't return inside the timeout, the
 * indicator reports the breaker state and {@code redisPingMs="timeout"}.
 */
public class HybridCacheHealthIndicator implements HealthIndicator {

    private static final Logger log = LoggerFactory.getLogger(HybridCacheHealthIndicator.class);
    private static final String PING_KEY = "hybrid-cache:health-ping";

    private final RedissonClient redisson;
    private final CircuitBreakerRegistry breakerRegistry;
    private final HybridCacheManager cacheManager;
    private final Duration pingTimeout;

    public HybridCacheHealthIndicator(RedissonClient redisson,
                                      CircuitBreakerRegistry breakerRegistry,
                                      HybridCacheManager cacheManager,
                                      Duration pingTimeout) {
        this.redisson = redisson;
        this.breakerRegistry = breakerRegistry;
        this.cacheManager = cacheManager;
        this.pingTimeout = pingTimeout;
    }

    @Override
    public Health health() {
        AggregateState aggregate = aggregateBreakerState();

        Health.Builder builder = switch (aggregate.worst()) {
            case OPEN, FORCED_OPEN -> Health.outOfService();
            case HALF_OPEN -> Health.unknown();
            case CLOSED, METRICS_ONLY, DISABLED -> Health.up();
        };

        PingResult ping = pingRedis();
        // If every breaker is nominally CLOSED but Redis is unreachable,
        // downgrade to DOWN — the application is one slow call away from
        // tripping and operators should know the L2 path is broken even if
        // no breaker has caught up yet.
        if (aggregate.allClosed() && !ping.reachable) {
            builder = Health.down();
        }

        builder.withDetail("redisPing", ping.label);
        if (ping.latencyMs >= 0) {
            builder.withDetail("redisPingMs", ping.latencyMs);
        }
        builder.withDetail("caches", buildCacheDetails());
        return builder.build();
    }

    /**
     * Worst breaker state across every cache, plus whether every breaker is in
     * a nominally-closed state. Defaults to CLOSED + allClosed=true when no
     * breakers have been registered yet (e.g., fresh context, no cache has
     * been touched).
     */
    private record AggregateState(CircuitBreaker.State worst, boolean allClosed) {}

    private AggregateState aggregateBreakerState() {
        CircuitBreaker.State worst = CircuitBreaker.State.CLOSED;
        boolean allClosed = true;
        for (CircuitBreaker breaker : breakerRegistry.getAllCircuitBreakers()) {
            CircuitBreaker.State state = breaker.getState();
            if (!isNominallyClosed(state)) allClosed = false;
            worst = worse(worst, state);
        }
        return new AggregateState(worst, allClosed);
    }

    private static boolean isNominallyClosed(CircuitBreaker.State state) {
        return state == CircuitBreaker.State.CLOSED
                || state == CircuitBreaker.State.METRICS_ONLY
                || state == CircuitBreaker.State.DISABLED;
    }

    /** Severity ordering: OPEN/FORCED_OPEN > HALF_OPEN > CLOSED/METRICS_ONLY/DISABLED. */
    private static CircuitBreaker.State worse(CircuitBreaker.State a, CircuitBreaker.State b) {
        return rank(b) > rank(a) ? b : a;
    }

    private static int rank(CircuitBreaker.State state) {
        return switch (state) {
            case OPEN, FORCED_OPEN -> 2;
            case HALF_OPEN -> 1;
            case CLOSED, METRICS_ONLY, DISABLED -> 0;
        };
    }

    private record PingResult(boolean reachable, long latencyMs, String label) {}

    private PingResult pingRedis() {
        long startNanos = System.nanoTime();
        try {
            redisson.getBucket(PING_KEY).isExistsAsync()
                    .toCompletableFuture()
                    .get(pingTimeout.toMillis(), TimeUnit.MILLISECONDS);
            long ms = (System.nanoTime() - startNanos) / 1_000_000;
            return new PingResult(true, ms, "ok");
        } catch (TimeoutException e) {
            return new PingResult(false, -1, "timeout after " + pingTimeout.toMillis() + "ms");
        } catch (Exception e) {
            log.debug("Redis ping failed in health indicator", e);
            String msg = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
            return new PingResult(false, -1, "unreachable: " + msg);
        }
    }

    private Map<String, Object> buildCacheDetails() {
        Map<String, Object> out = new LinkedHashMap<>();
        Collection<String> names = cacheManager.getCacheNames();
        for (String name : names) {
            Cache cache = cacheManager.getCache(name);
            if (cache == null) continue;
            Map<String, Object> entry = new LinkedHashMap<>();
            Object native_ = cache.getNativeCache();
            if (native_ instanceof com.github.benmanes.caffeine.cache.Cache<?, ?> caffeine) {
                CacheStats stats = caffeine.stats();
                entry.put("size", caffeine.estimatedSize());
                entry.put("hitRate", stats.hitRate());
                entry.put("missCount", stats.missCount());
                entry.put("hitCount", stats.hitCount());
            } else {
                // DistributedOnlyCache has no L1 to report on.
                entry.put("type", cache.getClass().getSimpleName());
            }
            // Per-cache breaker state. Look up by the same name the cache
            // registers under at construction. A LocalOnlyCache has no
            // breaker, so we omit those fields rather than report a bogus
            // CLOSED.
            breakerRegistry.find(CircuitBreakerFactory.breakerName(name)).ifPresent(breaker -> {
                CircuitBreaker.Metrics metrics = breaker.getMetrics();
                entry.put("breakerState", breaker.getState().name());
                entry.put("breakerFailureRate", metrics.getFailureRate());
                entry.put("breakerSlowCallRate", metrics.getSlowCallRate());
            });
            out.put(name, entry);
        }
        return out;
    }
}
