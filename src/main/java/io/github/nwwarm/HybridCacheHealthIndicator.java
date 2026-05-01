package io.github.nwwarm;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
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
 * <p>Reports the operational state of the L2 path: circuit breaker, Redis
 * reachability, and a per-cache snapshot of L1 stats.
 *
 * <h2>Status mapping</h2>
 * <ul>
 *   <li>Breaker {@code CLOSED} + Redis ping success → {@link Status#UP}.</li>
 *   <li>Breaker {@code OPEN} or {@code FORCED_OPEN} → {@link Status#OUT_OF_SERVICE}.
 *       Reads still return cached values from L1; writes are suppressed.</li>
 *   <li>Breaker {@code HALF_OPEN} → {@link Status#UNKNOWN}. Trial calls in
 *       progress; the next few results determine whether the breaker
 *       closes or reopens.</li>
 *   <li>Breaker {@code CLOSED} but Redis ping fails → {@link Status#DOWN}.
 *       Either Redis is briefly unavailable but the breaker hasn't tripped
 *       yet, or the connection is too slow to complete inside the
 *       configured ping timeout.</li>
 * </ul>
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
    private final CircuitBreaker breaker;
    private final HybridCacheManager cacheManager;
    private final Duration pingTimeout;

    public HybridCacheHealthIndicator(RedissonClient redisson,
                                      CircuitBreaker breaker,
                                      HybridCacheManager cacheManager,
                                      Duration pingTimeout) {
        this.redisson = redisson;
        this.breaker = breaker;
        this.cacheManager = cacheManager;
        this.pingTimeout = pingTimeout;
    }

    @Override
    public Health health() {
        CircuitBreaker.State state = breaker.getState();
        CircuitBreaker.Metrics metrics = breaker.getMetrics();

        Health.Builder builder = switch (state) {
            case OPEN, FORCED_OPEN -> Health.outOfService();
            case HALF_OPEN -> Health.unknown();
            case CLOSED, METRICS_ONLY, DISABLED -> Health.up();
        };

        builder.withDetail("breakerState", state.name())
               .withDetail("breakerFailureRate", metrics.getFailureRate())
               .withDetail("breakerSlowCallRate", metrics.getSlowCallRate());

        PingResult ping = pingRedis();
        builder.withDetail("redisPing", ping.label);
        if (ping.latencyMs >= 0) {
            builder.withDetail("redisPingMs", ping.latencyMs);
        }
        // If the breaker reports closed but Redis is unreachable, downgrade
        // to DOWN — the application is one slow call away from tripping and
        // operators should know the L2 path is broken even if the breaker
        // hasn't caught up yet.
        if (state == CircuitBreaker.State.CLOSED && !ping.reachable) {
            builder = Health.down()
                    .withDetail("breakerState", state.name())
                    .withDetail("breakerFailureRate", metrics.getFailureRate())
                    .withDetail("breakerSlowCallRate", metrics.getSlowCallRate())
                    .withDetail("redisPing", ping.label);
            if (ping.latencyMs >= 0) {
                builder.withDetail("redisPingMs", ping.latencyMs);
            }
        }

        builder.withDetail("caches", buildCacheDetails());
        return builder.build();
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
            out.put(name, entry);
        }
        return out;
    }
}
