package io.github.nwwarm.hybridcache.metrics;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.github.nwwarm.hybridcache.core.CircuitBreakerFactory;
import io.github.nwwarm.hybridcache.core.HybridCacheManager;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.redisson.api.RedissonClient;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.cache.Cache;

import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Spring Boot Actuator {@link HealthIndicator} for the hybrid cache.
 *
 * <p>Reports the operational state of the L2 path: per-cache circuit breaker
 * state (one entry per cache name) and a per-cache snapshot of L1 stats.
 * Redis reachability is <em>derived</em> from those signals rather than
 * probed by a separate command. See "Why no ping?" below.
 *
 * <h2>Status mapping</h2>
 *
 * <p>Top-level status is the aggregate over every breaker registered for a
 * cache:
 * <ul>
 *   <li>Any breaker {@code OPEN} or {@code FORCED_OPEN} → {@link Status#OUT_OF_SERVICE}.</li>
 *   <li>Else any breaker {@code HALF_OPEN} → {@link Status#UNKNOWN}.</li>
 *   <li>Else all breakers {@code CLOSED}/{@code METRICS_ONLY}/{@code DISABLED}
 *       and either at least one cache has executed Redis calls
 *       successfully or the {@link RedissonClient} is alive →
 *       {@link Status#UP}.</li>
 *   <li>All breakers nominally CLOSED, no cache has touched Redis yet,
 *       and the Redisson client is shut down or shutting down →
 *       {@link Status#DOWN}.</li>
 * </ul>
 *
 * <p>Each cache also reports its own breaker state under
 * {@code details.caches.<name>.breakerState}, so dashboards can show which
 * caches have lost L2 access without having to inspect every breaker
 * separately.
 *
 * <h2>Why no ping?</h2>
 *
 * <p>The 0.2.0 implementation issued a separate {@code EXISTS} on a
 * sentinel key with a configurable timeout. Three problems with that:
 * <ul>
 *   <li>Redundant. The cache hot path is reaching Redis on every miss
 *       and every write; if it works, Redis works. A second probe code
 *       path can fail in ways the production path does not (different
 *       connection pool semantics, different command, different
 *       event-loop scheduling) and tells operators nothing the
 *       production path is not already telling them.</li>
 *   <li>Pollutes the breaker. {@code /actuator/health} is hit on every
 *       liveness probe interval (Kubernetes, dashboards, load
 *       balancers); each probe ran an extra Redis command through the
 *       same connection pool, and a slow probe registered as a slow
 *       call against the per-cache breaker, inflating the very
 *       {@code breakerSlowCallRate} the indicator was reporting.</li>
 *   <li>Flaky. Event-loop contention from co-located tests or a busy
 *       JVM could push the EXISTS over the configured timeout while
 *       Redis itself was responding in well under a millisecond,
 *       producing {@code DOWN} in CI for a system that was actually
 *       healthy.</li>
 * </ul>
 *
 * <p>Mature health indicators follow the same pattern. Spring Boot's
 * {@code DataSourceHealthIndicator} consults the connection pool
 * rather than running a separate validation query when the pool's
 * own bookkeeping is authoritative.
 */
public class HybridCacheHealthIndicator implements HealthIndicator {

    private final RedissonClient redisson;
    private final CircuitBreakerRegistry breakerRegistry;
    private final HybridCacheManager cacheManager;

    /**
     * Constructor preserving the 0.2.0 signature for backward compatibility
     * with applications and tests that supplied a ping timeout. The timeout
     * is no longer load-bearing (no ping is issued) but the parameter is
     * retained so the wiring in {@code CacheConfig} and existing test
     * fixtures continue to compile against the same shape.
     *
     * @deprecated use {@link #HybridCacheHealthIndicator(RedissonClient,
     *             CircuitBreakerRegistry, HybridCacheManager)}; the
     *             {@code pingTimeout} argument is ignored.
     */
    @Deprecated
    public HybridCacheHealthIndicator(RedissonClient redisson,
                                      CircuitBreakerRegistry breakerRegistry,
                                      HybridCacheManager cacheManager,
                                      Duration pingTimeout) {
        this(redisson, breakerRegistry, cacheManager);
    }

    public HybridCacheHealthIndicator(RedissonClient redisson,
                                      CircuitBreakerRegistry breakerRegistry,
                                      HybridCacheManager cacheManager) {
        this.redisson = redisson;
        this.breakerRegistry = breakerRegistry;
        this.cacheManager = cacheManager;
    }

    @Override
    public Health health() {
        AggregateState aggregate = aggregateBreakerState();

        Health.Builder builder = switch (aggregate.worst()) {
            case OPEN, FORCED_OPEN -> Health.outOfService();
            case HALF_OPEN -> Health.unknown();
            case CLOSED, METRICS_ONLY, DISABLED -> Health.up();
        };

        Map<String, Object> caches = buildCacheDetails();
        String redisStatus = deriveRedisStatus(aggregate, caches);

        // The only path to DOWN now: every breaker nominally closed, no
        // cache has executed any Redis call yet (so the breaker activity
        // signal is silent), and the Redisson client itself is shut down
        // or shutting down. Without that last condition we cannot tell
        // "Redis is unhealthy" from "no traffic yet"; we prefer UP in
        // the silent-but-alive case so a freshly-booted application does
        // not flap DOWN before its first cache miss.
        if (aggregate.allClosed() && !anyBreakerActive() && !anyCacheActive(caches)
                && !redissonAlive()) {
            builder = Health.down();
        }

        builder.withDetail("redisStatus", redisStatus);
        builder.withDetail("caches", caches);
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

    /**
     * The breaker increments {@link CircuitBreaker.Metrics#getNumberOfBufferedCalls()}
     * on every {@code executeXXX} regardless of success or failure, so a
     * non-zero value means the cache's L2 path has actually executed at least
     * one Redis call. Combined with the breaker still being nominally
     * closed, that is direct evidence that Redis is reachable, equivalent to
     * what a successful ping would have told us, but for traffic the
     * production code already issued.
     */
    private boolean anyBreakerActive() {
        for (CircuitBreaker breaker : breakerRegistry.getAllCircuitBreakers()) {
            if (breaker.getMetrics().getNumberOfBufferedCalls() > 0) return true;
        }
        return false;
    }

    /**
     * L1 hit or miss counts as "active" for the purposes of the silent-app
     * fallback. A hit means the cache served from Caffeine which itself was
     * populated by an earlier loader run that exercised L2; a miss means
     * the L2 path was just attempted. Either is evidence the Redis path
     * is being exercised somewhere recent.
     */
    @SuppressWarnings("unchecked")
    private boolean anyCacheActive(Map<String, Object> caches) {
        for (Object value : caches.values()) {
            if (!(value instanceof Map<?, ?> entry)) continue;
            Object hits = ((Map<String, Object>) entry).get("hitCount");
            Object misses = ((Map<String, Object>) entry).get("missCount");
            if (hits instanceof Number h && h.longValue() > 0) return true;
            if (misses instanceof Number m && m.longValue() > 0) return true;
        }
        return false;
    }

    private boolean redissonAlive() {
        return !redisson.isShutdown() && !redisson.isShuttingDown();
    }

    private String deriveRedisStatus(AggregateState aggregate, Map<String, Object> caches) {
        if (!aggregate.allClosed()) {
            return "degraded; see caches.*.breakerState";
        }
        if (anyBreakerActive() || anyCacheActive(caches)) {
            return "reachable via active caches";
        }
        if (redissonAlive()) {
            return "no cache activity; redisson client alive";
        }
        return "redisson client shut down";
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
