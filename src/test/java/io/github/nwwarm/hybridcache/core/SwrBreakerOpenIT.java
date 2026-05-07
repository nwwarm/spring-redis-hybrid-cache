package io.github.nwwarm.hybridcache.core;

import io.github.nwwarm.hybridcache.testfixtures.RedisTestBase;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SWR behavior when the per-cache circuit breaker is open: the stale
 * value is still served (cache must remain available — that is the whole
 * point of SWR), but the async refresh dispatch is suppressed and the
 * suppression metric increments. Firing the loader against a downstream
 * during a Redis incident is exactly the stampede SWR is designed to
 * prevent.
 */
class SwrBreakerOpenIT extends RedisTestBase {

    private RedissonClient redisson;
    private RefreshExecutor refreshExecutor;
    private SimpleMeterRegistry meterRegistry;

    @BeforeEach
    void setUp() {
        redisson = newRedisson();
        meterRegistry = new SimpleMeterRegistry();
        refreshExecutor = new RefreshExecutor(2, meterRegistry);
    }

    @AfterEach
    void tearDown() {
        try {
            refreshExecutor.stop();
        } finally {
            if (redisson != null) redisson.shutdown();
        }
    }

    @Test
    void breakerOpen_staleReadReturnsStale_refreshSuppressed_metricRecorded() throws Exception {
        CircuitBreaker breaker = defaultBreaker();
        NearCache cache = newSwrNearCache("swr-breaker", redisson, breaker, "node",
                meterRegistry, refreshExecutor,
                Duration.ofMinutes(10),
                Duration.ofMillis(30),
                Duration.ofMinutes(1),
                null);

        cache.put("k", "v0");
        Thread.sleep(80);  // past freshFor

        // Open the breaker AFTER the put so the put itself succeeded —
        // simulates Redis going unhealthy after a successful write.
        breaker.transitionToOpenState();

        AtomicInteger loaderCalls = new AtomicInteger();
        // Stale read with the breaker open: stale value is still served
        // (caller is unaffected), but dispatch is suppressed.
        Object got = cache.get("k", () -> {
            loaderCalls.incrementAndGet();
            return "should-not-call";
        });
        assertThat(got).isEqualTo("v0");
        // Loader must not have been invoked synchronously OR asynchronously
        // — the breaker check on the dispatch path catches it.
        assertThat(loaderCalls.get()).isZero();
        assertThat(refreshesSuppressedBreaker()).isEqualTo(1);
        assertThat(refreshesStarted()).isZero();
        assertThat(staleReturns()).isEqualTo(1);
    }

    private double staleReturns() {
        return meterRegistry.find("cache.swr.stale_returns").counter().count();
    }

    private double refreshesStarted() {
        return meterRegistry.find("cache.swr.refreshes")
                .tag("status", "started").counter().count();
    }

    private double refreshesSuppressedBreaker() {
        return meterRegistry.find("cache.swr.refreshes")
                .tag("status", "suppressed_breaker_open").counter().count();
    }
}
