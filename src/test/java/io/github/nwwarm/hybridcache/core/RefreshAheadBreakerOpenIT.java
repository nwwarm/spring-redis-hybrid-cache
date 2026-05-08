package io.github.nwwarm.hybridcache.core;

import io.github.nwwarm.hybridcache.testfixtures.RedisTestBase;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;

import java.time.Duration;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * RA + breaker open: the speculative refresh dispatch is suppressed at
 * the calling thread (no executor slot burned), the
 * {@code suppressed_breaker_open} counter increments, and the read
 * still returns synchronously with the cached value. Same shape as
 * {@link SwrBreakerOpenIT}, but exercising the RA predicate path rather
 * than the SWR stale path.
 */
class RefreshAheadBreakerOpenIT extends RedisTestBase {

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
    void breakerOpen_raDispatchSuppressed_metricRecorded() throws Exception {
        CircuitBreaker breaker = defaultBreaker();
        // Use a seeded RNG that pegs U very small so the predicate fires
        // even for small time-since-write values; this keeps the test
        // deterministic without needing to age the entry.
        NearCache cache = newRaNearCache("ra-breaker", redisson, breaker, "node",
                meterRegistry, refreshExecutor,
                Duration.ofMinutes(10),
                Duration.ofMinutes(1),     // freshFor — long, so we stay fresh
                Duration.ofMinutes(2),     // staleFor
                1.0,
                new SplittableRandom(0xCAFE_F00DL));

        // Seed the EWMA via a cold load so RA's predicate has a non-zero
        // loader estimate.
        cache.get("k", () -> "v0");

        // Open the breaker AFTER the cold load completes, so the load
        // itself succeeded — simulates Redis going unhealthy after a
        // successful write.
        breaker.transitionToOpenState();

        AtomicInteger loaderCalls = new AtomicInteger();
        // Drive a number of reads. The seeded RNG on the RA path will
        // fire the XFetch predicate eventually; with the breaker open,
        // every fired dispatch must increment suppressed_breaker_open
        // and skip the loader entirely.
        for (int i = 0; i < 50; i++) {
            String got = cache.get("k", () -> {
                loaderCalls.incrementAndGet();
                return "should-not-call-async";
            });
            assertThat(got).isEqualTo("v0");
        }
        // Loader must not have been invoked async — the breaker check on
        // the RA dispatch path catches it. (Cold-load already counted as
        // a single sync invocation; that happened before the breaker
        // opened, so it doesn't bump async loaderCalls observed here.)
        assertThat(loaderCalls.get()).isZero();

        // suppressed_breaker_open must have ticked at least once across
        // 50 reads — the seeded RNG with β=1, EWMA seeded by the cold
        // load, and a long freshFor produces a non-zero fire rate.
        assertThat(raSuppressedBreaker())
                .as("seeded RNG should fire the predicate at least once across 50 reads")
                .isGreaterThanOrEqualTo(1);
        // No async refreshes started — every fire was suppressed.
        assertThat(raStarted()).isZero();
    }

    private double raStarted() {
        return meterRegistry.find("cache.refresh_ahead.refreshes")
                .tag("status", "started").counter().count();
    }

    private double raSuppressedBreaker() {
        return meterRegistry.find("cache.refresh_ahead.refreshes")
                .tag("status", "suppressed_breaker_open").counter().count();
    }
}
