package io.github.nwwarm.hybridcache.core;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link SwrSidecar}. The sidecar is a pure in-memory
 * primitive — no Redis, no Caffeine, no Spring context. Tests run fast and
 * cover the decision matrix, the single-flight collapse, refresh failure
 * containment, and breaker-open suppression.
 */
class SwrSidecarTest {

    private SimpleMeterRegistry meterRegistry;
    private RefreshExecutor refreshExecutor;
    private CircuitBreaker breaker;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        refreshExecutor = new RefreshExecutor(2, meterRegistry);
        breaker = CircuitBreakerRegistry.ofDefaults().circuitBreaker("test-cache");
    }

    @AfterEach
    void tearDown() {
        refreshExecutor.stop();
    }

    // -----------------------------------------------------------------------
    // Decision matrix: fresh window (no refresh), stale window (one refresh),
    // expired window is enforced by Caffeine — past stale-for, the wrapper
    // would have been physically evicted before classify() ever ran. The
    // sidecar's classify() therefore covers fresh and stale only.
    // -----------------------------------------------------------------------

    @Test
    void freshWindow_classifyReturnsFresh_noDispatchPossible() {
        SwrSidecar sidecar = new SwrSidecar("c", Duration.ofMinutes(5).toNanos(),
                refreshExecutor, breaker, meterRegistry);
        sidecar.recordWrite("k1");
        // freshFor is 5 minutes — classify must return FRESH well within it.
        assertThat(sidecar.classify("k1")).isEqualTo(SwrSidecar.Classification.FRESH);
        // No dispatch is invoked, so all SWR counters stay at zero.
        assertThat(staleReturns()).isZero();
        assertThat(refreshesStarted()).isZero();
    }

    @Test
    void staleWindow_classifyReturnsStale_dispatchesOneRefresh() throws Exception {
        // freshFor=1ms so we can land in the stale window deterministically
        // by sleeping past it.
        SwrSidecar sidecar = new SwrSidecar("c", Duration.ofMillis(1).toNanos(),
                refreshExecutor, breaker, meterRegistry);
        sidecar.recordWrite("k1");
        Thread.sleep(20);  // well past freshFor=1ms

        assertThat(sidecar.classify("k1")).isEqualTo(SwrSidecar.Classification.STALE);

        AtomicInteger loaderCalls = new AtomicInteger();
        CountDownLatch refreshDone = new CountDownLatch(1);
        sidecar.maybeDispatchRefresh("k1", () -> {
            loaderCalls.incrementAndGet();
            refreshDone.countDown();
            return "fresh-value";
        });
        assertThat(refreshDone.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(loaderCalls.get()).isEqualTo(1);
        assertThat(staleReturns()).isEqualTo(1);
        assertThat(refreshesStarted()).isEqualTo(1);
        assertThat(refreshesCompleted()).isEqualTo(1);
    }

    @Test
    void noSidecarEntry_classifyReturnsFresh() {
        // If a key has no recorded deadline (config swap, race with
        // RemovalListener), classify returns FRESH so we don't dispatch a
        // bogus refresh against an entry that may already have been
        // physically evicted on the next read.
        SwrSidecar sidecar = new SwrSidecar("c", Duration.ofMinutes(5).toNanos(),
                refreshExecutor, breaker, meterRegistry);
        assertThat(sidecar.classify("never-recorded")).isEqualTo(SwrSidecar.Classification.FRESH);
    }

    // -----------------------------------------------------------------------
    // Per-key single-flight collapse: N=100 concurrent stale reads on the
    // same key must dispatch exactly one loader call. The remaining 99
    // increment cache.swr.refreshes.skipped{reason=in_flight}.
    // -----------------------------------------------------------------------

    @Test
    void concurrentStaleReads_collapseToOneLoader() throws Exception {
        SwrSidecar sidecar = new SwrSidecar("c", Duration.ofMillis(1).toNanos(),
                refreshExecutor, breaker, meterRegistry);
        sidecar.recordWrite("hot-key");
        Thread.sleep(20);

        int n = 100;
        AtomicInteger loaderCalls = new AtomicInteger();
        CountDownLatch loaderEntered = new CountDownLatch(1);
        CountDownLatch loaderRelease = new CountDownLatch(1);

        // The loader blocks until release, so all N callers see the in-flight
        // future before the first runs to completion. This makes the collapse
        // assertion deterministic — without the block, Thread A could finish
        // before Thread B even calls maybeDispatchRefresh, and B would see
        // the in-flight slot already cleared.
        ExecutorService callers = Executors.newFixedThreadPool(n);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(n);
        try {
            for (int i = 0; i < n; i++) {
                callers.submit(() -> {
                    try {
                        start.await();
                        sidecar.maybeDispatchRefresh("hot-key", () -> {
                            loaderCalls.incrementAndGet();
                            loaderEntered.countDown();
                            loaderRelease.await();
                            return "v";
                        });
                    } catch (InterruptedException ignored) {
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();  // release all callers
            assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
            // The single loader has entered (or is about to) — wait for it
            // before asserting the collapse.
            assertThat(loaderEntered.await(2, TimeUnit.SECONDS)).isTrue();

            // Exactly one loader call across N concurrent stale reads.
            assertThat(loaderCalls.get()).isEqualTo(1);
            // Stale-returns increments on every classify() — once per caller.
            assertThat(staleReturns()).isEqualTo(n);
            // Started=1 (the lone winner), skipped=N-1 (the duplicates).
            assertThat(refreshesStarted()).isEqualTo(1);
            assertThat(refreshesSkippedInFlight()).isEqualTo(n - 1);

            // Release the loader so executor cleanup proceeds.
            loaderRelease.countDown();
        } finally {
            callers.shutdown();
            callers.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    // -----------------------------------------------------------------------
    // Refresh failure: the loader's exception is caught inside the executor
    // task, the failure metric increments, and no exception escapes to the
    // calling reader. Subsequent stale reads can re-attempt because the
    // in-flight slot is freed in finally.
    // -----------------------------------------------------------------------

    @Test
    void refreshFailure_caughtAndRecorded_stillFreesInflight() throws Exception {
        SwrSidecar sidecar = new SwrSidecar("c", Duration.ofMillis(1).toNanos(),
                refreshExecutor, breaker, meterRegistry);
        sidecar.recordWrite("k");
        Thread.sleep(20);

        CountDownLatch firstAttemptDone = new CountDownLatch(1);
        sidecar.maybeDispatchRefresh("k", () -> {
            try {
                throw new RuntimeException("loader blew up");
            } finally {
                firstAttemptDone.countDown();
            }
        });
        assertThat(firstAttemptDone.await(2, TimeUnit.SECONDS)).isTrue();
        // Wait for the executor task to finish unwinding so the in-flight
        // slot is cleared. The metric is incremented inside the catch
        // block, before the finally that removes from inflight.
        await(() -> refreshesFailed() == 1);
        await(() -> sidecar.inflightSize() == 0);

        // A second stale read can dispatch fresh — the slot was freed.
        AtomicInteger secondLoaderCalls = new AtomicInteger();
        CountDownLatch secondDone = new CountDownLatch(1);
        sidecar.maybeDispatchRefresh("k", () -> {
            secondLoaderCalls.incrementAndGet();
            secondDone.countDown();
            return "v";
        });
        assertThat(secondDone.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(secondLoaderCalls.get()).isEqualTo(1);
        assertThat(refreshesFailed()).isEqualTo(1);
        // refreshesCompleted is incremented in the executor lambda after
        // refreshTask.call() returns, but the loader signals secondDone
        // before that return. On a loaded runner the assertion can race
        // ahead of the increment, so poll instead of asserting once.
        await(() -> refreshesCompleted() == 1);
    }

    // -----------------------------------------------------------------------
    // Breaker open: refresh dispatch is suppressed at the calling thread
    // (no executor slot burned), the suppressed counter increments, and the
    // stale value is still returned (caller does NOT block on us).
    // -----------------------------------------------------------------------

    @Test
    void breakerOpen_suppressesRefreshDispatch() {
        breaker.transitionToOpenState();
        SwrSidecar sidecar = new SwrSidecar("c", Duration.ofNanos(1).toNanos(),
                refreshExecutor, breaker, meterRegistry);
        sidecar.recordWrite("k");

        AtomicInteger loaderCalls = new AtomicInteger();
        sidecar.maybeDispatchRefresh("k", () -> {
            loaderCalls.incrementAndGet();
            return "v";
        });
        // The breaker check happens on the calling thread, before submit;
        // the loader must not run.
        assertThat(loaderCalls.get()).isZero();
        assertThat(refreshesSuppressedBreaker()).isEqualTo(1);
        assertThat(refreshesStarted()).isZero();
        // Stale return still increments — the read is about to serve stale.
        assertThat(staleReturns()).isEqualTo(1);
    }

    // -----------------------------------------------------------------------
    // RemovalListener bridge: the owning cache calls onRemoval for every
    // cause except REPLACED. Verify the entry is dropped after the call.
    // The "skip REPLACED" decision is enforced by HybridCacheManager's
    // listener wiring; tested at integration level.
    // -----------------------------------------------------------------------

    @Test
    void onRemoval_dropsDeadlineEntry() {
        SwrSidecar sidecar = new SwrSidecar("c", Duration.ofMinutes(5).toNanos(),
                refreshExecutor, breaker, meterRegistry);
        sidecar.recordWrite("k");
        assertThat(sidecar.sidecarSize()).isEqualTo(1);
        sidecar.onRemoval("k");
        assertThat(sidecar.sidecarSize()).isZero();
        assertThat(sidecar.classify("k")).isEqualTo(SwrSidecar.Classification.FRESH);
    }

    // -----------------------------------------------------------------------
    // Constructor guards
    // -----------------------------------------------------------------------

    @Test
    void zeroFreshFor_rejected() {
        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                new SwrSidecar("c", 0L, refreshExecutor, breaker, meterRegistry))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("freshForNanos must be positive");
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private double staleReturns() {
        return meterRegistry.find("cache.swr.stale_returns").counter().count();
    }

    private double refreshesStarted() {
        return meterRegistry.find("cache.swr.refreshes")
                .tag("status", "started").counter().count();
    }

    private double refreshesCompleted() {
        return meterRegistry.find("cache.swr.refreshes")
                .tag("status", "completed").counter().count();
    }

    private double refreshesFailed() {
        return meterRegistry.find("cache.swr.refreshes")
                .tag("status", "failed").counter().count();
    }

    private double refreshesSuppressedBreaker() {
        return meterRegistry.find("cache.swr.refreshes")
                .tag("status", "suppressed_breaker_open").counter().count();
    }

    private double refreshesSkippedInFlight() {
        return meterRegistry.find("cache.swr.refreshes.skipped")
                .tag("reason", "in_flight").counter().count();
    }

    /** Polls a condition for up to 2s; useful for awaiting executor finalizers. */
    private static void await(java.util.function.BooleanSupplier cond) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (System.nanoTime() < deadline) {
            if (cond.getAsBoolean()) return;
            Thread.sleep(5);
        }
        throw new AssertionError("condition not met within 2s");
    }
}
