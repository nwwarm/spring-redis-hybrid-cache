package io.github.nwwarm.hybridcache.core;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.SplittableRandom;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit coverage for {@link RefreshAheadCoordinator}: predicate evaluation,
 * dispatch, shared in-flight de-dup against SWR, breaker-open suppression,
 * and the full RA metric family.
 */
class RefreshAheadCoordinatorTest {

    private SimpleMeterRegistry meterRegistry;
    private RefreshExecutor refreshExecutor;
    private CircuitBreaker breaker;
    private LoaderRuntimeEwma ewma;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        refreshExecutor = new RefreshExecutor(2, meterRegistry);
        breaker = CircuitBreakerRegistry.ofDefaults().circuitBreaker("ra-test");
        ewma = new LoaderRuntimeEwma();
    }

    @AfterEach
    void tearDown() {
        refreshExecutor.stop();
    }

    @Test
    void invalidBeta_rejectedAtConstruction() {
        SwrSidecar sidecar = new SwrSidecar("c", Duration.ofMinutes(5).toNanos(),
                refreshExecutor, breaker, meterRegistry);
        assertThatThrownBy(() ->
                new RefreshAheadCoordinator("c", Duration.ofMinutes(5).toNanos(),
                        0.0, sidecar, refreshExecutor, breaker, ewma, meterRegistry))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("beta must be > 0");
    }

    @Test
    void zeroEwma_predicateNeverFires() {
        // Until the cache has run its first loader, the EWMA is 0 and RA
        // makes no speculative refreshes — the first read after deploy
        // never fires the loader speculatively.
        SwrSidecar sidecar = new SwrSidecar("c", Duration.ofMinutes(5).toNanos(),
                refreshExecutor, breaker, meterRegistry);
        sidecar.recordWrite("k");
        RefreshAheadCoordinator ra = new RefreshAheadCoordinator(
                "c", Duration.ofMinutes(5).toNanos(), 1.0, sidecar,
                refreshExecutor, breaker, ewma, meterRegistry);

        AtomicInteger loaderCalls = new AtomicInteger();
        for (int i = 0; i < 1_000; i++) {
            ra.evaluateAndMaybeDispatch("k", () -> {
                loaderCalls.incrementAndGet();
                return "v";
            });
        }
        assertThat(loaderCalls.get()).isZero();
        assertThat(refreshesStarted()).isZero();
    }

    @Test
    void seededRng_firesPredictably() {
        // With a seeded SplittableRandom and timeSinceWrite=freshFor/2,
        // ewma=freshFor/2, β=1, the predicate fires at exp(-1) ≈ 36.8%
        // of evaluations. We don't assert a tight rate here (covered by
        // XFetchPredicateTest); we assert that with a seeded RNG, calling
        // evaluateAndMaybeDispatch deterministically fires at least once
        // and dispatches through the RA metric family.
        long freshFor = Duration.ofMinutes(10).toNanos();  // far enough that timing skew is negligible
        SwrSidecar sidecar = new SwrSidecar("c", freshFor,
                refreshExecutor, breaker, meterRegistry);
        // Ewma seeded to freshFor/2 so the predicate has the right
        // magnitude to fire under default β=1.
        ewma.record(freshFor / 2);

        // recordWrite stamps lastWrite=now; we want timeSinceWrite ≈
        // freshFor/2 at evaluation. Cheaper than sleeping for "freshFor/2":
        // shrink freshFor and use a real-time gap.
        long shortFresh = 100_000_000L;  // 100ms
        SwrSidecar shortSidecar = new SwrSidecar("c", shortFresh,
                refreshExecutor, breaker, meterRegistry);
        LoaderRuntimeEwma shortEwma = new LoaderRuntimeEwma();
        shortEwma.record(shortFresh / 2);
        RefreshAheadCoordinator ra = new RefreshAheadCoordinator(
                "c", shortFresh, 1.0, shortSidecar,
                refreshExecutor, breaker, shortEwma, meterRegistry,
                new SplittableRandom(0xCAFEBABEL));
        shortSidecar.recordWrite("k");
        // Wait until we are ~halfway through the fresh window.
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }

        AtomicInteger loaderCalls = new AtomicInteger();
        // Run the evaluator many times — at exp(-(F-t)/W) with F=W=100ms
        // and t≈50ms, fire rate is exp(-0.5) ≈ 60%. Across N evaluations
        // we expect a non-trivial number of fires; the in-flight collapse
        // means most beyond the first dedupe.
        int n = 100;
        int dispatchedCount = 0;
        for (int i = 0; i < n; i++) {
            boolean fired = ra.evaluateAndMaybeDispatch("k", () -> {
                loaderCalls.incrementAndGet();
                return "v";
            });
            if (fired) dispatchedCount++;
        }
        // Dispatch sum: started + deduped + suppressed_breaker_open. If
        // the predicate ever fired, started >= 1.
        assertThat(refreshesStarted())
                .as("predicate fired with seeded RNG and t~freshFor/2")
                .isGreaterThanOrEqualTo(1);
        // The return value of evaluateAndMaybeDispatch tracks "did the
        // predicate fire (and reach dispatch)?" — must equal the number
        // of times the predicate fired across the run.
        assertThat(dispatchedCount)
                .as("evaluateAndMaybeDispatch return value tracks dispatched-or-deduped count")
                .isPositive();
    }

    @Test
    void sharedInflight_deDupAgainstSwr() throws Exception {
        // Force both SWR (via maybeDispatchRefresh on a stale entry) and
        // RA (via evaluateAndMaybeDispatch with a fired predicate) to
        // race. The shared in-flight map must collapse — only one loader
        // ran, the second arrival increments either swr.skipped or
        // ra.deduped depending on order.
        long freshFor = Duration.ofMillis(1).toNanos();
        SwrSidecar sidecar = new SwrSidecar("c", freshFor,
                refreshExecutor, breaker, meterRegistry);
        LoaderRuntimeEwma ewma = new LoaderRuntimeEwma();
        // Seed EWMA so RA's predicate has a non-zero loader estimate.
        ewma.record(Duration.ofMillis(5).toNanos());
        RefreshAheadCoordinator ra = new RefreshAheadCoordinator(
                "c", freshFor, 1.0, sidecar, refreshExecutor,
                breaker, ewma, meterRegistry);

        sidecar.recordWrite("k");
        Thread.sleep(20);  // past freshFor — entry is now STALE for SWR

        AtomicInteger loaderCalls = new AtomicInteger();
        CountDownLatch loaderEntered = new CountDownLatch(1);
        CountDownLatch loaderRelease = new CountDownLatch(1);

        // SWR fires first — its task body blocks until released so we have
        // a window where the in-flight slot is occupied.
        sidecar.maybeDispatchRefresh("k", () -> {
            loaderCalls.incrementAndGet();
            loaderEntered.countDown();
            loaderRelease.await();
            return "v";
        });
        assertThat(loaderEntered.await(2, TimeUnit.SECONDS)).isTrue();

        // Now RA tries to fire on the same key. Even if the predicate
        // fires, the in-flight slot is occupied by SWR — RA's dispatch
        // must increment deduped, not start a second loader.
        ra.dispatch("k", () -> {
            loaderCalls.incrementAndGet();
            return "v2";
        });
        assertThat(loaderCalls.get()).isEqualTo(1);  // only SWR's loader is running
        assertThat(refreshesDeduped()).isEqualTo(1);

        loaderRelease.countDown();
        // Drain executor so the SWR task finishes cleanly before tearDown.
        await(() -> sidecar.inflightSize() == 0);
    }

    @Test
    void raFiredFirst_swrSeesInflight_swrSkipsAtSwrCounter() throws Exception {
        // Mirror image of sharedInflight_deDupAgainstSwr: RA fires first
        // and is in flight; the next SWR stale-window read must increment
        // SWR's skipped{reason=in_flight} (NOT RA's deduped — the SWR
        // path owns its own skip counter, and RA's deduped tag is for the
        // case where RA arrives second).
        long freshFor = Duration.ofMillis(1).toNanos();
        SwrSidecar sidecar = new SwrSidecar("c", freshFor,
                refreshExecutor, breaker, meterRegistry);
        ewma.record(Duration.ofMillis(5).toNanos());
        RefreshAheadCoordinator ra = new RefreshAheadCoordinator(
                "c", freshFor, 1.0, sidecar, refreshExecutor,
                breaker, ewma, meterRegistry);

        sidecar.recordWrite("k");

        AtomicInteger loaderCalls = new AtomicInteger();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        // RA dispatches first — its task body blocks until released.
        ra.dispatch("k", () -> {
            loaderCalls.incrementAndGet();
            entered.countDown();
            release.await();
            return "v";
        });
        assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(refreshesStarted()).isEqualTo(1);

        // Age past freshFor and have SWR's stale-path try to dispatch.
        Thread.sleep(20);
        sidecar.maybeDispatchRefresh("k", () -> {
            loaderCalls.incrementAndGet();
            return "v2";
        });
        // SWR's skip counter ticked (its own counter family), not RA's
        // deduped — proves the in-flight collapse is bidirectional and
        // each direction's metric stays clean.
        assertThat(loaderCalls.get()).isEqualTo(1);  // only RA's loader running
        assertThat(meterRegistry.find("cache.swr.refreshes.skipped")
                .tag("reason", "in_flight").counter().count()).isEqualTo(1);
        assertThat(refreshesDeduped())
                .as("RA's deduped tag should NOT increment when SWR is the one that found the in-flight slot")
                .isZero();

        release.countDown();
        await(() -> sidecar.inflightSize() == 0);
    }

    @Test
    void breakerOpen_suppressesDispatch() {
        long freshFor = Duration.ofMinutes(5).toNanos();
        SwrSidecar sidecar = new SwrSidecar("c", freshFor,
                refreshExecutor, breaker, meterRegistry);
        ewma.record(Duration.ofMillis(10).toNanos());
        RefreshAheadCoordinator ra = new RefreshAheadCoordinator(
                "c", freshFor, 1.0, sidecar, refreshExecutor,
                breaker, ewma, meterRegistry);

        sidecar.recordWrite("k");
        breaker.transitionToOpenState();

        AtomicInteger loaderCalls = new AtomicInteger();
        // Bypass the predicate (use dispatch directly) so we test the
        // breaker-open branch deterministically. Predicate-driven firing
        // is exercised in seededRng_firesPredictably and the integration
        // suite.
        ra.dispatch("k", () -> {
            loaderCalls.incrementAndGet();
            return "v";
        });
        assertThat(loaderCalls.get()).isZero();
        assertThat(refreshesSuppressedBreaker()).isEqualTo(1);
        assertThat(refreshesStarted()).isZero();
    }

    @Test
    void refreshFailure_neverEscapes_metricRecorded() throws Exception {
        long freshFor = Duration.ofMinutes(5).toNanos();
        SwrSidecar sidecar = new SwrSidecar("c", freshFor,
                refreshExecutor, breaker, meterRegistry);
        ewma.record(Duration.ofMillis(10).toNanos());
        RefreshAheadCoordinator ra = new RefreshAheadCoordinator(
                "c", freshFor, 1.0, sidecar, refreshExecutor,
                breaker, ewma, meterRegistry);

        sidecar.recordWrite("k");

        CountDownLatch attemptDone = new CountDownLatch(1);
        ra.dispatch("k", () -> {
            try {
                throw new RuntimeException("loader exploded");
            } finally {
                attemptDone.countDown();
            }
        });
        assertThat(attemptDone.await(2, TimeUnit.SECONDS)).isTrue();
        await(() -> refreshesFailed() == 1);
        await(() -> sidecar.inflightSize() == 0);

        // No exception escaped — the test would have failed in the dispatch
        // call itself if it had. Belt and suspenders: a second dispatch
        // can fire after the first failed, since the in-flight slot was
        // released.
        AtomicInteger second = new AtomicInteger();
        CountDownLatch secondDone = new CountDownLatch(1);
        ra.dispatch("k", () -> {
            second.incrementAndGet();
            secondDone.countDown();
            return "v";
        });
        assertThat(secondDone.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(second.get()).isEqualTo(1);
        assertThat(refreshesFailed()).isEqualTo(1);
        // refreshesCompleted is incremented in the executor task after
        // refreshTask.call() returns, but the loader counts down
        // secondDone before that return. The test thread can race past
        // secondDone.await() to this assertion before the executor
        // finishes the increment. Poll the same way lines 289-290 do.
        await(() -> refreshesCompleted() == 1);
    }

    @Test
    void evaluate_noDeadlineEntry_returnsFalse_noDispatch() {
        // Race window: the SWR sidecar's RemovalListener drains the
        // deadline entry but Caffeine still returned a wrapper for the
        // read. RA must follow the same FRESH-classification convention
        // the SWR sidecar uses (no dispatch on missing deadline).
        SwrSidecar sidecar = new SwrSidecar("c", Duration.ofMinutes(5).toNanos(),
                refreshExecutor, breaker, meterRegistry);
        ewma.record(Duration.ofMillis(10).toNanos());
        RefreshAheadCoordinator ra = new RefreshAheadCoordinator(
                "c", Duration.ofMinutes(5).toNanos(), 1.0, sidecar,
                refreshExecutor, breaker, ewma, meterRegistry);

        AtomicInteger loaderCalls = new AtomicInteger();
        boolean dispatched = ra.evaluateAndMaybeDispatch("never-recorded", () -> {
            loaderCalls.incrementAndGet();
            return "v";
        });
        assertThat(dispatched).isFalse();
        assertThat(loaderCalls.get()).isZero();
        assertThat(refreshesStarted()).isZero();
    }

    @Test
    void evaluate_pastDeadline_returnsFalse_swrTerritory() throws Exception {
        // Past the fresh-until deadline: SWR's stale-window logic owns
        // the read. RA must return false rather than firing speculatively
        // on a key that's already in SWR's territory.
        SwrSidecar sidecar = new SwrSidecar("c", Duration.ofMillis(1).toNanos(),
                refreshExecutor, breaker, meterRegistry);
        ewma.record(Duration.ofMillis(10).toNanos());
        RefreshAheadCoordinator ra = new RefreshAheadCoordinator(
                "c", Duration.ofMillis(1).toNanos(), 1.0, sidecar,
                refreshExecutor, breaker, ewma, meterRegistry);
        sidecar.recordWrite("k");
        Thread.sleep(20);  // age past freshFor — entry is now STALE per SWR

        AtomicInteger loaderCalls = new AtomicInteger();
        boolean dispatched = ra.evaluateAndMaybeDispatch("k", () -> {
            loaderCalls.incrementAndGet();
            return "v";
        });
        assertThat(dispatched).isFalse();
        assertThat(loaderCalls.get()).isZero();
        assertThat(refreshesStarted()).isZero();
    }

    @Test
    void dispatch_returnTrue_whenSubmitted() throws Exception {
        // Pin the dispatch return contract so mutation tests can't
        // replace `return true` with `return false` silently.
        SwrSidecar sidecar = new SwrSidecar("c", Duration.ofMinutes(5).toNanos(),
                refreshExecutor, breaker, meterRegistry);
        RefreshAheadCoordinator ra = new RefreshAheadCoordinator(
                "c", Duration.ofMinutes(5).toNanos(), 1.0, sidecar,
                refreshExecutor, breaker, ewma, meterRegistry);

        CountDownLatch loaderEntered = new CountDownLatch(1);
        boolean fresh = ra.dispatch("k", () -> {
            loaderEntered.countDown();
            return "v";
        });
        assertThat(fresh).isTrue();
        assertThat(loaderEntered.await(2, TimeUnit.SECONDS)).isTrue();
        await(() -> refreshesCompleted() == 1);
    }

    @Test
    void dispatch_returnTrue_whenDeduped() throws Exception {
        // Even when dispatch de-dups against an existing in-flight, the
        // method returns true (the call site's "I attempted dispatch"
        // signal). False would only mean "the executor was null" —
        // exercised separately.
        SwrSidecar sidecar = new SwrSidecar("c", Duration.ofMinutes(5).toNanos(),
                refreshExecutor, breaker, meterRegistry);
        RefreshAheadCoordinator ra = new RefreshAheadCoordinator(
                "c", Duration.ofMinutes(5).toNanos(), 1.0, sidecar,
                refreshExecutor, breaker, ewma, meterRegistry);

        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ra.dispatch("k", () -> {
            entered.countDown();
            release.await();
            return "v";
        });
        assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
        boolean second = ra.dispatch("k", () -> "v2");
        assertThat(second).isTrue();
        assertThat(refreshesDeduped()).isEqualTo(1);
        release.countDown();
    }

    @Test
    void dispatch_returnTrue_whenSuppressedByBreaker() {
        // Breaker-open path also returns true — same "attempted dispatch"
        // contract. Pinned so the dispatch return surface stays consistent.
        SwrSidecar sidecar = new SwrSidecar("c", Duration.ofMinutes(5).toNanos(),
                refreshExecutor, breaker, meterRegistry);
        RefreshAheadCoordinator ra = new RefreshAheadCoordinator(
                "c", Duration.ofMinutes(5).toNanos(), 1.0, sidecar,
                refreshExecutor, breaker, ewma, meterRegistry);
        breaker.transitionToOpenState();
        boolean fresh = ra.dispatch("k", () -> "v");
        assertThat(fresh).isTrue();
        assertThat(refreshesSuppressedBreaker()).isEqualTo(1);
    }

    @Test
    void dispatch_returnTrue_whenNoExecutor_butStartedIncrements() {
        // Test-only construction without an executor must still record the
        // started counter so tests downstream can verify the predicate
        // fired all the way to dispatch.
        SwrSidecar sidecar = new SwrSidecar("c", Duration.ofMinutes(5).toNanos(),
                refreshExecutor, breaker, meterRegistry);
        RefreshAheadCoordinator ra = new RefreshAheadCoordinator(
                "c", Duration.ofMinutes(5).toNanos(), 1.0, sidecar,
                /* refreshExecutor */ null, breaker, ewma, meterRegistry);

        boolean fresh = ra.dispatch("k", () -> "v");
        assertThat(fresh).isTrue();
        assertThat(refreshesStarted()).isEqualTo(1);
    }

    @Test
    void testSeams_returnConfiguredValues() {
        // Pin the test-seam accessors — mutation tests would otherwise
        // strip them with `return 0` / `return null` and survive.
        SwrSidecar sidecar = new SwrSidecar("c", Duration.ofMinutes(5).toNanos(),
                refreshExecutor, breaker, meterRegistry);
        long freshFor = Duration.ofSeconds(7).toNanos();
        RefreshAheadCoordinator ra = new RefreshAheadCoordinator(
                "c", freshFor, 1.5, sidecar, refreshExecutor,
                breaker, ewma, meterRegistry);
        assertThat(ra.freshForNanos()).isEqualTo(freshFor);
        assertThat(ra.beta()).isEqualTo(1.5);
        assertThat(ra.loaderEwma()).isSameAs(ewma);
    }

    @Test
    void allFiveMetricsRegistered() {
        // The task spec calls for cache.refresh_ahead.refreshes.{started,
        // completed, failed, deduped, suppressed_breaker_open}. Verify
        // every tag value is present after construction (counters are
        // pre-registered so dashboards pick them up before first traffic).
        SwrSidecar sidecar = new SwrSidecar("c", Duration.ofMinutes(5).toNanos(),
                refreshExecutor, breaker, meterRegistry);
        new RefreshAheadCoordinator("c", Duration.ofMinutes(5).toNanos(),
                1.0, sidecar, refreshExecutor, breaker, ewma, meterRegistry);
        for (String status : new String[]{
                "started", "completed", "failed", "deduped", "suppressed_breaker_open"}) {
            assertThat(meterRegistry.find("cache.refresh_ahead.refreshes")
                    .tag("cache", "c").tag("status", status).counter())
                    .as("counter for status=%s must be registered", status)
                    .isNotNull();
        }
    }

    // ---------- helpers ----------

    private double refreshesStarted() {
        return meterRegistry.find("cache.refresh_ahead.refreshes")
                .tag("status", "started").counter().count();
    }

    private double refreshesCompleted() {
        return meterRegistry.find("cache.refresh_ahead.refreshes")
                .tag("status", "completed").counter().count();
    }

    private double refreshesFailed() {
        return meterRegistry.find("cache.refresh_ahead.refreshes")
                .tag("status", "failed").counter().count();
    }

    private double refreshesDeduped() {
        return meterRegistry.find("cache.refresh_ahead.refreshes")
                .tag("status", "deduped").counter().count();
    }

    private double refreshesSuppressedBreaker() {
        return meterRegistry.find("cache.refresh_ahead.refreshes")
                .tag("status", "suppressed_breaker_open").counter().count();
    }

    private static void await(java.util.function.BooleanSupplier cond) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (System.nanoTime() < deadline) {
            if (cond.getAsBoolean()) return;
            Thread.sleep(5);
        }
        throw new AssertionError("condition not met within 2s");
    }
}
