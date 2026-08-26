package io.github.nwwarm.hybridcache.core;

import io.github.nwwarm.hybridcache.testfixtures.RedisTestBase;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RedissonClient;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Detection-logic integration tests for {@link NearCache#reconcile()}. Uses
 * Testcontainers Redis but bypasses the scheduler — the test drives the seq
 * counter directly and calls {@code reconcile()} synchronously. The matrix
 * exhausts the three branches of the comparison (no miss, miss, regression)
 * across the tolerance threshold.
 *
 * <p>The pure-unit version of the comparison classification (no Redis,
 * no cache) lives in {@link ReconciliationDecisionTest} — that file
 * exercises the in-memory rule across boundary values; this file checks
 * the wiring through to a real {@code RAtomicLong} and a real Caffeine
 * cache.
 */
class ReconcilerDetectionIT extends RedisTestBase {

    private RedissonClient redisson;
    private SimpleMeterRegistry meterRegistry;
    private NearCache cache;
    private final String name = "detect-" + System.nanoTime();

    @BeforeEach
    void setUp() {
        redisson = newRedisson();
        meterRegistry = new SimpleMeterRegistry();
        cache = newReconcilingNearCache(name, redisson, defaultBreaker(),
                "node-A", meterRegistry, Duration.ofMinutes(10), 5);
    }

    @AfterEach
    void tearDown() {
        cache.shutdown();
        redisson.shutdown();
    }

    @Test
    void delta_zero_doesNotDeclareMiss() {
        // Both sides at 0; the cycle is a no-op.
        cache.reconcile();
        assertThat(missesCounter()).isZero();
        assertThat(cyclesCounter()).isOne();
    }

    @Test
    void delta_belowTolerance_doesNotDeclareMiss() {
        // Bump canonical to 5 — exactly the tolerance — so delta == tolerance,
        // which is NOT > tolerance and therefore not a miss.
        canonicalSeq().set(5);
        cache.reconcile();
        assertThat(missesCounter()).isZero();
    }

    @Test
    void delta_atTolerance_isStillNotAMiss() {
        // Boundary check — tolerance is "delta MUST EXCEED tolerance to count."
        canonicalSeq().set(5);
        cache.reconcile();
        assertThat(missesCounter()).isZero();
    }

    @Test
    void delta_aboveTolerance_declaresMiss_andClearsLocalL1() {
        // Seed L1 so the recovery action has something to clear.
        cache.put("k", "v");
        assertThat(nativeOf(cache).getIfPresent("k")).isNotNull();

        // Canonical jumps to 100; lastObservedSeq is 0; delta=100 > tolerance=5.
        canonicalSeq().set(100);
        cache.reconcile();

        assertThat(missesCounter())
                .as("delta above tolerance must declare a miss")
                .isOne();
        assertThat(nativeOf(cache).getIfPresent("k"))
                .as("miss recovery must clear the local L1")
                .isNull();
        assertThat(cache.lastObservedSeq())
                .as("lastObservedSeq jumps to redisSeq so the next cycle doesn't re-fire")
                .isEqualTo(100L);
    }

    @Test
    void afterMiss_nextCycle_doesNotReFire() {
        cache.put("k", "v");
        canonicalSeq().set(100);
        cache.reconcile();
        assertThat(missesCounter()).isOne();

        // The watermark just jumped; another cycle with no further publishes
        // must not re-detect the same gap.
        cache.reconcile();
        assertThat(missesCounter())
                .as("watermark advance prevents re-firing on the same gap")
                .isOne();
    }

    @Test
    void regression_doesNotDeclareMiss_andResetsWatermark() {
        // Force a high watermark via the message-observed path.
        cache.onMessageObserved(50L);
        assertThat(cache.lastObservedSeq()).isEqualTo(50L);

        // Operator deletes <cache>:seq externally — value defaults to 0.
        canonicalSeq().delete();

        cache.reconcile();
        assertThat(missesCounter())
                .as("regression is a separate signal, not a miss")
                .isZero();
        assertThat(regressionsCounter()).isOne();
        assertThat(recheckResolvedCounter())
                .as("genuine deletion is not a race; recheck must not resolve it")
                .isZero();
        assertThat(cache.lastObservedSeq())
                .as("regression resets the watermark to the new redisSeq")
                .isZero();
    }

    @Test
    void concurrentPublishes_withReconcileLoop_doNotProduceFalseRegressions() throws Exception {
        // The reconciler's Redis-seq read and its watermark read are not
        // atomic. Under sustained publisher concurrency a stale redisSeq
        // snapshot paired with an up-to-date observed watermark trips the
        // REGRESSION branch despite no genuine counter loss. As of 1.0.3 the
        // bracketed read (pre-GET sample for REGRESSION, post-GET sample for
        // MISS) settles this without needing the recheck; the 1.0.2
        // breaker-mediated recheck remains as a second line of defence.
        //
        // Pre-1.0.2: regressions > 0 within seconds.
        // 1.0.2: regressions == 0, but the *misses* counter was never asserted
        // and it was not zero. The recheck resolved the suspected regression
        // and then re-classified a FRESH redisSeq against the STALE observed
        // snapshot — a delta of tens — so nearly every suppressed regression
        // came back as a MISS and cleared L1. Measured on this test at 1.0.2:
        // ~11k misses in 5s. The regression-only assertion hid it completely.
        // 1.0.3: the bracketed read keeps the cycle out of the recheck path
        // altogether, and misses go to 0.
        //
        // writers MUST stay <= the configured missTolerance (5, from setUp).
        // Each writer holds at most one INCR whose reply has landed but whose
        // watermark bump has not yet run, so the watermark lags the canonical
        // by at most `writers`. Above the tolerance that lag is a *correct*
        // miss declaration, not a false one, and this assertion would flake.
        // Four writers still reproduces the defect decisively (~290 misses at
        // 1.0.2 vs 0 here), so there is nothing to buy by raising it.
        int writers = 4;
        Duration duration = Duration.ofSeconds(5);
        AtomicBoolean stop = new AtomicBoolean(false);
        CountDownLatch ready = new CountDownLatch(writers);
        CountDownLatch go = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(writers);
        RAtomicLong seq = canonicalSeq();
        try {
            for (int i = 0; i < writers; i++) {
                pool.submit(() -> {
                    ready.countDown();
                    try { go.await(); } catch (InterruptedException e) { return; }
                    while (!stop.get()) {
                        // Mimic publishInvalidationAsync's INCR + self-bump
                        // pair without paying for a full put() (Caffeine +
                        // L2 SET + publish). The race is in the watermark/
                        // canonical-seq ordering, not in L1 traffic, so a
                        // direct INCR+observe pair is the right shape.
                        //
                        // Note that onMessageObserved is the *receiver* entry
                        // point standing in for the *publisher* self-bump at
                        // NearCache.publishInvalidationAsync. The substitution
                        // is valid only while both perform the same
                        // accumulateAndGet(seq, Math::max); if the publisher
                        // path ever diverges, this loop stops reproducing the
                        // real interleaving and silently passes. The variant
                        // below drives put() for exactly that reason.
                        long n = seq.incrementAndGet();
                        cache.onMessageObserved(n);
                    }
                });
            }
            ready.await();
            go.countDown();
            long deadline = System.nanoTime() + duration.toNanos();
            long cycles = 0;
            while (System.nanoTime() < deadline) {
                cache.reconcile();
                cycles++;
            }
            stop.set(true);
            pool.shutdown();
            pool.awaitTermination(5, TimeUnit.SECONDS);
            assertThat(regressionsCounter())
                    .as("bracketed read must suppress race-driven false regressions across %d cycles", cycles)
                    .isZero();
            assertThat(missesCounter())
                    .as("and must not cascade into a false miss: the watermark tracks the"
                            + " canonical exactly here, so there is never a real gap")
                    .isZero();
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void concurrentRealPublishes_withReconcileLoop_produceNoRegressionsOrMisses() throws Exception {
        // The companion above simulates the publisher self-bump via
        // onMessageObserved. This one drives the genuine path — put() →
        // publishInvalidationAsync → INCR → self-bump — so the invariant the
        // 1.0.3 fix rests on is exercised end to end against a real Redis:
        //
        //   every value lastObservedSeq holds was durable in Redis before it
        //   was written locally, therefore observedBefore <= redisSeq
        //
        // If that holds, REGRESSION (judged against observedBefore) can never
        // fire without genuine counter loss, and MISS (judged against
        // observedAfter, which on a self-publishing node tracks the canonical)
        // can never fire either.
        int writers = 4;
        Duration duration = Duration.ofSeconds(2);
        AtomicBoolean stop = new AtomicBoolean(false);
        CountDownLatch ready = new CountDownLatch(writers);
        CountDownLatch go = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(writers);
        try {
            for (int i = 0; i < writers; i++) {
                final int writer = i;
                pool.submit(() -> {
                    ready.countDown();
                    try { go.await(); } catch (InterruptedException e) { return; }
                    long n = 0;
                    while (!stop.get()) {
                        cache.put("w" + writer + "-k" + (n++ % 32), "v" + n);
                    }
                });
            }
            ready.await();
            go.countDown();
            long deadline = System.nanoTime() + duration.toNanos();
            long cycles = 0;
            while (System.nanoTime() < deadline) {
                cache.reconcile();
                cycles++;
            }
            stop.set(true);
            pool.shutdown();
            pool.awaitTermination(10, TimeUnit.SECONDS);

            assertThat(cycles)
                    .as("sanity: the loop must actually have driven cycles")
                    .isPositive();
            assertThat(regressionsCounter())
                    .as("real publishes across %d cycles must produce no regression", cycles)
                    .isZero();
            assertThat(missesCounter())
                    .as("nor any miss — this node published everything it is comparing against")
                    .isZero();
            assertThat(cache.lastObservedSeq())
                    .as("watermark never lags the canonical on a self-publishing node")
                    .isEqualTo(canonicalSeq().get());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void onMessageObserved_isMonotonic_underOutOfOrderDelivery() {
        // Simulate cluster-reroute out-of-order delivery: seq 7, then 3, then 5.
        cache.onMessageObserved(7L);
        cache.onMessageObserved(3L);
        cache.onMessageObserved(5L);
        assertThat(cache.lastObservedSeq())
                .as("accumulateAndGet(Math::max) keeps the watermark monotonic")
                .isEqualTo(7L);
    }

    @Test
    void zeroSeq_messages_doNotAdvanceWatermark() {
        cache.onMessageObserved(10L);
        cache.onMessageObserved(0L);     // 0.4.0-publisher payload
        assertThat(cache.lastObservedSeq()).isEqualTo(10L);
    }

    @Test
    void disabled_cache_neverReconciles() {
        // Build a non-reconciling cache and verify reconcile() is a no-op.
        NearCache unconfigured = newNearCache("disabled-" + System.nanoTime(),
                redisson, defaultBreaker(), "node-A");
        try {
            assertThat(unconfigured.isReconciliationEnabled()).isFalse();
            unconfigured.reconcile();
            // With reconciliation disabled the cycle counter should not even
            // be registered. SimpleMeterRegistry returns 0 for find() when the
            // metric was never created.
            assertThat(meterRegistry.find("cache.reconciliation.cycles.completed")
                    .tag("cache", unconfigured.getName()).counter())
                    .as("disabled cache must not register cycle metrics")
                    .isNull();
        } finally {
            unconfigured.shutdown();
        }
    }

    @Test
    void publishOnEvict_advancesCanonicalSeq_byExactlyOne() {
        long before = canonicalSeq().get();
        cache.put("k", "v");
        long afterPut = canonicalSeq().get();
        cache.evict("k");
        long afterEvict = canonicalSeq().get();
        assertThat(afterPut - before)
                .as("each put publishes exactly once → seq +1")
                .isEqualTo(1L);
        assertThat(afterEvict - afterPut)
                .as("each evict publishes exactly once → seq +1")
                .isEqualTo(1L);
    }

    // ---------- helpers ----------

    private RAtomicLong canonicalSeq() {
        return redisson.getAtomicLong(CacheKeys.seqKey(name));
    }

    private double missesCounter() {
        Counter c = meterRegistry.find("cache.reconciliation.misses.detected")
                .tag("cache", name).counter();
        return c == null ? 0.0 : c.count();
    }

    private double regressionsCounter() {
        Counter c = meterRegistry.find("cache.reconciliation.seq.regressions")
                .tag("cache", name).counter();
        return c == null ? 0.0 : c.count();
    }

    private double recheckResolvedCounter() {
        Counter c = meterRegistry.find("cache.reconciliation.seq.regression_recheck_resolved")
                .tag("cache", name).counter();
        return c == null ? 0.0 : c.count();
    }

    private double cyclesCounter() {
        Counter c = meterRegistry.find("cache.reconciliation.cycles.completed")
                .tag("cache", name).counter();
        return c == null ? 0.0 : c.count();
    }
}
