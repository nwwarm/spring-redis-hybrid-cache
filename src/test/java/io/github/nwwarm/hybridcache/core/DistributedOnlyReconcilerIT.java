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
 * Reconciliation race + genuine-deletion coverage for
 * {@link DistributedOnlyCache#reconcile()}. Parallels the NearCache
 * coverage in {@link ReconcilerDetectionIT}; the underlying race
 * mechanism (non-atomic read-pair of Redis seq and local watermark) is
 * identical in both cache classes, and so is the fix.
 */
class DistributedOnlyReconcilerIT extends RedisTestBase {

    private RedissonClient redisson;
    private SimpleMeterRegistry meterRegistry;
    private DistributedOnlyCache cache;
    private final String name = "dist-recon-" + System.nanoTime();

    @BeforeEach
    void setUp() {
        redisson = newRedisson();
        meterRegistry = new SimpleMeterRegistry();
        cache = newReconcilingDistributedCache(name, redisson, defaultBreaker(), "node-D",
                meterRegistry, Duration.ofMinutes(10), 5);
    }

    @AfterEach
    void tearDown() {
        if (cache != null) cache.shutdown();
        if (redisson != null) redisson.shutdown();
    }

    @Test
    void genuineCounterDeletion_logsRegression_andDoesNotIncrementRecheckResolved() {
        // Drive the watermark via the receiver path, then operator-DEL the
        // canonical counter. Recheck still observes the lower value → the
        // existing handler fires, recheck_resolved must NOT increment.
        cache.handleInvalidation("invalidate", "k", 50L);
        assertThat(cache.lastObservedSeq()).isEqualTo(50L);

        canonicalSeq().delete();

        cache.reconcile();

        assertThat(regressionsCounter())
                .as("genuine deletion must still log and reset")
                .isOne();
        assertThat(recheckResolvedCounter())
                .as("genuine deletion is not a race; recheck must not resolve it")
                .isZero();
        assertThat(cache.lastObservedSeq())
                .as("regression resets the watermark to the new redisSeq")
                .isZero();
    }

    @Test
    void concurrentPublishes_withReconcileLoop_doNotProduceFalseRegressions() throws Exception {
        // See the NearCache twin in ReconcilerDetectionIT for the full
        // rationale. Short version: 1.0.2 zeroed the regression counter but
        // turned the suppressed regressions into MISSes (the recheck compared
        // a fresh redisSeq against a stale watermark snapshot) — ~12.8k of
        // them in 5s on this test, invisible because only regressions were
        // asserted. 1.0.3's bracketed read takes both to zero.
        //
        // writers MUST stay <= the configured missTolerance (5, from setUp):
        // the watermark lags the canonical by at most one in-flight bump per
        // writer, and beyond the tolerance that lag is a correct miss, not a
        // false one.
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
                        // Mirror publishClearAsync's INCR + self-bump pair
                        // without paying for a full clear() (SCAN+UNLINK).
                        // The race is in the (redisSeq, watermark)
                        // ordering inside reconcile(); this is the minimal
                        // shape that reproduces it.
                        //
                        // onMessageObserved is the *receiver* entry point
                        // standing in for the *publisher* self-bump in
                        // publishClearAsync. Interchangeable only while both
                        // perform the same accumulateAndGet(seq, Math::max)
                        // (DistributedOnlyCache:588 and :525 respectively);
                        // if the publisher path diverges, this loop stops
                        // reproducing the real interleaving.
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
                    .as("and must not cascade into a false miss: a regression used to reset the"
                            + " watermark below the canonical, which the next cycle read as a gap")
                    .isZero();
        } finally {
            pool.shutdownNow();
        }
    }

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
}
