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
        assertThat(cache.lastObservedSeq())
                .as("regression resets the watermark to the new redisSeq")
                .isZero();
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

    private double cyclesCounter() {
        Counter c = meterRegistry.find("cache.reconciliation.cycles.completed")
                .tag("cache", name).counter();
        return c == null ? 0.0 : c.count();
    }
}
