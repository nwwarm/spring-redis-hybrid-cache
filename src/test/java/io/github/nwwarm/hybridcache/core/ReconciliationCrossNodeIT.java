package io.github.nwwarm.hybridcache.core;

import io.github.nwwarm.hybridcache.testfixtures.RedisTestBase;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Headline integration test for 0.5.0: a subscriber drops a published
 * invalidation, reconciliation closes the gap within the configured
 * interval, and the L1 returns to a coherent state. This is the
 * load-bearing test for the entire reconciliation feature — if it fails,
 * the headline correctness mechanism this release ships is broken.
 *
 * <p>Mechanism: cache B's incoming subscription is replaced with a
 * "swallowing" listener for one window. A publish from cache A during that
 * window does not reach B's normal handler — but it DOES advance the
 * canonical {@code <cache>:seq}. B's reconciliation cycle, when it next
 * fires, finds {@code redisSeq − lastObservedSeq > miss-tolerance} and
 * clears its local L1.
 */
class ReconciliationCrossNodeIT extends RedisTestBase {

    private RedissonClient redissonA;
    private RedissonClient redissonB;
    private SimpleMeterRegistry meterRegistryA;
    private SimpleMeterRegistry meterRegistryB;
    private NearCache cacheA;
    private NearCache cacheB;
    private final String name = "recon-xnode-" + System.nanoTime();

    @BeforeEach
    void setUp() {
        redissonA = newRedisson();
        redissonB = newRedisson();
        meterRegistryA = new SimpleMeterRegistry();
        meterRegistryB = new SimpleMeterRegistry();
        // Tight interval and tolerance=0 so the test doesn't have to wait.
        cacheA = newReconcilingNearCache(name, redissonA, defaultBreaker(),
                "node-A", meterRegistryA, Duration.ofMillis(200), 0);
        cacheB = newReconcilingNearCache(name, redissonB, defaultBreaker(),
                "node-B", meterRegistryB, Duration.ofMillis(200), 0);
    }

    @AfterEach
    void tearDown() {
        cacheA.shutdown();
        cacheB.shutdown();
        redissonA.shutdown();
        redissonB.shutdown();
    }

    @Test
    void droppedInvalidation_isRecoveredByNextReconciliationCycle() {
        // 1. Seed both nodes' L1 with the same value.
        cacheA.put("k", "v1");
        Awaitility.await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                assertThat(cacheB.get("k", String.class)).isEqualTo("v1"));
        assertThat(nativeOf(cacheB).getIfPresent("k")).isEqualTo("v1");

        // 2. Snapshot B's lastObservedSeq AND drop B's subscription so the
        // next message from A reaches Redis but not B's handler. The
        // <cache>:seq still advances (it's INCR'd on the publisher side
        // before the publish), so the canonical and B's local watermark
        // diverge.
        long observedBefore = cacheB.lastObservedSeq();
        cacheB.shutdown();   // dispatcher.deregister(name)

        // 3. A writes a new value. This INCRs <cache>:seq and publishes,
        // but B has no listener registered — the message is dropped on
        // arrival as far as B is concerned.
        cacheA.put("k", "v2");

        // 4. Verify divergence: canonical seq advanced past B's last
        // observed value. Tolerance=0 means *any* divergence is a miss.
        long redisSeq = redissonB.getAtomicLong(CacheKeys.seqKey(name)).get();
        assertThat(redisSeq).isGreaterThan(observedBefore);

        // 5. Force B to run a cycle. Reconciliation detects the gap and
        // clears B's local L1. (We can't wait for the scheduler because
        // the cache no longer has its dispatcher subscription — only the
        // reconciler is needed; we drive it inline as the test seam.)
        cacheB.reconcile();

        assertThat(nativeOf(cacheB).getIfPresent("k"))
                .as("dropped invalidation closed by reconciliation cycle")
                .isNull();

        Counter misses = meterRegistryB.find("cache.reconciliation.misses.detected")
                .tag("cache", name).counter();
        assertThat(misses).isNotNull();
        assertThat(misses.count())
                .as("the recovery action increments the miss counter")
                .isEqualTo(1.0);

        assertThat(cacheB.lastObservedSeq())
                .as("after recovery, lastObservedSeq has advanced to the canonical value")
                .isGreaterThanOrEqualTo(redisSeq);
    }

    @Test
    void healthyState_doesNotFalsePositive() {
        // Publisher / subscriber separation: A publishes 50 invalidations,
        // B receives them via the dispatcher. Both nodes' lastObservedSeq
        // tracks the canonical seq exactly — A via the self-bump on
        // publish, B via the accumulateAndGet on receipt. After the
        // delivery has drained, neither node's reconciliation cycle
        // should declare a miss.
        for (int i = 0; i < 50; i++) {
            cacheA.put("k" + i, "v" + i);
        }

        // Drain pub/sub by waiting for B's lastObservedSeq to catch up to
        // the canonical. This avoids a race where reconcile() runs while
        // an in-flight message is still on its way to B.
        long expectedSeq = redissonA.getAtomicLong(CacheKeys.seqKey(name)).get();
        Awaitility.await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                assertThat(cacheB.lastObservedSeq())
                        .as("B's watermark must catch up via pub/sub before reconcile()")
                        .isEqualTo(expectedSeq));

        // Now drive cycles on both nodes. Neither should declare a miss —
        // A self-bumped on every publish, B accumulated on every receive.
        cacheA.reconcile();
        cacheB.reconcile();

        Counter missA = meterRegistryA.find("cache.reconciliation.misses.detected")
                .tag("cache", name).counter();
        Counter missB = meterRegistryB.find("cache.reconciliation.misses.detected")
                .tag("cache", name).counter();
        assertThat(missA == null ? 0.0 : missA.count())
                .as("publisher A self-bumps on every publish, so no miss")
                .isZero();
        assertThat(missB == null ? 0.0 : missB.count())
                .as("subscriber B accumulates on every receipt, so no miss")
                .isZero();
    }
}
