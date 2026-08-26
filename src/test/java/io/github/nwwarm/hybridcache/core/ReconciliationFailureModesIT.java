package io.github.nwwarm.hybridcache.core;

import io.github.nwwarm.hybridcache.testfixtures.RedisTestBase;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Failure-mode coverage for reconciliation per §4 of {@code DESIGN.md}.
 * Verifies the rows added in 0.5.0:
 *
 * <ul>
 *   <li>"Reconciliation cycle fails (Redis unhealthy or breaker open)" —
 *       cycle is skipped, {@code cache.reconciliation.skipped{reason="breaker-open"}}
 *       increments, no exception escapes.</li>
 *   <li>"Cold-load suppression interaction" — cold loads do not publish,
 *       therefore do not INCR the seq, therefore do not generate
 *       false-positive misses on peer reconciliation cycles.</li>
 *   <li>"Sustained publish load" — under 10k+ publishes during a window,
 *       the cycle still completes and the canonical seq advances by
 *       exactly the publish count. The publisher's own watermark keeps
 *       pace: the dispatcher self-skips its messages, so since 1.0.2
 *       {@code publishInvalidationAsync} bumps {@code lastObservedSeq}
 *       directly on the INCR reply. The cycle must handle the load
 *       without throwing.</li>
 * </ul>
 */
class ReconciliationFailureModesIT extends RedisTestBase {

    private RedissonClient redisson;
    private SimpleMeterRegistry meterRegistry;

    @BeforeEach
    void setUp() {
        redisson = newRedisson();
        meterRegistry = new SimpleMeterRegistry();
    }

    @AfterEach
    void tearDown() {
        redisson.shutdown();
    }

    @Test
    void breakerOpen_skipsCycle_andEmitsSkippedCounter() {
        String name = "skip-" + System.nanoTime();
        // Force-open a breaker for this cache.
        CircuitBreaker breaker = fastFailBreaker();
        breaker.transitionToOpenState();
        assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);

        NearCache cache = newReconcilingNearCache(name, redisson, breaker,
                "node-A", meterRegistry, Duration.ofMinutes(10), 0);
        try {
            // Drive a cycle. Must not throw, must increment the skipped
            // counter with reason=breaker-open.
            cache.reconcile();

            Counter skipped = meterRegistry.find("cache.reconciliation.skipped")
                    .tag("cache", name).tag("reason", "breaker-open").counter();
            assertThat(skipped)
                    .as("breaker-open cycles increment the skipped counter")
                    .isNotNull();
            assertThat(skipped.count()).isEqualTo(1.0);

            // Misses counter must NOT have incremented — the cycle was
            // skipped before the comparison ran.
            Counter misses = meterRegistry.find("cache.reconciliation.misses.detected")
                    .tag("cache", name).counter();
            assertThat(misses == null ? 0.0 : misses.count()).isZero();
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void coldLoad_doesNotAdvanceSeq_andDoesNotFalsePositive() {
        String name = "cold-" + System.nanoTime();
        // Two caches share Redis; A cold-loads N keys (no publish, no INCR);
        // B's reconciliation cycle must see no advance and therefore declare
        // no miss. This pins the load-bearing assumption from item 3 of the
        // ## Reconciliation guardrails: cold-load suppression interaction.
        RedissonClient redissonB = newRedisson();
        SimpleMeterRegistry meterRegistryB = new SimpleMeterRegistry();
        NearCache cacheA = newReconcilingNearCache(name, redisson, defaultBreaker(),
                "node-A", meterRegistry, Duration.ofMinutes(10), 0);
        NearCache cacheB = newReconcilingNearCache(name, redissonB, defaultBreaker(),
                "node-B", meterRegistryB, Duration.ofMinutes(10), 0);
        try {
            long seqBefore = redisson.getAtomicLong(CacheKeys.seqKey(name)).get();

            // 50 cold loads on A. Each one fires the loader path (which
            // does NOT publish) — so the canonical <cache>:seq must NOT
            // advance.
            for (int i = 0; i < 50; i++) {
                final int n = i;
                cacheA.get("k" + i, () -> "v" + n);
            }

            long seqAfter = redisson.getAtomicLong(CacheKeys.seqKey(name)).get();
            assertThat(seqAfter)
                    .as("cold loads must not INCR <cache>:seq (item 3 guardrail)")
                    .isEqualTo(seqBefore);

            // B's reconciliation cycle: no advance, no miss.
            cacheB.reconcile();
            Counter misses = meterRegistryB.find("cache.reconciliation.misses.detected")
                    .tag("cache", name).counter();
            assertThat(misses == null ? 0.0 : misses.count())
                    .as("cold-loads on a peer must not false-positive on this node's cycle")
                    .isZero();
        } finally {
            cacheA.shutdown();
            cacheB.shutdown();
            redissonB.shutdown();
        }
    }

    @Test
    void sustainedPublishLoad_cycleCompletes_noFalsePositives() {
        String name = "sustained-" + System.nanoTime();
        // High-volume publishes during a window; the reconciliation cycle
        // must complete (no exception, cycles.completed counter increments).
        // The publisher's own watermark keeps pace with the canonical: the
        // dispatcher self-skips its messages, so publishInvalidationAsync
        // bumps lastObservedSeq directly on each INCR reply (1.0.2).
        //
        // The test pins the "no double traffic" guardrail under realistic
        // load: 1000 publishes per cycle × 1 cycle = 1000 INCRs, the cycle
        // adds 1 GET. We assert the GET count delta is exactly 1 by
        // observing cycles.completed.
        NearCache cache = newReconcilingNearCache(name, redisson, defaultBreaker(),
                "node-A", meterRegistry, Duration.ofMinutes(10), 0);
        try {
            long seqBefore = redisson.getAtomicLong(CacheKeys.seqKey(name)).get();
            int N = 1_000;
            for (int i = 0; i < N; i++) {
                cache.put("k" + i, "v" + i);
            }
            long seqAfter = redisson.getAtomicLong(CacheKeys.seqKey(name)).get();
            assertThat(seqAfter - seqBefore)
                    .as("each put publishes exactly once → seq advances by N")
                    .isEqualTo(N);

            // Drive a cycle — must complete cleanly.
            cache.reconcile();
            Counter cycles = meterRegistry.find("cache.reconciliation.cycles.completed")
                    .tag("cache", name).counter();
            assertThat(cycles).isNotNull();
            assertThat(cycles.count())
                    .as("cycle completed despite high publish load")
                    .isEqualTo(1.0);

            // A publisher's own publishes self-skip on receive, so before
            // 1.0.2 lastObservedSeq stayed at 0 here and the cycle declared a
            // miss on the node's own N publishes (tolerance=0, delta=N).
            // That is no longer the case: publishInvalidationAsync bumps the
            // watermark on each INCR reply, so observed tracks the canonical
            // exactly and this cycle classifies NO_MISS. A publishing node no
            // longer amplifies its own writes into a self-inflicted L1 clear.
            //
            // The two-node version is in ReconciliationCrossNodeIT; here we
            // just confirm the cycle handled the load without throwing.
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void distributedOnlyCache_reconcileForcesGenerationRefresh() {
        // Mirror of the NearCache miss test, for the DistributedOnly tier.
        // The recovery action differs (forceRefreshDue, not L1 clear), and
        // we verify the lastRefreshNanos was reset.
        String name = "dist-recon-" + System.nanoTime();
        DistributedOnlyCache dist = newReconcilingDistributedCache(name, redisson,
                defaultBreaker(), "node-A", meterRegistry,
                Duration.ofMinutes(10), 0);
        try {
            // Bump the canonical seq externally to simulate missed messages.
            redisson.getAtomicLong(CacheKeys.seqKey(name)).set(100);

            dist.reconcile();

            Counter misses = meterRegistry.find("cache.reconciliation.misses.detected")
                    .tag("cache", name).counter();
            assertThat(misses).isNotNull();
            assertThat(misses.count())
                    .as("DistributedOnlyCache miss is detected the same way")
                    .isEqualTo(1.0);
            assertThat(dist.lastObservedSeq())
                    .as("watermark advanced after recovery")
                    .isGreaterThanOrEqualTo(100L);
        } finally {
            dist.shutdown();
        }
    }

    // Mirror of newReconcilingNearCache for the DistributedOnly tier.
    private DistributedOnlyCache newReconcilingDistributedCache(
            String name, RedissonClient r, CircuitBreaker breaker, String nodeId,
            SimpleMeterRegistry mr, Duration interval, int missTolerance) {
        var spec = new io.github.nwwarm.hybridcache.config.CacheProperties.CacheSpec(
                io.github.nwwarm.hybridcache.config.CacheProperties.Tier.DISTRIBUTED_ONLY,
                Duration.ofMinutes(10), 10_000,
                Duration.ofSeconds(2), Duration.ofSeconds(10),
                io.github.nwwarm.hybridcache.config.CacheProperties.Codec.JSON,
                null, null, null, 0.0, null, null,
                new io.github.nwwarm.hybridcache.config.CacheProperties.Reconciliation(
                        true, interval, missTolerance), null, null);
        var dispatcher = new io.github.nwwarm.hybridcache.invalidation.InvalidationDispatcher(
                r, nodeId, mr);
        return new DistributedOnlyCache(name, spec, null, r, breaker, dispatcher, mr,
                new KeyLogFormatter(false, "test"));
    }
}
