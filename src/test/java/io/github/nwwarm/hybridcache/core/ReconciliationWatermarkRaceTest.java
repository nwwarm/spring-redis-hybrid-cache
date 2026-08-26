package io.github.nwwarm.hybridcache.core;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.nwwarm.hybridcache.config.CacheProperties;
import io.github.nwwarm.hybridcache.invalidation.InvalidationDispatcher;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RedissonClient;
import org.redisson.misc.CompletableFutureWrapper;
import org.springframework.cache.caffeine.CaffeineCache;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.endsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Deterministic coverage for the 1.0.3 watermark-race fix in
 * {@code reconcile()}. Mockito rather than Testcontainers, because the
 * whole point is to place a publisher's watermark bump at an exact
 * instruction boundary inside the cycle — timing that a real Redis and a
 * real publisher thread can only reach probabilistically.
 *
 * <p>Two distinct defects are pinned here, one per fix:
 *
 * <ol>
 *   <li><b>Bracketed read.</b> {@code reconcile()} samples
 *       {@code lastObservedSeq} both before and after the
 *       {@code <cache>:seq} GET. REGRESSION is judged against the
 *       pre-GET sample — the only one that can prove genuine counter
 *       loss, since every value the watermark holds was durable in Redis
 *       before it was written locally. A bump that lands while the GET is
 *       in flight used to be read as a regression; it is now correctly
 *       NO_MISS.</li>
 *   <li><b>MISS-branch monotonicity.</b> The MISS recovery advances the
 *       watermark with {@code accumulateAndGet(redisSeq, Math::max)}
 *       rather than {@code set(redisSeq)}. A bump landing between the
 *       GET and the recovery used to be discarded, walking the watermark
 *       backwards and producing a spurious MISS — and a second, needless
 *       L1 clear — on the following cycle.</li>
 *   <li><b>Bracketed recheck.</b> The recheck GET is bracketed like the
 *       first one. Before this, a resolved regression was re-classified by
 *       comparing the <em>fresh</em> recheck value against a watermark
 *       snapshot taken before the recheck round-trip started — so every
 *       publish that landed during that round-trip counted as missed, and
 *       suppressed regressions came straight back as false misses. This
 *       was the dominant cost of the whole defect: measured on
 *       {@code ReconcilerDetectionIT} at 1.0.2, ~11k false misses in 5s,
 *       each one an L1 clear, entirely invisible because only the
 *       regression counter was ever asserted.</li>
 * </ol>
 *
 * <p><b>On the simulated publisher bump.</b> These tests advance the
 * watermark by calling {@code onMessageObserved(n)}, which is the
 * <em>receiver</em> entry point, standing in for the publisher-side
 * self-bump in {@code publishInvalidationAsync}. The two are
 * interchangeable only because both perform the identical
 * {@code lastObservedSeq.accumulateAndGet(seq, Math::max)}
 * ({@code NearCache:1026} for the receiver, {@code NearCache:957} for the
 * publisher; {@code DistributedOnlyCache:588} and {@code :525}). Driving
 * the real publisher path would require a live Redis round-trip inside
 * the injected hook, which is exactly the non-determinism this class
 * exists to avoid. If the publisher self-bump ever stops being a
 * {@code Math::max} accumulate, this substitution is no longer valid and
 * these tests must be rebuilt against the real path.
 */
class ReconciliationWatermarkRaceTest {

    private static final String CACHE_NAME = "watermark-race";
    private static final int TOLERANCE = 5;

    // ---------- NearCache ----------

    @Test
    void nearCache_missRecovery_concurrentBump_doesNotWalkWatermarkBackward() {
        Fixture f = Fixture.create();
        // Redis is at 200; the node last observed 100 → delta 95 > tolerance 5,
        // so this cycle legitimately declares a MISS.
        when(f.distributedSeq.get()).thenReturn(200L);

        AtomicReference<Runnable> onClear = new AtomicReference<>(() -> { });
        NearCache cache = f.buildNear(onClear);
        try {
            cache.onMessageObserved(100L);

            // A publisher lands its INCR + self-bump to 250 *after* this
            // cycle read redisSeq=200, i.e. between the GET and the MISS
            // recovery. caffeineCache.clear() is the last statement in that
            // branch before the watermark write, so hooking it places the
            // bump at exactly the contested instruction boundary.
            AtomicBoolean fired = new AtomicBoolean(false);
            onClear.set(() -> {
                if (fired.compareAndSet(false, true)) cache.onMessageObserved(250L);
            });

            cache.reconcile();

            assertThat(f.misses())
                    .as("the gap was real; this cycle must still declare the miss")
                    .isEqualTo(1.0);
            assertThat(cache.lastObservedSeq())
                    .as("set(redisSeq) would clobber the concurrent bump back to 200; "
                            + "accumulateAndGet must keep the higher value")
                    .isEqualTo(250L);

            // Second cycle: the publisher's INCR is now visible to the GET.
            // Watermark 250 vs redisSeq 250 → no gap, no second clear.
            when(f.distributedSeq.get()).thenReturn(250L);
            cache.reconcile();

            assertThat(f.misses())
                    .as("a watermark walked backwards would re-fire MISS here; it must not")
                    .isEqualTo(1.0);
            assertThat(f.regressions()).isZero();
            assertThat(cache.lastObservedSeq()).isEqualTo(250L);
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void nearCache_bumpWhileSeqGetInFlight_isNotARegression() {
        Fixture f = Fixture.create();
        AtomicReference<Runnable> onClear = new AtomicReference<>(() -> { });
        NearCache cache = f.buildNear(onClear);
        try {
            cache.onMessageObserved(100L);

            // The GET is served while Redis holds 120. A burst of peer
            // messages then pushes the watermark to 150 before the cycle
            // takes its post-GET sample. Pre-1.0.3 the single (post-GET)
            // sample made this look like redisSeq 120 < observed 150 —
            // a REGRESSION that never happened.
            AtomicBoolean fired = new AtomicBoolean(false);
            when(f.distributedSeq.get()).thenAnswer(invocation -> {
                if (fired.compareAndSet(false, true)) cache.onMessageObserved(150L);
                return 120L;
            });

            cache.reconcile();

            assertThat(f.regressions())
                    .as("redisSeq 120 >= observedBefore 100, so nothing regressed")
                    .isZero();
            assertThat(f.regressionRecheckResolved())
                    .as("the bracketed read settles this without a recheck round-trip")
                    .isZero();
            assertThat(f.misses())
                    .as("we ended the cycle ahead of what the GET saw; there is no gap to repair")
                    .isZero();
            assertThat(cache.lastObservedSeq())
                    .as("NO_MISS touches the watermark not at all")
                    .isEqualTo(150L);
        } finally {
            cache.shutdown();
        }
    }

    /**
     * The deliberate guard for the recheck path. Read this before deleting it.
     *
     * <p>After the 1.0.3 bracketed read, {@code redisSeq < observedBefore} is
     * unreachable-by-construction on a master read: the counter is INCR-only,
     * Redis is single-threaded per key, and every watermark value was durable
     * in Redis before it was written locally. So on the library's own client
     * ({@code CacheConfig.applyTopology} pins {@code ReadMode.MASTER}) this
     * path should never execute, and {@code seq.regression_recheck_resolved}
     * should sit at zero forever.
     *
     * <p>That combination — a branch that healthy deployments never enter and
     * a metric that reads zero whether it works or not — is how code rots
     * silently. Before this test the branch's only positive coverage was
     * incidental, a side effect of the false-regression storm that 1.0.3
     * fixed; removing the storm removed the coverage, and nothing would have
     * reported it. Verified by mutation: flipping the resolve condition to
     * {@code false} left both reconciler ITs green.
     *
     * <p>What it pins is the case the recheck actually exists for: a
     * user-supplied {@code RedissonClient} with {@code ReadMode.SLAVE}, where
     * a replica serves a stale counter and the retry lands on a caught-up
     * one. Nothing else in the suite exercises a dip that recovers.
     */
    @Test
    void nearCache_recheck_dipThenRecover_isReportedAsResolvedRace_notARegression() {
        Fixture f = Fixture.create();
        AtomicReference<Runnable> onClear = new AtomicReference<>(() -> { });
        NearCache cache = f.buildNear(onClear);
        try {
            cache.onMessageObserved(100L);

            // Replica lag: the first GET is served by a replica sitting at 90,
            // below our watermark of 100. The recheck lands on a node that has
            // caught up to 103. Nothing arrives locally in between, so this is
            // purely about the recheck — no watermark movement to confound it.
            when(f.distributedSeq.get())
                    .thenReturn(90L)
                    .thenReturn(103L);

            cache.reconcile();

            assertThat(f.regressionRecheckResolved())
                    .as("a dip that recovers is a race, and the recheck must say so")
                    .isEqualTo(1.0);
            assertThat(f.regressions())
                    .as("nothing was lost; reporting a regression would send an operator "
                            + "hunting for a FLUSHDB that never happened")
                    .isZero();
            assertThat(f.misses())
                    .as("103 is within tolerance 5 of the watermark 100 — no gap")
                    .isZero();
            assertThat(f.l1Clears.get()).isZero();
            assertThat(cache.lastObservedSeq())
                    .as("NO_MISS leaves the watermark alone; the REGRESSION handler "
                            + "would have reset it to the stale 90")
                    .isEqualTo(100L);
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void nearCache_bumpDuringRecheck_resolvedRegressionDoesNotBecomeMiss() {
        Fixture f = Fixture.create();
        AtomicReference<Runnable> onClear = new AtomicReference<>(() -> { });
        NearCache cache = f.buildNear(onClear);
        try {
            cache.onMessageObserved(100L);

            // First GET shows 90 against a watermark of 100 — a transient dip
            // (replica lag under a custom ReadMode.SLAVE client, or the tail of
            // a genuine loss). That is a REGRESSION verdict, so the cycle takes
            // the recheck path. The recheck round-trip is slow enough that a
            // burst of peer messages lands while it is in flight: Redis has
            // moved to 150 and the local watermark to 155.
            when(f.distributedSeq.get())
                    .thenReturn(90L)
                    .thenAnswer(invocation -> {
                        cache.onMessageObserved(155L);
                        return 150L;
                    });

            cache.reconcile();

            assertThat(f.regressionRecheckResolved())
                    .as("sanity: this cycle must actually have gone through the recheck")
                    .isEqualTo(1.0);
            assertThat(f.regressions())
                    .as("the recheck caught up to the pre-read watermark, so the "
                            + "regression was a race and must not be reported")
                    .isZero();
            assertThat(f.misses())
                    .as("nor may it come back as a miss: 150 is behind the watermark of 155, "
                            + "so there is no gap. Comparing 150 against the pre-recheck "
                            + "snapshot of 100 is what produced the false-miss storm")
                    .isZero();
            assertThat(f.l1Clears.get())
                    .as("and therefore no L1 clear")
                    .isZero();
            assertThat(cache.lastObservedSeq())
                    .as("NO_MISS leaves the watermark alone")
                    .isEqualTo(155L);
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void nearCache_recheckResolves_butWatermarkDidNotCatchUp_stillDeclaresMiss() {
        Fixture f = Fixture.create();
        AtomicReference<Runnable> onClear = new AtomicReference<>(() -> { });
        NearCache cache = f.buildNear(onClear);
        try {
            cache.onMessageObserved(100L);

            // Same recheck path, but nothing arrives during the round-trip: the
            // watermark stays at 100 while Redis is at 150. That is a genuine
            // gap of 50 and the miss must still fire. Pins that the bracketed
            // recheck suppresses only the in-flight window, not real detection.
            when(f.distributedSeq.get())
                    .thenReturn(90L)
                    .thenReturn(150L);

            cache.reconcile();

            assertThat(f.regressionRecheckResolved()).isEqualTo(1.0);
            assertThat(f.regressions()).isZero();
            assertThat(f.misses())
                    .as("a real gap behind a resolved regression must still be declared")
                    .isEqualTo(1.0);
            assertThat(f.l1Clears.get()).isEqualTo(1);
            assertThat(cache.lastObservedSeq()).isEqualTo(150L);
        } finally {
            cache.shutdown();
        }
    }

    // ---------- DistributedOnlyCache ----------

    @Test
    void distributedOnlyCache_missRecovery_concurrentBump_doesNotWalkWatermarkBackward() {
        Fixture f = Fixture.create();
        when(f.distributedSeq.get()).thenReturn(200L);

        DistributedOnlyCache cache = f.buildDistributed();
        try {
            cache.onMessageObserved(100L);

            // This tier has no L1 to clear; its MISS recovery forces a
            // generation refresh, so the dispatched getAsync() is the
            // statement immediately before the watermark write. Same
            // contested boundary, different tier.
            AtomicBoolean fired = new AtomicBoolean(false);
            when(f.distributedGeneration.getAsync()).thenAnswer(invocation -> {
                if (fired.compareAndSet(false, true)) cache.onMessageObserved(250L);
                return new CompletableFutureWrapper<>(CompletableFuture.completedFuture(0L));
            });

            cache.reconcile();

            assertThat(f.misses()).isEqualTo(1.0);
            assertThat(cache.lastObservedSeq())
                    .as("concurrent bump must survive the MISS recovery")
                    .isEqualTo(250L);

            when(f.distributedSeq.get()).thenReturn(250L);
            cache.reconcile();

            assertThat(f.misses())
                    .as("no spurious second miss from a backwards watermark")
                    .isEqualTo(1.0);
            assertThat(f.regressions()).isZero();
            assertThat(cache.lastObservedSeq()).isEqualTo(250L);
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void distributedOnlyCache_bumpWhileSeqGetInFlight_isNotARegression() {
        Fixture f = Fixture.create();
        DistributedOnlyCache cache = f.buildDistributed();
        try {
            cache.onMessageObserved(100L);

            AtomicBoolean fired = new AtomicBoolean(false);
            when(f.distributedSeq.get()).thenAnswer(invocation -> {
                if (fired.compareAndSet(false, true)) cache.onMessageObserved(150L);
                return 120L;
            });

            cache.reconcile();

            assertThat(f.regressions()).isZero();
            assertThat(f.regressionRecheckResolved()).isZero();
            assertThat(f.misses()).isZero();
            assertThat(cache.lastObservedSeq()).isEqualTo(150L);
        } finally {
            cache.shutdown();
        }
    }

    /** DistributedOnly twin of the NearCache recheck guard — see that test's javadoc. */
    @Test
    void distributedOnlyCache_recheck_dipThenRecover_isReportedAsResolvedRace_notARegression() {
        Fixture f = Fixture.create();
        DistributedOnlyCache cache = f.buildDistributed();
        try {
            cache.onMessageObserved(100L);

            when(f.distributedSeq.get())
                    .thenReturn(90L)
                    .thenReturn(103L);

            cache.reconcile();

            assertThat(f.regressionRecheckResolved())
                    .as("a dip that recovers is a race, and the recheck must say so")
                    .isEqualTo(1.0);
            assertThat(f.regressions()).isZero();
            assertThat(f.misses()).isZero();
            assertThat(cache.lastObservedSeq())
                    .as("the REGRESSION handler would have reset this to the stale 90")
                    .isEqualTo(100L);
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void distributedOnlyCache_bumpDuringRecheck_resolvedRegressionDoesNotBecomeMiss() {
        Fixture f = Fixture.create();
        DistributedOnlyCache cache = f.buildDistributed();
        try {
            cache.onMessageObserved(100L);

            when(f.distributedSeq.get())
                    .thenReturn(90L)
                    .thenAnswer(invocation -> {
                        cache.onMessageObserved(155L);
                        return 150L;
                    });

            cache.reconcile();

            assertThat(f.regressionRecheckResolved())
                    .as("sanity: this cycle must actually have gone through the recheck")
                    .isEqualTo(1.0);
            assertThat(f.regressions()).isZero();
            assertThat(f.misses())
                    .as("a resolved regression must not re-enter as a miss")
                    .isZero();
            assertThat(cache.lastObservedSeq()).isEqualTo(155L);
        } finally {
            cache.shutdown();
        }
    }

    // ---------- Fixture ----------

    private static final class Fixture {
        final RedissonClient redisson = mock(RedissonClient.class);
        final RAtomicLong distributedGeneration = mock(RAtomicLong.class);
        final RAtomicLong distributedSeq = mock(RAtomicLong.class);
        final InvalidationDispatcher dispatcher = mock(InvalidationDispatcher.class);
        final CircuitBreaker breaker = CircuitBreakerRegistry.ofDefaults()
                .circuitBreaker("watermark-race-" + System.nanoTime());
        final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        /** Counts {@code caffeineCache.clear()} calls — the user-visible cost of a false miss. */
        final AtomicInteger l1Clears = new AtomicInteger();

        static Fixture create() {
            Fixture f = new Fixture();
            when(f.redisson.getAtomicLong(endsWith(":generation")))
                    .thenReturn(f.distributedGeneration);
            when(f.redisson.getAtomicLong(endsWith(":seq")))
                    .thenReturn(f.distributedSeq);
            when(f.distributedGeneration.get()).thenReturn(0L);
            // DistributedOnlyCache's MISS recovery forces a generation refresh.
            // Stubbed by default so a test that reaches that branch measures the
            // miss rather than dying on an unstubbed async call — individual
            // tests override this when they need the call as an ordering hook.
            when(f.distributedGeneration.getAsync())
                    .thenReturn(new CompletableFutureWrapper<>(CompletableFuture.completedFuture(0L)));
            when(f.dispatcher.getNodeId()).thenReturn("test-node");
            return f;
        }

        /**
         * @param onClear hook run at the top of {@code CaffeineCache.clear()},
         *                the last statement of the MISS branch before the
         *                watermark write. Held behind a reference because the
         *                cache under test does not exist yet when the Caffeine
         *                delegate is constructed.
         */
        NearCache buildNear(AtomicReference<Runnable> onClear) {
            com.github.benmanes.caffeine.cache.Cache<Object, Object> native_ =
                    Caffeine.newBuilder().maximumSize(100).recordStats().build();
            CaffeineCache hooked = new CaffeineCache(CACHE_NAME, native_, true) {
                @Override
                public void clear() {
                    l1Clears.incrementAndGet();
                    onClear.get().run();
                    super.clear();
                }
            };
            return new NearCache(
                    hooked, spec(), null,
                    redisson, breaker, dispatcher, meterRegistry,
                    new KeyLogFormatter(false, "test"));
        }

        DistributedOnlyCache buildDistributed() {
            return new DistributedOnlyCache(
                    CACHE_NAME, distributedSpec(), null,
                    redisson, breaker, dispatcher, meterRegistry,
                    new KeyLogFormatter(false, "test"));
        }

        double misses() {
            return count("cache.reconciliation.misses.detected");
        }

        double regressions() {
            return count("cache.reconciliation.seq.regressions");
        }

        double regressionRecheckResolved() {
            return count("cache.reconciliation.seq.regression_recheck_resolved");
        }

        private double count(String name) {
            Counter c = meterRegistry.find(name).tag("cache", CACHE_NAME).counter();
            return c == null ? 0.0 : c.count();
        }
    }

    private static CacheProperties.CacheSpec spec() {
        return specFor(CacheProperties.Tier.NEAR_CACHE);
    }

    private static CacheProperties.CacheSpec distributedSpec() {
        return specFor(CacheProperties.Tier.DISTRIBUTED_ONLY);
    }

    private static CacheProperties.CacheSpec specFor(CacheProperties.Tier tier) {
        return new CacheProperties.CacheSpec(
                tier,
                Duration.ofMinutes(10), 100,
                Duration.ofSeconds(2), Duration.ofSeconds(10),
                CacheProperties.Codec.JSON, null, null, null, 0.0,
                null, null,
                new CacheProperties.Reconciliation(true, Duration.ofMinutes(10), TOLERANCE),
                null, null);
    }
}
