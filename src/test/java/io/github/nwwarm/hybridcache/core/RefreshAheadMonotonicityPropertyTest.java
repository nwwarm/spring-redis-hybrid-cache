package io.github.nwwarm.hybridcache.core;

import org.junit.jupiter.api.Test;

import java.util.SplittableRandom;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property: for a fixed key, refresh probability is non-decreasing as
 * remaining TTL decreases (equivalently, as time-since-write increases).
 *
 * <p>This is a load-bearing property — a non-monotonic predicate would
 * mean RA could refresh a freshly-written entry MORE eagerly than one
 * about to expire, which inverts the whole point of the feature.
 *
 * <p>jqwik is not on the dependency list; we exercise the property via
 * bounded random parameter tuples generated with a seeded
 * {@link SplittableRandom} (same approach
 * {@link SwrPredicatePropertyTest} uses).
 */
class RefreshAheadMonotonicityPropertyTest {

    private static final long FIXED_SEED = 0xDEADBEEFL;

    /**
     * For each of 200 random parameter tuples, assert empirical fire
     * rates are monotonically non-decreasing along time-since-write.
     * "Empirical" because the predicate is stochastic — we sample N=2000
     * times at each evaluation point and compare rate buckets.
     */
    @Test
    void empiricalFireRate_isMonotonicAlongTimeSinceWrite() {
        SplittableRandom paramRng = new SplittableRandom(FIXED_SEED);
        int trials = 200;

        for (int trial = 0; trial < trials; trial++) {
            long freshFor = paramRng.nextLong(50_000_000L, 500_000_000L);  // 50-500ms
            // Pick ewma in a range that produces meaningful but not extreme
            // fire rates across the fresh window.
            long ewma = paramRng.nextLong(freshFor / 10, freshFor);
            // β in [0.5, 2.0] — typical operator-tunable range.
            double beta = 0.5 + paramRng.nextDouble() * 1.5;

            // Sample fire rates at increasing time-since-write fractions.
            double[] fractions = {0.1, 0.3, 0.5, 0.7, 0.9, 0.99};
            double[] rates = new double[fractions.length];
            int n = 2_000;
            for (int i = 0; i < fractions.length; i++) {
                long t = (long) (freshFor * fractions[i]);
                SplittableRandom rng = new SplittableRandom(paramRng.nextLong());
                int fires = 0;
                for (int s = 0; s < n; s++) {
                    if (XFetchPredicate.shouldRefresh(t, freshFor, ewma, beta, rng)) {
                        fires++;
                    }
                }
                rates[i] = (double) fires / (double) n;
            }

            // Strict-ish monotonicity: the fire rate must not decrease as
            // time-since-write increases. Allow a small tolerance (0.01)
            // for sampling jitter — with N=2000 the standard error is
            // around sqrt(p(1-p)/2000) ≤ 0.011, so 0.01 is right at the
            // edge of one-sigma. Two-sigma protection: 0.025 would be
            // safer if a flake surfaces, but for the seeded generator and
            // these N's, 0.015 has held in repeated local runs.
            for (int i = 1; i < rates.length; i++) {
                assertThat(rates[i] + 0.015)
                        .as("trial=%d freshFor=%d ewma=%d β=%.2f"
                                + " rate at t=%.2fF (%.4f) must be >= rate at t=%.2fF (%.4f)",
                                trial, freshFor, ewma, beta,
                                fractions[i], rates[i],
                                fractions[i - 1], rates[i - 1])
                        .isGreaterThanOrEqualTo(rates[i - 1]);
            }
        }
    }

    /**
     * Strong monotonicity for a fixed RNG seed: with the random sample
     * pinned, re-running the predicate with a larger {@code timeSinceWrite}
     * NEVER returns FALSE if the smaller {@code timeSinceWrite} returned
     * TRUE. This is the deterministic flavor of the property — same
     * sample, more time elapsed → predicate fires at least as often.
     */
    @Test
    void strictMonotonicity_atFixedSample() {
        long freshFor = 100_000_000L;
        long ewma = 50_000_000L;
        double beta = 1.0;

        // Try 100 different sample seeds; for each, walk t from 0 to
        // freshFor and verify the firing pattern is "FFFFTTTT" — once
        // the predicate fires, every subsequent (larger) t also fires.
        SplittableRandom seedRng = new SplittableRandom(FIXED_SEED);
        for (int trial = 0; trial < 100; trial++) {
            long sampleSeed = seedRng.nextLong();
            boolean previouslyFired = false;
            for (long t = 0; t <= freshFor; t += freshFor / 100) {
                SplittableRandom rng = new SplittableRandom(sampleSeed);
                boolean fires = XFetchPredicate.shouldRefresh(
                        t, freshFor, ewma, beta, rng);
                if (previouslyFired && !fires) {
                    throw new AssertionError(String.format(
                            "Strict monotonicity violation: trial=%d seed=%d"
                                    + " predicate fired at smaller t and stopped firing"
                                    + " at t=%d", trial, sampleSeed, t));
                }
                if (fires) previouslyFired = true;
            }
        }
    }
}
