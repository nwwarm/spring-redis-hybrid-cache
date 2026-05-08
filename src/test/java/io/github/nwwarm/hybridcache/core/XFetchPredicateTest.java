package io.github.nwwarm.hybridcache.core;

import org.junit.jupiter.api.Test;

import java.util.SplittableRandom;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link XFetchPredicate}.
 *
 * <p>The predicate is a pure function; tests pass a seeded
 * {@link SplittableRandom} so stochastic assertions are deterministic.
 *
 * <h2>Theoretical fire rate</h2>
 *
 * <p>For a single read with {@code timeSinceWrite=t}, {@code freshFor=F},
 * {@code β·loaderEwma=W}, the predicate
 *
 * <pre>{@code
 * t + W · (−ln U) ≥ F
 * }</pre>
 *
 * <p>is equivalent (algebraically) to
 *
 * <pre>{@code
 * U ≤ exp(−(F − t) / W)
 * }</pre>
 *
 * <p>so {@code P(fire) = exp(−(F − t) / W)}. That closed form is what the
 * curve assertion below compares against.
 */
class XFetchPredicateTest {

    private static final long FRESH_FOR_NS = 100_000_000L;  // 100 ms
    private static final double DEFAULT_BETA = 1.0;
    private static final long FIXED_SEED = 0xC0FFEE_BEEFL;

    /**
     * At {@code t = freshFor / 2} with default {@code β = 1.0} and
     * {@code loaderEwma = freshFor / 2}, theoretical
     * {@code P(fire) = exp(−1) ≈ 0.3679}. Run 10 000 samples and assert
     * the empirical rate lies within ±0.02 of the theoretical (the seeded
     * generator pegs the deviation to a fixed value, but a tolerance
     * keeps the test honest if the generator's distribution drifts).
     */
    @Test
    void atHalfFreshFor_defaultBeta_matchesXFetchCurve() {
        long timeSinceWrite = FRESH_FOR_NS / 2;
        long loaderEwma = FRESH_FOR_NS / 2;
        double theoretical = Math.exp(-1.0);  // ≈ 0.3679

        SplittableRandom rng = new SplittableRandom(FIXED_SEED);
        int n = 10_000;
        int fires = 0;
        for (int i = 0; i < n; i++) {
            if (XFetchPredicate.shouldRefresh(
                    timeSinceWrite, FRESH_FOR_NS, loaderEwma, DEFAULT_BETA, rng)) {
                fires++;
            }
        }
        double empirical = (double) fires / (double) n;
        assertThat(empirical)
                .as("XFetch fire rate at t=freshFor/2, β=1, ewma=freshFor/2;"
                        + " theoretical=exp(-1)=%.4f, empirical=%.4f", theoretical, empirical)
                .isCloseTo(theoretical, org.assertj.core.data.Offset.offset(0.02));
    }

    /**
     * Curve check at multiple points along the fresh window. Confirms the
     * predicate's empirical rate tracks the theoretical curve, not just
     * the single point at TTL/2.
     */
    @Test
    void curveTracksTheoreticalAcrossFreshWindow() {
        long ewma = FRESH_FOR_NS / 4;  // small enough that exp(-(F-t)/W) varies sharply
        SplittableRandom rng = new SplittableRandom(FIXED_SEED);
        int n = 5_000;

        // Sample at 5 points: t = 0.2 * F, 0.4 * F, 0.6 * F, 0.8 * F, 0.95 * F
        double[] fractions = {0.2, 0.4, 0.6, 0.8, 0.95};
        for (double f : fractions) {
            long t = (long) (FRESH_FOR_NS * f);
            double theoretical = Math.exp(-((double) (FRESH_FOR_NS - t)) / (double) ewma);
            int fires = 0;
            for (int i = 0; i < n; i++) {
                if (XFetchPredicate.shouldRefresh(t, FRESH_FOR_NS, ewma, DEFAULT_BETA, rng)) {
                    fires++;
                }
            }
            double empirical = (double) fires / (double) n;
            // Tolerance widens a bit at the curve extremes where small
            // sample fluctuations show up larger in absolute terms; 0.03
            // covers all five points reliably for the seeded generator.
            assertThat(empirical)
                    .as("t=%.2f * F, theoretical=%.4f, empirical=%.4f", f, theoretical, empirical)
                    .isCloseTo(theoretical, org.assertj.core.data.Offset.offset(0.03));
        }
    }

    @Test
    void zeroEwma_neverFires() {
        SplittableRandom rng = new SplittableRandom(FIXED_SEED);
        for (int i = 0; i < 1_000; i++) {
            assertThat(XFetchPredicate.shouldRefresh(
                    FRESH_FOR_NS / 2, FRESH_FOR_NS, 0L, DEFAULT_BETA, rng)).isFalse();
        }
    }

    @Test
    void negativeEwma_neverFires() {
        SplittableRandom rng = new SplittableRandom(FIXED_SEED);
        assertThat(XFetchPredicate.shouldRefresh(
                FRESH_FOR_NS / 2, FRESH_FOR_NS, -1L, DEFAULT_BETA, rng)).isFalse();
    }

    @Test
    void zeroBeta_neverFires() {
        SplittableRandom rng = new SplittableRandom(FIXED_SEED);
        // β=0 makes the speculative-refresh window zero. Even at t=freshFor−1
        // the predicate cannot fire.
        for (int i = 0; i < 1_000; i++) {
            assertThat(XFetchPredicate.shouldRefresh(
                    FRESH_FOR_NS - 1, FRESH_FOR_NS, FRESH_FOR_NS, 0.0, rng)).isFalse();
        }
    }

    @Test
    void timeSinceWriteAtFreshForBoundary_alwaysFires() {
        // When t = freshFor, t + (anything ≥ 0) ≥ freshFor trivially.
        // Only -ln(U) = 0 (U = 1) would NOT fire; the generator never
        // returns 1.0 (its contract is [0, 1)), so this is monotonically
        // true.
        SplittableRandom rng = new SplittableRandom(FIXED_SEED);
        int fires = 0;
        for (int i = 0; i < 1_000; i++) {
            if (XFetchPredicate.shouldRefresh(
                    FRESH_FOR_NS, FRESH_FOR_NS, FRESH_FOR_NS, DEFAULT_BETA, rng)) {
                fires++;
            }
        }
        assertThat(fires).isEqualTo(1_000);
    }

    @Test
    void higherBeta_firesMoreOften() {
        // Same t, F, ewma; varying β. β=2 should fire roughly exp(-0.5)
        // ≈ 0.606 at t=freshFor/2, ewma=freshFor/2 vs exp(-1) ≈ 0.368 for
        // β=1. Strictly: a higher β means a larger speculative-refresh
        // window, which means more reads classify as "should refresh."
        long t = FRESH_FOR_NS / 2;
        long ewma = FRESH_FOR_NS / 2;
        int n = 5_000;

        SplittableRandom rng1 = new SplittableRandom(FIXED_SEED);
        int firesBeta1 = 0;
        for (int i = 0; i < n; i++) {
            if (XFetchPredicate.shouldRefresh(t, FRESH_FOR_NS, ewma, 1.0, rng1)) firesBeta1++;
        }

        SplittableRandom rng2 = new SplittableRandom(FIXED_SEED);
        int firesBeta2 = 0;
        for (int i = 0; i < n; i++) {
            if (XFetchPredicate.shouldRefresh(t, FRESH_FOR_NS, ewma, 2.0, rng2)) firesBeta2++;
        }

        assertThat(firesBeta2).isGreaterThan(firesBeta1);
    }

    @Test
    void zeroFreshFor_neverFires() {
        // freshFor=0 is a precondition violation (validator catches it),
        // but the predicate's defense-in-depth check returns false rather
        // than firing on every read.
        SplittableRandom rng = new SplittableRandom(FIXED_SEED);
        assertThat(XFetchPredicate.shouldRefresh(0L, 0L, 100L, 1.0, rng)).isFalse();
    }

    @Test
    void negativeFreshFor_neverFires() {
        // Defense-in-depth: a negative freshFor is impossible from the
        // validated config but pinning the < 0 boundary keeps mutation
        // tests from replacing `<= 0` with `< 0`.
        SplittableRandom rng = new SplittableRandom(FIXED_SEED);
        assertThat(XFetchPredicate.shouldRefresh(0L, -1L, 100L, 1.0, rng)).isFalse();
    }

    @Test
    void negativeBeta_neverFires() {
        SplittableRandom rng = new SplittableRandom(FIXED_SEED);
        assertThat(XFetchPredicate.shouldRefresh(
                FRESH_FOR_NS / 2, FRESH_FOR_NS, FRESH_FOR_NS, -1e-9, rng)).isFalse();
    }

    @Test
    void negativeTimeSinceWrite_clampedToZero() {
        // Clock skew between sample and write timestamp can produce a
        // negative value; the predicate clamps to 0 rather than letting
        // the sign flow into the comparison. With t clamped to 0 and
        // freshFor=ewma=F, the fire rate is exp(-1) ≈ 0.368 — same as
        // t=0 explicitly.
        SplittableRandom rng = new SplittableRandom(FIXED_SEED);
        int fires = 0;
        int n = 5_000;
        for (int i = 0; i < n; i++) {
            if (XFetchPredicate.shouldRefresh(
                    -1_000_000L, FRESH_FOR_NS, FRESH_FOR_NS, 1.0, rng)) {
                fires++;
            }
        }
        // exp(-1) ≈ 0.368 — clamping to 0 leaves the predicate's behavior
        // identical to a t=0 evaluation. Tolerance ±0.03.
        assertThat((double) fires / n)
                .isCloseTo(Math.exp(-1.0), org.assertj.core.data.Offset.offset(0.03));
    }
}
