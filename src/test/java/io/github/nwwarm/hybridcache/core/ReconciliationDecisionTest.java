package io.github.nwwarm.hybridcache.core;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.SplittableRandom;

import static io.github.nwwarm.hybridcache.core.ReconciliationDecision.MISS;
import static io.github.nwwarm.hybridcache.core.ReconciliationDecision.NO_MISS;
import static io.github.nwwarm.hybridcache.core.ReconciliationDecision.REGRESSION;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pure-unit coverage of the reconciliation classification rule. No Redis,
 * no Spring, no scheduler — every test is a one-line {@code classify()} call.
 *
 * <p>Tracks every boundary in the rule: regression boundary
 * ({@code redisSeq < observedSeq}), miss boundary
 * ({@code delta > tolerance} vs {@code delta == tolerance}), and the
 * tolerance=0 edge case (every gap is a miss).
 *
 * <p>The randomized property test at the end is a bounded fuzz that
 * exercises the rule against the property promised in {@code DESIGN.md}:
 * "for any sequence of (publish, drop, reconcile) operations,
 * post-reconciliation L1 state matches L2 within one cycle." The local
 * version of that property here is "every input maps to exactly one
 * outcome" — i.e., the rule is total and disjoint.
 */
class ReconciliationDecisionTest {

    @ParameterizedTest
    @CsvSource({
            // redisSeq, observedSeq, tolerance, expected
            "0,    0,   0, NO_MISS",        // empty cycle
            "0,    0,   5, NO_MISS",        // both at 0, any tolerance
            "5,    5,   0, NO_MISS",        // equal; delta=0
            "5,    5,   5, NO_MISS",
            "10,   5,   5, NO_MISS",        // delta=5 == tolerance — boundary, NOT a miss
            "11,   5,   5, MISS",           // delta=6 > tolerance — boundary, IS a miss
            "100,  0,   5, MISS",           // big delta
            "1,    0,   0, MISS",           // tolerance=0, any positive delta is a miss
            "100,  100, 0, NO_MISS",
            "4,    5,   5, REGRESSION",     // redisSeq < observed
            "0,    1,   0, REGRESSION",
            "0,    1000,5, REGRESSION",
    })
    void classify_boundaries(long redisSeq, long observedSeq, int tolerance,
                              ReconciliationDecision expected) {
        assertThat(ReconciliationDecision.classify(redisSeq, observedSeq, tolerance))
                .isEqualTo(expected);
    }

    @Test
    void regressionWins_evenWhenDeltaWouldBeMiss() {
        // observedSeq > redisSeq even by a huge margin → REGRESSION, not MISS.
        assertThat(ReconciliationDecision.classify(0, 1_000_000, 0))
                .isEqualTo(REGRESSION);
    }

    @Test
    void hugePositiveDelta_isStillMiss_notOverflow() {
        // Long.MAX_VALUE − 0 doesn't overflow the rule (we don't add to it).
        assertThat(ReconciliationDecision.classify(Long.MAX_VALUE, 0, 5))
                .isEqualTo(MISS);
    }

    @Test
    void totality_property_classify_alwaysReturnsExactlyOneOutcome() {
        // 10k random triples — every input must classify to one outcome.
        // The classifier is a pure function so this is really an "is it
        // total / well-defined" check, but it covers the full long range
        // including negative observedSeq (which can't happen in practice
        // but the rule must still be defined).
        SplittableRandom rng = new SplittableRandom(0xCAFEBABEL);
        for (int i = 0; i < 10_000; i++) {
            long r = rng.nextLong();
            long o = rng.nextLong();
            int t = rng.nextInt(0, 100);
            ReconciliationDecision decision =
                    ReconciliationDecision.classify(r, o, t);
            // Property: outcome matches the rule we documented.
            ReconciliationDecision expected;
            if (r < o) expected = REGRESSION;
            else if (r - o > t) expected = MISS;
            else expected = NO_MISS;
            assertThat(decision)
                    .as("redisSeq=%d, observedSeq=%d, tolerance=%d", r, o, t)
                    .isEqualTo(expected);
        }
    }
}
