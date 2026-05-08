package io.github.nwwarm.hybridcache.core;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.LongRange;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property test for the reconciliation comparison rule.
 *
 * <p>Invariant (§10 0.5.0 / DESIGN.md / Reconciliation): for any
 * {@code (redisSeq, observedSeq, tolerance)} triple, the
 * {@link ReconciliationDecision#classify} call returns exactly one of
 * {@link ReconciliationDecision#NO_MISS},
 * {@link ReconciliationDecision#MISS}, or
 * {@link ReconciliationDecision#REGRESSION}, with the boundaries
 * pinned by the §10 entry "Reconciliation seq-counter regression
 * handled, not asserted away."
 *
 * <p>Complements the JUnit-style fixtures in
 * {@link ReconciliationDecisionTest} with random-walk coverage that
 * the enumerated cases miss.
 */
class ReconciliationDecisionPropertyTest {

    @Property(tries = 500)
    void classification_is_total_and_disjoint(
            @ForAll @LongRange(min = 0L, max = 1_000_000L) long redisSeq,
            @ForAll @LongRange(min = 0L, max = 1_000_000L) long observedSeq,
            @ForAll @IntRange(min = 0, max = 100) int tolerance) {

        ReconciliationDecision decision =
                ReconciliationDecision.classify(redisSeq, observedSeq, tolerance);

        // Totality: every triple maps to exactly one Kind.
        assertThat(decision).isNotNull();

        // Regression is operator-driven: never reclassify as miss.
        if (redisSeq < observedSeq) {
            assertThat(decision).isEqualTo(ReconciliationDecision.REGRESSION);
        }

        // Miss requires a strict delta past tolerance.
        if (decision == ReconciliationDecision.MISS) {
            assertThat(redisSeq - observedSeq).isGreaterThan(tolerance);
        }

        // No-miss requires the delta to be in [0, tolerance].
        if (decision == ReconciliationDecision.NO_MISS) {
            long delta = redisSeq - observedSeq;
            assertThat(delta).isGreaterThanOrEqualTo(0L);
            assertThat(delta).isLessThanOrEqualTo((long) tolerance);
        }
    }
}
