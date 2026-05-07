package io.github.nwwarm.hybridcache.config;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validator coverage for the reconciliation block (E29 / E30 / E31). Pure
 * unit tests — no Spring, no Redis. Lives in its own file rather than
 * appending to {@link CacheSpecValidatorTest} so the 0.5.0 surface is
 * self-contained and mutation-testing tooling can target it cleanly.
 */
class CacheSpecValidatorReconciliationTest {

    @Test
    void disabled_neverValidatesTimings() {
        // E29-style: disabled reconciliation with bogus interval is harmless.
        CacheProperties.Reconciliation disabled = new CacheProperties.Reconciliation(
                false, Duration.ZERO, -1);
        assertThatNoException().isThrownBy(() ->
                CacheSpecValidator.validate(propertiesWithReconciliation(
                        CacheProperties.Tier.NEAR_CACHE, disabled)));
    }

    @Test
    void e29_zeroInterval_fails() {
        CacheProperties.Reconciliation r = new CacheProperties.Reconciliation(
                true, Duration.ZERO, 5);
        assertThatThrownBy(() ->
                CacheSpecValidator.validate(propertiesWithReconciliation(
                        CacheProperties.Tier.NEAR_CACHE, r)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reconciliation.interval must be positive");
    }

    @Test
    void e29_negativeInterval_fails() {
        CacheProperties.Reconciliation r = new CacheProperties.Reconciliation(
                true, Duration.ofSeconds(-1), 5);
        assertThatThrownBy(() ->
                CacheSpecValidator.validate(propertiesWithReconciliation(
                        CacheProperties.Tier.NEAR_CACHE, r)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reconciliation.interval must be positive");
    }

    @Test
    void e30_negativeMissTolerance_fails() {
        CacheProperties.Reconciliation r = new CacheProperties.Reconciliation(
                true, Duration.ofSeconds(60), -1);
        assertThatThrownBy(() ->
                CacheSpecValidator.validate(propertiesWithReconciliation(
                        CacheProperties.Tier.NEAR_CACHE, r)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reconciliation.miss-tolerance must be >= 0");
    }

    @Test
    void e30_zeroMissTolerance_isValid() {
        // Tolerance=0 is a valid configuration: maximum sensitivity, no
        // false-positive forgiveness. Operators who want this opt in.
        CacheProperties.Reconciliation r = new CacheProperties.Reconciliation(
                true, Duration.ofSeconds(60), 0);
        assertThatNoException().isThrownBy(() ->
                CacheSpecValidator.validate(propertiesWithReconciliation(
                        CacheProperties.Tier.NEAR_CACHE, r)));
    }

    @Test
    void e31_localOnly_fails() {
        CacheProperties.Reconciliation r = new CacheProperties.Reconciliation(
                true, Duration.ofSeconds(60), 5);
        assertThatThrownBy(() ->
                CacheSpecValidator.validate(propertiesWithReconciliation(
                        CacheProperties.Tier.LOCAL_ONLY, r)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reconciliation.enabled=true is not allowed on")
                .hasMessageContaining("LOCAL_ONLY");
    }

    @Test
    void e31_distributedOnly_isValid() {
        // DISTRIBUTED_ONLY participates — the recovery action is forceRefreshDue,
        // which repairs a missed OP_CLEAR.
        CacheProperties.Reconciliation r = new CacheProperties.Reconciliation(
                true, Duration.ofSeconds(60), 5);
        assertThatNoException().isThrownBy(() ->
                CacheSpecValidator.validate(propertiesWithReconciliation(
                        CacheProperties.Tier.DISTRIBUTED_ONLY, r)));
    }

    @Test
    void e31_nearCache_isValid() {
        CacheProperties.Reconciliation r = new CacheProperties.Reconciliation(
                true, Duration.ofSeconds(60), 5);
        assertThatNoException().isThrownBy(() ->
                CacheSpecValidator.validate(propertiesWithReconciliation(
                        CacheProperties.Tier.NEAR_CACHE, r)));
    }

    @Test
    void multipleReconciliationViolations_collectedTogether() {
        // Negative interval AND negative tolerance AND LOCAL_ONLY tier ⇒ 3 violations.
        // Confirms the validator's all-violations-collected pattern works for
        // the new rules too.
        CacheProperties.Reconciliation bad = new CacheProperties.Reconciliation(
                true, Duration.ofSeconds(-1), -2);
        Throwable t = catchViolation(propertiesWithReconciliation(
                CacheProperties.Tier.LOCAL_ONLY, bad));
        assertThat(t.getMessage())
                .contains("3 cache configuration violation(s)")
                .contains("reconciliation.interval must be positive")
                .contains("reconciliation.miss-tolerance must be >= 0")
                .contains("LOCAL_ONLY");
    }

    @Test
    void healthyDefaults_areValid() {
        // The defaults the production YAML produces (60s interval, tolerance=5,
        // NEAR_CACHE tier) must not be a violation. Cheap regression guard
        // against a future change that accidentally tightens the rule.
        CacheProperties.Reconciliation healthy = new CacheProperties.Reconciliation(
                true, Duration.ofSeconds(60), 5);
        assertThatNoException().isThrownBy(() ->
                CacheSpecValidator.validate(propertiesWithReconciliation(
                        CacheProperties.Tier.NEAR_CACHE, healthy)));
    }

    // ---------- helpers ----------

    private static Throwable catchViolation(CacheProperties props) {
        try {
            CacheSpecValidator.validate(props);
            throw new AssertionError("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            return e;
        }
    }

    private static CacheProperties propertiesWithReconciliation(
            CacheProperties.Tier tier, CacheProperties.Reconciliation r) {
        CacheProperties.CacheSpec spec = new CacheProperties.CacheSpec(
                tier, Duration.ofHours(1), 10_000,
                Duration.ofSeconds(5), Duration.ofSeconds(30),
                CacheProperties.Codec.JSON,
                null, null, null, 0.0, null, null, r, null);
        return new CacheProperties(
                new CacheProperties.Server(CacheProperties.Mode.SINGLE,
                        "redis://localhost:6379", null, null, null, null),
                Map.of("foo", spec),
                null,
                null,
                List.of("io.github.nwwarm."),
                null, null, null, false, null, null, null, null, null);
    }
}
