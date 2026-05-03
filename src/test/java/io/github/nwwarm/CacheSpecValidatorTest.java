package io.github.nwwarm;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * One test per semantic constraint (E1–E12 + W1 + multi-violation + happy path).
 * All tests are pure unit tests — no Spring context, no Redis.
 */
class CacheSpecValidatorTest {

    // -----------------------------------------------------------------------
    // Happy path
    // -----------------------------------------------------------------------

    @Test
    void validConfig_doesNotThrow() {
        assertThatNoException().isThrownBy(() ->
                CacheSpecValidator.validate(minimalValidProperties()));
    }

    @Test
    void validConfig_withExplicitCaches_doesNotThrow() {
        CacheProperties.CacheSpec products = spec(
                CacheProperties.Tier.NEAR_CACHE,
                Duration.ofHours(6),
                Duration.ofSeconds(2),
                Duration.ofSeconds(30),
                CacheProperties.Codec.JSON,
                null);
        CacheProperties.CacheSpec sessions = spec(
                CacheProperties.Tier.DISTRIBUTED_ONLY,
                Duration.ofMinutes(30),
                Duration.ofSeconds(5),
                Duration.ofSeconds(60),
                CacheProperties.Codec.JSON,
                null);
        assertThatNoException().isThrownBy(() ->
                CacheSpecValidator.validate(propertiesWithCaches(
                        Map.of("products", products, "sessions", sessions))));
    }

    // -----------------------------------------------------------------------
    // Single-violation tests — one per constraint
    // -----------------------------------------------------------------------

    @Test
    void e1_ttlZero_failsWithCacheNameAndConstraint() {
        CacheProperties.CacheSpec bad = spec(
                CacheProperties.Tier.NEAR_CACHE,
                Duration.ZERO,
                Duration.ofSeconds(5), Duration.ofSeconds(30),
                CacheProperties.Codec.JSON, null);
        assertViolation(propertiesWithCaches(Map.of("foo", bad)),
                "cache 'foo'", "ttl must be positive");
    }

    @Test
    void e1_ttlNegative_failsWithCacheNameAndConstraint() {
        CacheProperties.CacheSpec bad = spec(
                CacheProperties.Tier.NEAR_CACHE,
                Duration.ofSeconds(-1),
                Duration.ofSeconds(5), Duration.ofSeconds(30),
                CacheProperties.Codec.JSON, null);
        assertViolation(propertiesWithCaches(Map.of("bar", bad)),
                "cache 'bar'", "ttl must be positive");
    }

    @Test
    void e1_defaultSpec_failsWithDefaultSpecLabel() {
        CacheProperties.CacheSpec bad = spec(
                CacheProperties.Tier.NEAR_CACHE,
                Duration.ZERO,
                Duration.ofSeconds(5), Duration.ofSeconds(30),
                CacheProperties.Codec.JSON, null);
        assertViolation(propertiesWithDefaultSpec(bad),
                "default-spec", "ttl must be positive");
    }

    @Test
    void e2_lockLeaseEqualToLockWait_fails() {
        CacheProperties.CacheSpec bad = spec(
                CacheProperties.Tier.NEAR_CACHE,
                Duration.ofHours(1),
                Duration.ofSeconds(30), Duration.ofSeconds(30),  // equal
                CacheProperties.Codec.JSON, null);
        assertViolation(propertiesWithCaches(Map.of("orders", bad)),
                "cache 'orders'", "lock-lease");
    }

    @Test
    void e2_lockLeaseShorterThanLockWait_fails() {
        CacheProperties.CacheSpec bad = spec(
                CacheProperties.Tier.NEAR_CACHE,
                Duration.ofHours(1),
                Duration.ofSeconds(30), Duration.ofSeconds(10),  // lease < wait
                CacheProperties.Codec.JSON, null);
        assertViolation(propertiesWithCaches(Map.of("orders", bad)),
                "cache 'orders'", "lock-lease");
    }

    @Test
    void e2_lockWaitZero_skipsLockLeaseCheck() {
        // lockWait=0 is the documented "no single-flight" mode; lease is irrelevant.
        CacheProperties.CacheSpec ok = spec(
                CacheProperties.Tier.DISTRIBUTED_ONLY,
                Duration.ofHours(1),
                Duration.ZERO, Duration.ofSeconds(1),  // wait=0, lease can be anything
                CacheProperties.Codec.JSON, null);
        assertThatNoException().isThrownBy(() ->
                CacheSpecValidator.validate(propertiesWithCaches(Map.of("idem", ok))));
    }

    @Test
    void e3_kryoOnLocalOnly_fails() {
        CacheProperties.CacheSpec bad = spec(
                CacheProperties.Tier.LOCAL_ONLY,
                Duration.ofHours(1),
                Duration.ofSeconds(5), Duration.ofSeconds(30),
                CacheProperties.Codec.KRYO, null);
        assertViolation(propertiesWithCaches(Map.of("rl", bad)),
                "cache 'rl'", "codec=KRYO has no effect on tier=LOCAL_ONLY");
    }

    @Test
    void e4_kryoWithEmptyRegisteredClasses_fails() {
        CacheProperties.CacheSpec kryo = spec(
                CacheProperties.Tier.NEAR_CACHE,
                Duration.ofHours(1),
                Duration.ofSeconds(5), Duration.ofSeconds(30),
                CacheProperties.Codec.KRYO, null);
        // registered-classes is empty (default)
        assertViolation(propertiesWithCaches(Map.of("tokens", kryo)),
                "cache 'tokens'", "cache.kryo.registered-classes");
    }

    @Test
    void e4_kryoWithRegisteredClasses_doesNotThrow() {
        CacheProperties.CacheSpec kryo = spec(
                CacheProperties.Tier.NEAR_CACHE,
                Duration.ofHours(1),
                Duration.ofSeconds(5), Duration.ofSeconds(30),
                CacheProperties.Codec.KRYO, null);
        CacheProperties props = new CacheProperties(
                server(), Map.of("tokens", kryo), defaultSpec(), null,
                List.of("io.github.nwwarm."),
                new CacheProperties.Kryo(List.of("java.lang.String")),
                null, null, false, null);
        assertThatNoException().isThrownBy(() -> CacheSpecValidator.validate(props));
    }

    @Test
    void e5_failureRateThresholdZero_fails() {
        assertCbViolation(cbWith(0.0f, null, null, null, null, null, null),
                "failure-rate-threshold");
    }

    @Test
    void e5_failureRateThresholdOver100_fails() {
        assertCbViolation(cbWith(101.0f, null, null, null, null, null, null),
                "failure-rate-threshold");
    }

    @Test
    void e5_failureRateThreshold100_isValid() {
        assertCbNoViolation(cbWith(100.0f, null, null, null, null, null, null));
    }

    @Test
    void e6_slowCallRateThresholdZero_fails() {
        assertCbViolation(cbWith(null, 0.0f, null, null, null, null, null),
                "slow-call-rate-threshold");
    }

    @Test
    void e6_slowCallRateThresholdNegative_fails() {
        assertCbViolation(cbWith(null, -5.0f, null, null, null, null, null),
                "slow-call-rate-threshold");
    }

    @Test
    void e7_slidingWindowSizeZero_fails() {
        assertCbViolation(cbWith(null, null, 0, null, null, null, null),
                "sliding-window-size");
    }

    @Test
    void e8_minimumCallsZero_fails() {
        assertCbViolation(cbWith(null, null, null, 0, null, null, null),
                "minimum-number-of-calls");
    }

    @Test
    void e9_minimumCallsGreaterThanWindowSize_fails() {
        assertCbViolation(cbWith(null, null, 10, 20, null, null, null),
                "minimum-number-of-calls");
    }

    @Test
    void e9_onlyCheckedWhenBothPresent_minCallsAloneDoesNotFail() {
        // min-calls=20 with no window-size in the same block is fine;
        // the constraint isn't evaluable without the second field.
        assertCbNoViolation(cbWith(null, null, null, 20, null, null, null));
    }

    @Test
    void e9_violationMessageMentionsInBlockScopeNote() {
        CacheProperties.CircuitBreaker bad = cbWith(null, null, 5, 20, null, null, null);
        CacheProperties.CacheSpec spec = specWithCb(bad);
        Throwable t = catchViolation(propertiesWithCaches(Map.of("x", spec)));
        assertThat(t.getMessage()).contains("same configuration block");
    }

    @Test
    void e10_slowCallDurationZero_fails() {
        assertCbViolation(cbWith(null, null, null, null, Duration.ZERO, null, null),
                "slow-call-duration-threshold");
    }

    @Test
    void e11_waitDurationZero_fails() {
        assertCbViolation(cbWith(null, null, null, null, null, Duration.ZERO, null),
                "wait-duration-in-open-state");
    }

    @Test
    void e12_permittedCallsZero_fails() {
        assertCbViolation(cbWith(null, null, null, null, null, null, 0),
                "permitted-number-of-calls-in-half-open-state");
    }

    @Test
    void e5_onGlobalDefaults_failsWithGlobalLabel() {
        CacheProperties.CircuitBreaker badGlobal =
                cbWith(0.0f, null, null, null, null, null, null);
        CacheProperties props = new CacheProperties(
                server(), Map.of(), defaultSpec(), null,
                List.of("io.github.nwwarm."), null, null,
                new CacheProperties.Resilience(badGlobal), false, null);
        assertViolation(props, "global circuit-breaker defaults", "failure-rate-threshold");
    }

    // -----------------------------------------------------------------------
    // Multi-violation test (acceptance criterion: all three in one exception)
    // -----------------------------------------------------------------------

    @Test
    void multipleViolations_reportedInSingleException() {
        CacheProperties.CacheSpec zeroTtl = spec(
                CacheProperties.Tier.NEAR_CACHE,
                Duration.ZERO,
                Duration.ofSeconds(5), Duration.ofSeconds(30),
                CacheProperties.Codec.JSON, null);
        CacheProperties.CacheSpec badLock = spec(
                CacheProperties.Tier.NEAR_CACHE,
                Duration.ofHours(1),
                Duration.ofSeconds(60), Duration.ofSeconds(5),  // lease < wait
                CacheProperties.Codec.JSON, null);
        CacheProperties.CacheSpec badCb = specWithCb(
                cbWith(0.0f, null, null, null, null, null, null)); // failure-rate=0

        Throwable t = catchViolation(propertiesWithCaches(
                Map.of("a", zeroTtl, "b", badLock, "c", badCb)));

        assertThat(t.getMessage())
                .contains("3 cache configuration violation(s)")
                .contains("cache 'a'").contains("ttl must be positive")
                .contains("cache 'b'").contains("lock-lease")
                .contains("cache 'c'").contains("failure-rate-threshold");
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static void assertViolation(CacheProperties props, String label, String fragment) {
        assertThatThrownBy(() -> CacheSpecValidator.validate(props))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(label)
                .hasMessageContaining(fragment);
    }

    private static Throwable catchViolation(CacheProperties props) {
        try {
            CacheSpecValidator.validate(props);
            throw new AssertionError("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            return e;
        }
    }

    private static void assertCbViolation(CacheProperties.CircuitBreaker cb, String fragment) {
        assertViolation(propertiesWithCaches(Map.of("x", specWithCb(cb))),
                "cache 'x'", fragment);
    }

    private static void assertCbNoViolation(CacheProperties.CircuitBreaker cb) {
        assertThatNoException().isThrownBy(() ->
                CacheSpecValidator.validate(propertiesWithCaches(Map.of("x", specWithCb(cb)))));
    }

    private static CacheProperties minimalValidProperties() {
        return new CacheProperties(server(), Map.of(), defaultSpec(), null,
                List.of("io.github.nwwarm."), null, null, null, false, null);
    }

    private static CacheProperties propertiesWithCaches(
            Map<String, CacheProperties.CacheSpec> caches) {
        return new CacheProperties(server(), caches, defaultSpec(), null,
                List.of("io.github.nwwarm."), null, null, null, false, null);
    }

    private static CacheProperties propertiesWithDefaultSpec(CacheProperties.CacheSpec defaultSpec) {
        return new CacheProperties(server(), Map.of(), defaultSpec, null,
                List.of("io.github.nwwarm."), null, null, null, false, null);
    }

    private static CacheProperties.Server server() {
        return new CacheProperties.Server(
                CacheProperties.Mode.SINGLE, "redis://localhost:6379",
                null, null, null, null);
    }

    private static CacheProperties.CacheSpec defaultSpec() {
        return new CacheProperties.CacheSpec(
                CacheProperties.Tier.NEAR_CACHE, Duration.ofHours(1), 10_000,
                Duration.ofSeconds(5), Duration.ofSeconds(30),
                CacheProperties.Codec.JSON, null, null, null);
    }

    private static CacheProperties.CacheSpec spec(
            CacheProperties.Tier tier, Duration ttl,
            Duration lockWait, Duration lockLease,
            CacheProperties.Codec codec, CacheProperties.CircuitBreaker cb) {
        return new CacheProperties.CacheSpec(
                tier, ttl, 10_000, lockWait, lockLease, codec, cb, null, null);
    }

    private static CacheProperties.CacheSpec specWithCb(CacheProperties.CircuitBreaker cb) {
        return spec(CacheProperties.Tier.NEAR_CACHE, Duration.ofHours(1),
                Duration.ofSeconds(5), Duration.ofSeconds(30),
                CacheProperties.Codec.JSON, cb);
    }

    private static CacheProperties.CircuitBreaker cbWith(
            Float failureRate, Float slowCallRate,
            Integer windowSize, Integer minCalls,
            Duration slowCallDuration, Duration waitInOpen,
            Integer halfOpenPermitted) {
        return new CacheProperties.CircuitBreaker(
                windowSize, minCalls, failureRate, slowCallDuration,
                slowCallRate, waitInOpen, halfOpenPermitted);
    }
}
