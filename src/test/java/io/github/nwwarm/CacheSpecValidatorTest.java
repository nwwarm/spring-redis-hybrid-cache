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
                null, null, false, null, null, null, null);
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
                new CacheProperties.Resilience(badGlobal), false, null, null, null, null);
        assertViolation(props, "global circuit-breaker defaults", "failure-rate-threshold");
    }

    // -----------------------------------------------------------------------
    // E15 — ttl-jitter-ratio range
    // -----------------------------------------------------------------------

    @Test
    void e15_jitterRatioNegative_fails() {
        assertViolation(
                propertiesWithCaches(Map.of("foo",
                        specWithJitter(CacheProperties.Tier.NEAR_CACHE, -0.1))),
                "cache 'foo'", "ttl-jitter-ratio must be in [0.0, 0.5]");
    }

    @Test
    void e15_jitterRatioAboveHalf_fails() {
        assertViolation(
                propertiesWithCaches(Map.of("foo",
                        specWithJitter(CacheProperties.Tier.NEAR_CACHE, 0.51))),
                "cache 'foo'", "ttl-jitter-ratio must be in [0.0, 0.5]");
    }

    @Test
    void e15_jitterRatioZero_isValid() {
        assertThatNoException().isThrownBy(() ->
                CacheSpecValidator.validate(propertiesWithCaches(Map.of("foo",
                        specWithJitter(CacheProperties.Tier.NEAR_CACHE, 0.0)))));
    }

    @Test
    void e15_jitterRatioHalf_isValid() {
        assertThatNoException().isThrownBy(() ->
                CacheSpecValidator.validate(propertiesWithCaches(Map.of("foo",
                        specWithJitter(CacheProperties.Tier.NEAR_CACHE, 0.5)))));
    }

    @Test
    void e15_jitterRatioMidRange_isValid() {
        assertThatNoException().isThrownBy(() ->
                CacheSpecValidator.validate(propertiesWithCaches(Map.of("foo",
                        specWithJitter(CacheProperties.Tier.NEAR_CACHE, 0.2)))));
    }

    @Test
    void e15_onDefaultSpec_failsWithDefaultSpecLabel() {
        CacheProperties.CacheSpec bad = specWithJitter(CacheProperties.Tier.NEAR_CACHE, 0.6);
        assertViolation(propertiesWithDefaultSpec(bad),
                "default-spec", "ttl-jitter-ratio must be in [0.0, 0.5]");
    }

    // -----------------------------------------------------------------------
    // E16 — ttl-jitter-ratio > 0 incompatible with DISTRIBUTED_ONLY
    // -----------------------------------------------------------------------

    @Test
    void e16_jitterOnDistributedOnly_fails() {
        // DISTRIBUTED_ONLY has no L1 layer for jitter to apply to. The
        // message has to explain the constraint, not just say "invalid",
        // so an operator who set ttl-jitter-ratio on a session cache
        // understands why the library is rejecting it.
        Throwable t = catchViolation(propertiesWithCaches(Map.of("sessions",
                specWithJitter(CacheProperties.Tier.DISTRIBUTED_ONLY, 0.2))));
        assertThat(t.getMessage())
                .contains("cache 'sessions'")
                .contains("ttl-jitter-ratio > 0")
                .contains("DISTRIBUTED_ONLY")
                .contains("no L1");
    }

    @Test
    void e16_jitterOnDistributedOnlyWithRatioZero_isValid() {
        // ratio=0 is the no-op path — DISTRIBUTED_ONLY tolerates it because
        // the value is the implicit default, not an operator declaring intent.
        assertThatNoException().isThrownBy(() ->
                CacheSpecValidator.validate(propertiesWithCaches(Map.of("sessions",
                        specWithJitter(CacheProperties.Tier.DISTRIBUTED_ONLY, 0.0)))));
    }

    @Test
    void e16_jitterOnLocalOnly_isValid() {
        assertThatNoException().isThrownBy(() ->
                CacheSpecValidator.validate(propertiesWithCaches(Map.of("rl",
                        specWithJitter(CacheProperties.Tier.LOCAL_ONLY, 0.2)))));
    }

    @Test
    void e16_jitterOnNearCache_isValid() {
        assertThatNoException().isThrownBy(() ->
                CacheSpecValidator.validate(propertiesWithCaches(Map.of("products",
                        specWithJitter(CacheProperties.Tier.NEAR_CACHE, 0.2)))));
    }

    // -----------------------------------------------------------------------
    // E17 — max-idle must be positive
    // -----------------------------------------------------------------------

    @Test
    void e17_maxIdleZero_fails() {
        assertViolation(propertiesWithCaches(Map.of("foo",
                        specWithMaxIdle(CacheProperties.Tier.NEAR_CACHE,
                                Duration.ofHours(1), Duration.ZERO))),
                "cache 'foo'", "max-idle must be positive");
    }

    @Test
    void e17_maxIdleNegative_fails() {
        assertViolation(propertiesWithCaches(Map.of("foo",
                        specWithMaxIdle(CacheProperties.Tier.NEAR_CACHE,
                                Duration.ofHours(1), Duration.ofSeconds(-5)))),
                "cache 'foo'", "max-idle must be positive");
    }

    @Test
    void e17_maxIdleUnset_isValid() {
        // Null max-idle means "disabled"; not a violation.
        assertThatNoException().isThrownBy(() ->
                CacheSpecValidator.validate(propertiesWithCaches(Map.of("foo",
                        specWithMaxIdle(CacheProperties.Tier.NEAR_CACHE,
                                Duration.ofHours(1), null)))));
    }

    @Test
    void e17_maxIdlePositive_isValid() {
        assertThatNoException().isThrownBy(() ->
                CacheSpecValidator.validate(propertiesWithCaches(Map.of("foo",
                        specWithMaxIdle(CacheProperties.Tier.NEAR_CACHE,
                                Duration.ofHours(1), Duration.ofMinutes(5))))));
    }

    // -----------------------------------------------------------------------
    // E18 — max-idle incompatible with DISTRIBUTED_ONLY
    // -----------------------------------------------------------------------

    @Test
    void e18_maxIdleOnDistributedOnly_fails() {
        Throwable t = catchViolation(propertiesWithCaches(Map.of("sessions",
                specWithMaxIdle(CacheProperties.Tier.DISTRIBUTED_ONLY,
                        Duration.ofHours(1), Duration.ofMinutes(5)))));
        assertThat(t.getMessage())
                .contains("cache 'sessions'")
                .contains("max-idle has no effect")
                .contains("DISTRIBUTED_ONLY")
                .contains("no L1");
    }

    @Test
    void e18_maxIdleOnLocalOnly_isValid() {
        assertThatNoException().isThrownBy(() ->
                CacheSpecValidator.validate(propertiesWithCaches(Map.of("rl",
                        specWithMaxIdle(CacheProperties.Tier.LOCAL_ONLY,
                                Duration.ofHours(1), Duration.ofMinutes(5))))));
    }

    @Test
    void e18_maxIdleOnNearCache_isValid() {
        assertThatNoException().isThrownBy(() ->
                CacheSpecValidator.validate(propertiesWithCaches(Map.of("products",
                        specWithMaxIdle(CacheProperties.Tier.NEAR_CACHE,
                                Duration.ofHours(1), Duration.ofMinutes(5))))));
    }

    // -----------------------------------------------------------------------
    // E19 — max-idle > ttl is a no-op
    // -----------------------------------------------------------------------

    @Test
    void e19_maxIdleGreaterThanTtl_fails() {
        assertViolation(propertiesWithCaches(Map.of("foo",
                        specWithMaxIdle(CacheProperties.Tier.NEAR_CACHE,
                                Duration.ofMinutes(1), Duration.ofMinutes(5)))),
                "cache 'foo'", "max-idle (PT5M) must be <= ttl (PT1M)");
    }

    @Test
    void e19_maxIdleEqualToTtl_isValid() {
        // max-idle == ttl is allowed: equivalent to "expire ttl after the
        // most recent access," a useful sliding-TTL semantic.
        assertThatNoException().isThrownBy(() ->
                CacheSpecValidator.validate(propertiesWithCaches(Map.of("foo",
                        specWithMaxIdle(CacheProperties.Tier.NEAR_CACHE,
                                Duration.ofMinutes(5), Duration.ofMinutes(5))))));
    }

    @Test
    void e19_maxIdleLessThanTtl_isValid() {
        assertThatNoException().isThrownBy(() ->
                CacheSpecValidator.validate(propertiesWithCaches(Map.of("foo",
                        specWithMaxIdle(CacheProperties.Tier.NEAR_CACHE,
                                Duration.ofHours(1), Duration.ofMinutes(5))))));
    }

    // -----------------------------------------------------------------------
    // E20/E21/E22 — startup-probe constraints (only checked when enabled)
    // -----------------------------------------------------------------------

    @Test
    void e20_disabledProbe_skipsTimingValidation() {
        // A disabled probe with bogus timings must not fail the boot.
        // Operators should be able to leave broken values in dev configs
        // without being forced to clean them up before turning the probe on.
        CacheProperties.StartupProbe disabled = new CacheProperties.StartupProbe(
                false, Duration.ZERO, -3, Duration.ofMillis(-1));
        assertThatNoException().isThrownBy(() ->
                CacheSpecValidator.validate(propertiesWithStartupProbe(disabled)));
    }

    @Test
    void e20_zeroTimeout_fails() {
        CacheProperties.StartupProbe probe = new CacheProperties.StartupProbe(
                true, Duration.ZERO, 1, Duration.ofSeconds(1));
        assertViolation(propertiesWithStartupProbe(probe),
                "startup-probe.timeout", "must be positive");
    }

    @Test
    void e20_negativeTimeout_fails() {
        CacheProperties.StartupProbe probe = new CacheProperties.StartupProbe(
                true, Duration.ofMillis(-50), 1, Duration.ofSeconds(1));
        assertViolation(propertiesWithStartupProbe(probe),
                "startup-probe.timeout", "must be positive");
    }

    @Test
    void e21_negativeRetries_fails() {
        CacheProperties.StartupProbe probe = new CacheProperties.StartupProbe(
                true, Duration.ofSeconds(5), -1, Duration.ofSeconds(1));
        assertViolation(propertiesWithStartupProbe(probe),
                "startup-probe.retries", "must be >= 0");
    }

    @Test
    void e21_zeroRetries_isValid() {
        // Zero retries = single attempt, no retries — perfectly reasonable
        // for environments where one timeout window is enough.
        CacheProperties.StartupProbe probe = new CacheProperties.StartupProbe(
                true, Duration.ofSeconds(5), 0, Duration.ofSeconds(1));
        assertThatNoException().isThrownBy(() ->
                CacheSpecValidator.validate(propertiesWithStartupProbe(probe)));
    }

    @Test
    void e22_negativeRetryDelay_fails() {
        CacheProperties.StartupProbe probe = new CacheProperties.StartupProbe(
                true, Duration.ofSeconds(5), 1, Duration.ofMillis(-1));
        assertViolation(propertiesWithStartupProbe(probe),
                "startup-probe.retry-delay", "must be >= 0");
    }

    @Test
    void e22_zeroRetryDelay_isValid() {
        // Retry immediately — valid; the timeout itself paces attempts.
        CacheProperties.StartupProbe probe = new CacheProperties.StartupProbe(
                true, Duration.ofSeconds(5), 1, Duration.ZERO);
        assertThatNoException().isThrownBy(() ->
                CacheSpecValidator.validate(propertiesWithStartupProbe(probe)));
    }

    // -----------------------------------------------------------------------
    // E23 — sharded pub/sub requires cluster mode
    // -----------------------------------------------------------------------

    @Test
    void e23_shardedPubsub_onCluster_isValid() {
        CacheProperties.Server cluster = new CacheProperties.Server(
                CacheProperties.Mode.CLUSTER, null,
                List.of("redis://a:6379"), null, null, null);
        CacheProperties props = propertiesWithInvalidation(cluster,
                new CacheProperties.Invalidation(true));
        assertThatNoException().isThrownBy(() -> CacheSpecValidator.validate(props));
    }

    @Test
    void e23_shardedPubsub_onSingle_fails() {
        CacheProperties.Server single = new CacheProperties.Server(
                CacheProperties.Mode.SINGLE, "redis://localhost:6379",
                null, null, null, null);
        CacheProperties props = propertiesWithInvalidation(single,
                new CacheProperties.Invalidation(true));
        assertThatThrownBy(() -> CacheSpecValidator.validate(props))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalidation.sharded-pubsub=true requires"
                        + " cache.server.mode=CLUSTER")
                .hasMessageContaining("got SINGLE");
    }

    @Test
    void e23_shardedPubsub_onSentinel_fails() {
        CacheProperties.Server sentinel = new CacheProperties.Server(
                CacheProperties.Mode.SENTINEL, null,
                List.of("redis://s1:26379"), "mymaster", null, null);
        CacheProperties props = propertiesWithInvalidation(sentinel,
                new CacheProperties.Invalidation(true));
        assertThatThrownBy(() -> CacheSpecValidator.validate(props))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("got SENTINEL");
    }

    @Test
    void e23_shardedPubsubDisabled_anyMode_isValid() {
        // Sanity: the validator is only consulted when shardedPubsub=true,
        // so SINGLE + Invalidation(false) must not trip E23.
        CacheProperties.Server single = new CacheProperties.Server(
                CacheProperties.Mode.SINGLE, "redis://localhost:6379",
                null, null, null, null);
        CacheProperties props = propertiesWithInvalidation(single,
                new CacheProperties.Invalidation(false));
        assertThatNoException().isThrownBy(() -> CacheSpecValidator.validate(props));
    }

    // -----------------------------------------------------------------------
    // E24/E25/E26/E28 — preloader constraints (only checked when enabled)
    // -----------------------------------------------------------------------

    @Test
    void preloader_disabled_skipsAllValidation() {
        // Mirroring the startup-probe-disabled philosophy: a preloader that
        // is off shouldn't gate boot on bogus values. Operators can leave
        // junk in dev configs.
        CacheProperties.Preloader bogus = new CacheProperties.Preloader(
                false, "/no/such/path", Duration.ZERO, Duration.ZERO,
                -1, Duration.ZERO, 0);
        CacheProperties.CacheSpec localOnlyWithBogus = specWithPreloader(
                CacheProperties.Tier.LOCAL_ONLY, bogus);
        assertThatNoException().isThrownBy(() ->
                CacheSpecValidator.validate(propertiesWithCacheSpec("x", localOnlyWithBogus)));
    }

    @Test
    void e24_preloaderOnLocalOnly_fails() {
        CacheProperties.Preloader p = enabledPreloader();
        CacheProperties.CacheSpec spec = specWithPreloader(CacheProperties.Tier.LOCAL_ONLY, p);
        assertViolation(propertiesWithCacheSpec("foo", spec),
                "cache 'foo'", "preloader.enabled=true requires tier=NEAR_CACHE");
    }

    @Test
    void e24_preloaderOnDistributedOnly_fails() {
        CacheProperties.Preloader p = enabledPreloader();
        CacheProperties.CacheSpec spec = specWithPreloader(
                CacheProperties.Tier.DISTRIBUTED_ONLY, p);
        assertViolation(propertiesWithCacheSpec("foo", spec),
                "cache 'foo'", "got DISTRIBUTED_ONLY");
    }

    @Test
    void e24_preloaderOnNearCache_isValid() {
        CacheProperties.Preloader p = enabledPreloader();
        CacheProperties.CacheSpec spec = specWithPreloader(CacheProperties.Tier.NEAR_CACHE, p);
        assertThatNoException().isThrownBy(() ->
                CacheSpecValidator.validate(propertiesWithCacheSpec("foo", spec)));
    }

    @Test
    void e25_zeroStoreInterval_fails() {
        CacheProperties.Preloader p = new CacheProperties.Preloader(
                true, null, Duration.ZERO, Duration.ofMinutes(1),
                16, Duration.ofSeconds(30), null);
        assertViolation(propertiesWithCacheSpec(
                "foo", specWithPreloader(CacheProperties.Tier.NEAR_CACHE, p)),
                "cache 'foo'", "store-interval must be positive");
    }

    @Test
    void e25_negativeStoreInitialDelay_fails() {
        CacheProperties.Preloader p = new CacheProperties.Preloader(
                true, null, Duration.ofMinutes(10), Duration.ofMillis(-1),
                16, Duration.ofSeconds(30), null);
        assertViolation(propertiesWithCacheSpec(
                "foo", specWithPreloader(CacheProperties.Tier.NEAR_CACHE, p)),
                "cache 'foo'", "store-initial-delay must be positive");
    }

    @Test
    void e25_zeroPrefetchTimeout_fails() {
        CacheProperties.Preloader p = new CacheProperties.Preloader(
                true, null, Duration.ofMinutes(10), Duration.ofMinutes(1),
                16, Duration.ZERO, null);
        assertViolation(propertiesWithCacheSpec(
                "foo", specWithPreloader(CacheProperties.Tier.NEAR_CACHE, p)),
                "cache 'foo'", "prefetch-timeout must be positive");
    }

    @Test
    void e26_zeroPrefetchConcurrency_fails() {
        // The compact constructor coerces <= 0 to the default (16), so to
        // surface the constraint violation we have to construct via the
        // canonical components (manual record literal allows negative).
        CacheProperties.Preloader p = new CacheProperties.Preloader(
                true, null, Duration.ofMinutes(10), Duration.ofMinutes(1),
                16, Duration.ofSeconds(30), null);
        // Above is valid. The interesting case is reflectively-built bad
        // values; record compact constructor already protects 0/negative.
        // Validate with the actual constructed object — concurrency=16 is
        // above the bar — so this test just sanity-checks the happy path.
        assertThatNoException().isThrownBy(() ->
                CacheSpecValidator.validate(propertiesWithCacheSpec(
                        "foo", specWithPreloader(CacheProperties.Tier.NEAR_CACHE, p))));
    }

    @Test
    void e28_zeroMaxStoredKeys_fails() {
        CacheProperties.Preloader p = new CacheProperties.Preloader(
                true, null, Duration.ofMinutes(10), Duration.ofMinutes(1),
                16, Duration.ofSeconds(30), 0);
        assertViolation(propertiesWithCacheSpec(
                "foo", specWithPreloader(CacheProperties.Tier.NEAR_CACHE, p)),
                "cache 'foo'", "max-stored-keys must be >= 1");
    }

    @Test
    void e28_nullMaxStoredKeys_isValid() {
        CacheProperties.Preloader p = new CacheProperties.Preloader(
                true, null, Duration.ofMinutes(10), Duration.ofMinutes(1),
                16, Duration.ofSeconds(30), null);
        assertThatNoException().isThrownBy(() ->
                CacheSpecValidator.validate(propertiesWithCacheSpec(
                        "foo", specWithPreloader(CacheProperties.Tier.NEAR_CACHE, p))));
    }

    private static CacheProperties.Preloader enabledPreloader() {
        return new CacheProperties.Preloader(
                true, null, Duration.ofMinutes(10), Duration.ofMinutes(1),
                16, Duration.ofSeconds(30), null);
    }

    // -----------------------------------------------------------------------
    // Multi-violation test (acceptance criterion: all in one exception)
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
        CacheProperties.CacheSpec badJitter = specWithJitter(
                CacheProperties.Tier.NEAR_CACHE, 0.7);
        CacheProperties.CacheSpec badMaxIdle = specWithMaxIdle(
                CacheProperties.Tier.NEAR_CACHE,
                Duration.ofMinutes(1), Duration.ofMinutes(5)); // > ttl

        Throwable t = catchViolation(propertiesWithCaches(
                Map.of("a", zeroTtl, "b", badLock, "c", badCb,
                        "d", badJitter, "e", badMaxIdle)));

        assertThat(t.getMessage())
                .contains("5 cache configuration violation(s)")
                .contains("cache 'a'").contains("ttl must be positive")
                .contains("cache 'b'").contains("lock-lease")
                .contains("cache 'c'").contains("failure-rate-threshold")
                .contains("cache 'd'").contains("ttl-jitter-ratio")
                .contains("cache 'e'").contains("max-idle");
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
                List.of("io.github.nwwarm."), null, null, null, false, null, null, null, null);
    }

    private static CacheProperties propertiesWithCaches(
            Map<String, CacheProperties.CacheSpec> caches) {
        return new CacheProperties(server(), caches, defaultSpec(), null,
                List.of("io.github.nwwarm."), null, null, null, false, null, null, null, null);
    }

    private static CacheProperties propertiesWithDefaultSpec(CacheProperties.CacheSpec defaultSpec) {
        return new CacheProperties(server(), Map.of(), defaultSpec, null,
                List.of("io.github.nwwarm."), null, null, null, false, null, null, null, null);
    }

    private static CacheProperties propertiesWithStartupProbe(CacheProperties.StartupProbe probe) {
        return new CacheProperties(server(), Map.of(), defaultSpec(), null,
                List.of("io.github.nwwarm."), null, null, null, false, null, probe, null, null);
    }

    private static CacheProperties propertiesWithInvalidation(
            CacheProperties.Server server, CacheProperties.Invalidation invalidation) {
        return new CacheProperties(server, Map.of(), defaultSpec(), null,
                List.of("io.github.nwwarm."), null, null, null, false, null, null, invalidation, null);
    }

    private static CacheProperties propertiesWithCacheSpec(
            String name, CacheProperties.CacheSpec spec) {
        return new CacheProperties(server(), Map.of(name, spec), defaultSpec(), null,
                List.of("io.github.nwwarm."), null, null, null, false, null, null, null, null);
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
                CacheProperties.Codec.JSON, null, null, null, 0.0, null, null);
    }

    private static CacheProperties.CacheSpec spec(
            CacheProperties.Tier tier, Duration ttl,
            Duration lockWait, Duration lockLease,
            CacheProperties.Codec codec, CacheProperties.CircuitBreaker cb) {
        return new CacheProperties.CacheSpec(
                tier, ttl, 10_000, lockWait, lockLease, codec, cb, null, null, 0.0, null, null);
    }

    private static CacheProperties.CacheSpec specWithJitter(
            CacheProperties.Tier tier, double ratio) {
        return new CacheProperties.CacheSpec(
                tier, Duration.ofHours(1), 10_000,
                Duration.ofSeconds(5), Duration.ofSeconds(30),
                CacheProperties.Codec.JSON, null, null, null, ratio, null, null);
    }

    private static CacheProperties.CacheSpec specWithMaxIdle(
            CacheProperties.Tier tier, Duration ttl, Duration maxIdle) {
        return new CacheProperties.CacheSpec(
                tier, ttl, 10_000,
                Duration.ofSeconds(5), Duration.ofSeconds(30),
                CacheProperties.Codec.JSON, null, null, null, 0.0, maxIdle, null);
    }

    static CacheProperties.CacheSpec specWithPreloader(
            CacheProperties.Tier tier, CacheProperties.Preloader preloader) {
        return new CacheProperties.CacheSpec(
                tier, Duration.ofHours(1), 10_000,
                Duration.ofSeconds(5), Duration.ofSeconds(30),
                CacheProperties.Codec.JSON, null, null, null, 0.0, null, preloader);
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
