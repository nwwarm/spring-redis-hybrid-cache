package io.github.nwwarm.hybridcache.config;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validator coverage for refresh-ahead (E37–E39).
 *
 * <p>RA depends on SWR — the validator is one of two enforcement points
 * for that contract (the other is the runtime construction-site assertion
 * in {@code HybridCacheManager.buildRefreshAheadCoordinator}). Tests here
 * cover the boot-time enforcement; the runtime assertion is exercised by
 * the integration suite.
 */
class CacheSpecValidatorRefreshAheadTest {

    private static final CacheProperties.Swr DEFAULT_SWR =
            new CacheProperties.Swr(Duration.ofMinutes(5), Duration.ofMinutes(30));

    @Test
    void ra_validNearCacheWithSwr_doesNotThrow() {
        CacheProperties.CacheSpec spec = raSpec(
                CacheProperties.Tier.NEAR_CACHE,
                DEFAULT_SWR,
                new CacheProperties.RefreshAhead(true, 1.0));
        assertThatNoException().isThrownBy(() ->
                CacheSpecValidator.validate(propsWithCache("users", spec)));
    }

    @Test
    void ra_validLocalOnlyWithSwr_doesNotThrow() {
        CacheProperties.CacheSpec spec = raSpec(
                CacheProperties.Tier.LOCAL_ONLY,
                DEFAULT_SWR,
                new CacheProperties.RefreshAhead(true, 1.0));
        assertThatNoException().isThrownBy(() ->
                CacheSpecValidator.validate(propsWithCache("local", spec)));
    }

    @Test
    void ra_disabled_doesNotValidateOtherFields() {
        // Mirrors the startup-probe validation philosophy: a disabled RA
        // block with bogus values must not gate boot. Operators who
        // toggled RA off shouldn't need to also fix the (irrelevant)
        // beta value to deploy.
        CacheProperties.CacheSpec spec = raSpec(
                CacheProperties.Tier.NEAR_CACHE,
                DEFAULT_SWR,
                new CacheProperties.RefreshAhead(false, 0.0));  // β=0 OK when disabled
        assertThatNoException().isThrownBy(() ->
                CacheSpecValidator.validate(propsWithCache("users", spec)));
    }

    @Test
    void e37_raOnDistributedOnly_isRejected() {
        CacheProperties.CacheSpec spec = raSpec(
                CacheProperties.Tier.DISTRIBUTED_ONLY,
                null,  // SWR is also rejected on DISTRIBUTED_ONLY; null here
                new CacheProperties.RefreshAhead(true, 1.0));
        assertThatThrownBy(() ->
                CacheSpecValidator.validate(propsWithCache("sessions", spec)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("refresh-ahead.enabled=true is not allowed")
                .hasMessageContaining("DISTRIBUTED_ONLY");
    }

    @Test
    void e38_raWithoutSwr_isRejected() {
        // The headline "RA does nothing without SWR" failure mode the
        // guardrail explicitly calls out. Must surface at boot, not as
        // a silent runtime "RA configured but never fires."
        CacheProperties.CacheSpec spec = raSpec(
                CacheProperties.Tier.NEAR_CACHE,
                null,  // no SWR
                new CacheProperties.RefreshAhead(true, 1.0));
        assertThatThrownBy(() ->
                CacheSpecValidator.validate(propsWithCache("users", spec)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("refresh-ahead.enabled=true requires")
                .hasMessageContaining("swr.fresh-for");
    }

    @Test
    void e39_betaZero_isRejected() {
        // β=0 is misconfiguration, not a disable switch. The validator
        // catches it at boot per the guardrail.
        CacheProperties.CacheSpec spec = raSpec(
                CacheProperties.Tier.NEAR_CACHE,
                DEFAULT_SWR,
                new CacheProperties.RefreshAhead(true, 0.0));
        assertThatThrownBy(() ->
                CacheSpecValidator.validate(propsWithCache("users", spec)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("refresh-ahead.beta must be > 0");
    }

    @Test
    void e39_betaNegative_isRejected() {
        CacheProperties.CacheSpec spec = raSpec(
                CacheProperties.Tier.NEAR_CACHE,
                DEFAULT_SWR,
                new CacheProperties.RefreshAhead(true, -0.5));
        assertThatThrownBy(() ->
                CacheSpecValidator.validate(propsWithCache("users", spec)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("refresh-ahead.beta must be > 0");
    }

    @Test
    void multipleViolations_aggregatedIntoOneException() {
        // β=0 AND no SWR — both should appear in the failure message so
        // the operator fixes them in one boot cycle. Matches the
        // validator's "collect all violations" philosophy.
        CacheProperties.CacheSpec spec = raSpec(
                CacheProperties.Tier.NEAR_CACHE,
                null,  // no SWR
                new CacheProperties.RefreshAhead(true, 0.0));
        assertThatThrownBy(() ->
                CacheSpecValidator.validate(propsWithCache("users", spec)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("refresh-ahead.enabled=true requires")
                .hasMessageContaining("refresh-ahead.beta must be > 0");
    }

    // ---------- helpers ----------

    private static CacheProperties.CacheSpec raSpec(
            CacheProperties.Tier tier,
            CacheProperties.Swr swr,
            CacheProperties.RefreshAhead ra) {
        return new CacheProperties.CacheSpec(
                tier, Duration.ofHours(1), 10_000,
                Duration.ofSeconds(5), Duration.ofSeconds(30),
                CacheProperties.Codec.JSON,
                null, null, null, 0.0, null, null, null, swr, ra);
    }

    private static CacheProperties propsWithCache(
            String name, CacheProperties.CacheSpec spec) {
        return new CacheProperties(
                new CacheProperties.Server(
                        CacheProperties.Mode.SINGLE, "redis://localhost:6379",
                        null, null, null, null),
                Map.of(name, spec),
                defaultSpec(),
                null,
                List.of("io.github.nwwarm."),
                null, null, null, false, null, null, null, null, null);
    }

    private static CacheProperties.CacheSpec defaultSpec() {
        return new CacheProperties.CacheSpec(
                CacheProperties.Tier.NEAR_CACHE, Duration.ofHours(1), 10_000,
                Duration.ofSeconds(5), Duration.ofSeconds(30),
                CacheProperties.Codec.JSON, null, null, null, 0.0, null, null,
                null, null, null);
    }
}
