package io.github.nwwarm.hybridcache.config;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validator coverage for SWR (E32–E36). Mirrors the structure of
 * {@link CacheSpecValidatorTest} — one test per constraint, plus the
 * tier-rule rejection on {@code DISTRIBUTED_ONLY} and the "happy path"
 * + "rejected on LOCAL_ONLY/NEAR_CACHE allowed" pair.
 */
class CacheSpecValidatorSwrTest {

    @Test
    void swr_validNearCache_doesNotThrow() {
        CacheProperties.CacheSpec spec = swrSpec(
                CacheProperties.Tier.NEAR_CACHE,
                Duration.ofHours(1),
                new CacheProperties.Swr(Duration.ofMinutes(5), Duration.ofMinutes(30)));
        assertThatNoException().isThrownBy(() ->
                CacheSpecValidator.validate(propsWithCache("users", spec)));
    }

    @Test
    void swr_validLocalOnly_doesNotThrow() {
        CacheProperties.CacheSpec spec = swrSpec(
                CacheProperties.Tier.LOCAL_ONLY,
                Duration.ofHours(1),
                new CacheProperties.Swr(Duration.ofMinutes(5), Duration.ofMinutes(30)));
        assertThatNoException().isThrownBy(() ->
                CacheSpecValidator.validate(propsWithCache("ratelimits", spec)));
    }

    @Test
    void e32_swrOnDistributedOnly_isRejected() {
        // E32: DISTRIBUTED_ONLY has no L1 to apply dual deadlines to.
        // The validator should fail at startup with a clear tier-rule
        // message rather than letting SWR opt-in silently no-op.
        CacheProperties.CacheSpec spec = swrSpec(
                CacheProperties.Tier.DISTRIBUTED_ONLY,
                Duration.ofHours(1),
                new CacheProperties.Swr(Duration.ofMinutes(5), Duration.ofMinutes(30)));
        assertThatThrownBy(() ->
                CacheSpecValidator.validate(propsWithCache("sessions", spec)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("swr.* is not allowed on tier=DISTRIBUTED_ONLY");
    }

    @Test
    void e33_freshForZero_isRejected() {
        CacheProperties.CacheSpec spec = swrSpec(
                CacheProperties.Tier.NEAR_CACHE,
                Duration.ofHours(1),
                new CacheProperties.Swr(Duration.ZERO, Duration.ofMinutes(30)));
        assertThatThrownBy(() ->
                CacheSpecValidator.validate(propsWithCache("foo", spec)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("swr.fresh-for must be positive");
    }

    @Test
    void e33_freshForNegative_isRejected() {
        CacheProperties.CacheSpec spec = swrSpec(
                CacheProperties.Tier.NEAR_CACHE,
                Duration.ofHours(1),
                new CacheProperties.Swr(Duration.ofMinutes(-1), Duration.ofMinutes(30)));
        assertThatThrownBy(() ->
                CacheSpecValidator.validate(propsWithCache("foo", spec)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("swr.fresh-for must be positive");
    }

    @Test
    void e34_staleForZero_isRejected() {
        CacheProperties.CacheSpec spec = swrSpec(
                CacheProperties.Tier.NEAR_CACHE,
                Duration.ofHours(1),
                new CacheProperties.Swr(Duration.ofMinutes(5), Duration.ZERO));
        assertThatThrownBy(() ->
                CacheSpecValidator.validate(propsWithCache("foo", spec)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("swr.stale-for must be positive");
    }

    @Test
    void e35_freshForEqualsStaleFor_isRejected() {
        // Equality means an empty stale window — SWR would never fire.
        CacheProperties.CacheSpec spec = swrSpec(
                CacheProperties.Tier.NEAR_CACHE,
                Duration.ofHours(1),
                new CacheProperties.Swr(Duration.ofMinutes(10), Duration.ofMinutes(10)));
        assertThatThrownBy(() ->
                CacheSpecValidator.validate(propsWithCache("foo", spec)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("swr.fresh-for")
                .hasMessageContaining("must be < swr.stale-for");
    }

    @Test
    void e35_freshForGreaterThanStaleFor_isRejected() {
        CacheProperties.CacheSpec spec = swrSpec(
                CacheProperties.Tier.NEAR_CACHE,
                Duration.ofHours(1),
                new CacheProperties.Swr(Duration.ofMinutes(30), Duration.ofMinutes(5)));
        assertThatThrownBy(() ->
                CacheSpecValidator.validate(propsWithCache("foo", spec)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be < swr.stale-for");
    }

    @Test
    void e36_staleForEqualsTtl_isAllowed() {
        // The validator rule is `<=`, not `<` — equality is allowed.
        CacheProperties.CacheSpec spec = swrSpec(
                CacheProperties.Tier.NEAR_CACHE,
                Duration.ofMinutes(30),
                new CacheProperties.Swr(Duration.ofMinutes(5), Duration.ofMinutes(30)));
        assertThatNoException().isThrownBy(() ->
                CacheSpecValidator.validate(propsWithCache("foo", spec)));
    }

    @Test
    void e36_staleForGreaterThanTtl_isRejected() {
        // stale-for is the Caffeine expireAfterWrite ceiling on L1; if it
        // exceeds the L2 ttl, the L1 entry can outlive the L2 source.
        CacheProperties.CacheSpec spec = swrSpec(
                CacheProperties.Tier.NEAR_CACHE,
                Duration.ofMinutes(15),
                new CacheProperties.Swr(Duration.ofMinutes(5), Duration.ofMinutes(30)));
        assertThatThrownBy(() ->
                CacheSpecValidator.validate(propsWithCache("foo", spec)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("swr.stale-for")
                .hasMessageContaining("must be <= ttl");
    }

    // ---------- helpers ----------

    private static CacheProperties.CacheSpec swrSpec(
            CacheProperties.Tier tier, Duration ttl, CacheProperties.Swr swr) {
        return new CacheProperties.CacheSpec(
                tier, ttl, 10_000,
                Duration.ofSeconds(5), Duration.ofSeconds(30),
                CacheProperties.Codec.JSON,
                null, null, null, 0.0, null, null, null, swr);
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
                CacheProperties.Codec.JSON, null, null, null, 0.0, null, null, null, null);
    }
}
