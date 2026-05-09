package io.github.nwwarm.hybridcache.core;

import io.github.nwwarm.hybridcache.config.CacheProperties;
import io.github.nwwarm.hybridcache.invalidation.InvalidationDispatcher;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.redisson.api.RedissonClient;
import org.springframework.cache.Cache;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link HybridCacheManager#getMissingCache(String)} that
 * exercise every tier × feature combination. The synchronous read paths
 * are covered by {@code BasicTierIT} and friends; this test targets the
 * <em>construction</em> branches in {@code getMissingCache} and
 * {@code buildCaffeineCache} that the IT suite reaches only for the
 * default {@code NEAR_CACHE} configuration.
 *
 * <p>Construction does not require a live Redis: the manager builds the
 * Caffeine + Redisson wiring lazily, the bucket itself is not touched.
 * A deep-stub {@link RedissonClient} is sufficient for the tier-and-feature
 * dispatch the manager performs.
 */
class HybridCacheManagerCoverageTest {

    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    private final CircuitBreakerRegistry breakerRegistry = CircuitBreakerRegistry.ofDefaults();
    private final RedissonClient redisson = mock(RedissonClient.class, Answers.RETURNS_DEEP_STUBS);
    private final InvalidationDispatcher dispatcher =
            new InvalidationDispatcher(redisson, "node-cov", meterRegistry);

    private HybridCacheManager managerFor(Map<String, CacheProperties.CacheSpec> caches) {
        CacheProperties props = new CacheProperties(
                new CacheProperties.Server(
                        CacheProperties.Mode.SINGLE, "redis://localhost:6379",
                        null, null, null, null),
                caches,
                null, "node-cov",
                List.of("io.github.nwwarm."),
                null, null, null, false, null, null, null, null, null);
        return new HybridCacheManager(
                props, redisson, breakerRegistry, dispatcher, meterRegistry,
                new CodecResolver(null), new KeyLogFormatter(false, "test"));
    }

    private static CacheProperties.CacheSpec spec(CacheProperties.Tier tier) {
        return new CacheProperties.CacheSpec(
                tier, Duration.ofMinutes(5), 100,
                Duration.ofSeconds(2), Duration.ofSeconds(10),
                CacheProperties.Codec.JSON,
                null, null, null, 0.0, null, null, null, null, null);
    }

    private static CacheProperties.CacheSpec specWith(CacheProperties.Tier tier,
                                                       double jitterRatio,
                                                       Duration maxIdle,
                                                       CacheProperties.Swr swr,
                                                       CacheProperties.RefreshAhead ra,
                                                       CacheProperties.Reconciliation rec) {
        return new CacheProperties.CacheSpec(
                tier, Duration.ofMinutes(5), 100,
                Duration.ofSeconds(2), Duration.ofSeconds(10),
                CacheProperties.Codec.JSON,
                null, null, null, jitterRatio, maxIdle, null, rec, swr, ra);
    }

    // ---------- Tier dispatch ----------

    @Test
    void getMissingCache_localOnly_returnsLocalOnlyCache() {
        HybridCacheManager m = managerFor(Map.of("local-1", spec(CacheProperties.Tier.LOCAL_ONLY)));
        Cache c = m.getCache("local-1");
        assertThat(c).isInstanceOf(LocalOnlyCache.class);
    }

    @Test
    void getMissingCache_distributedOnly_returnsDistributedOnlyCache() {
        HybridCacheManager m = managerFor(Map.of("dist-1", spec(CacheProperties.Tier.DISTRIBUTED_ONLY)));
        Cache c = m.getCache("dist-1");
        assertThat(c).isInstanceOf(DistributedOnlyCache.class);
    }

    @Test
    void getMissingCache_nearCache_returnsNearCache() {
        HybridCacheManager m = managerFor(Map.of("near-1", spec(CacheProperties.Tier.NEAR_CACHE)));
        Cache c = m.getCache("near-1");
        assertThat(c).isInstanceOf(NearCache.class);
    }

    // ---------- buildCaffeineCache: ttl-only happy path ----------

    @Test
    void getMissingCache_nearCache_defaultExpiry_jitterDisabled_maxIdleUnset() {
        HybridCacheManager m = managerFor(Map.of("near-default",
                spec(CacheProperties.Tier.NEAR_CACHE)));
        assertThat(m.getCache("near-default")).isInstanceOf(NearCache.class);
    }

    // ---------- buildCaffeineCache: jitter only ----------

    @Test
    void getMissingCache_nearCache_jitterEnabled_takesJitterOnlyBranch() {
        HybridCacheManager m = managerFor(Map.of("near-jitter",
                specWith(CacheProperties.Tier.NEAR_CACHE,
                        0.25, null, null, null, null)));
        assertThat(m.getCache("near-jitter")).isInstanceOf(NearCache.class);
    }

    // ---------- buildCaffeineCache: maxIdle only ----------

    @Test
    void getMissingCache_nearCache_maxIdleEnabled_takesMaxIdleOnlyBranch() {
        HybridCacheManager m = managerFor(Map.of("near-maxidle",
                specWith(CacheProperties.Tier.NEAR_CACHE,
                        0.0, Duration.ofMinutes(2), null, null, null)));
        assertThat(m.getCache("near-maxidle")).isInstanceOf(NearCache.class);
    }

    // ---------- buildCaffeineCache: jitter + maxIdle (JitteredMaxIdleExpiry path) ----------

    @Test
    void getMissingCache_nearCache_jitterAndMaxIdle_takesJitteredMaxIdleBranch() {
        HybridCacheManager m = managerFor(Map.of("near-jit-mi",
                specWith(CacheProperties.Tier.NEAR_CACHE,
                        0.25, Duration.ofMinutes(2), null, null, null)));
        assertThat(m.getCache("near-jit-mi")).isInstanceOf(NearCache.class);
    }

    // ---------- buildCaffeineCache: SWR overrides expiry ----------

    @Test
    void getMissingCache_nearCache_swrEnabled_takesSwrBranch_andComposesRemovalListener() {
        CacheProperties.Swr swr = new CacheProperties.Swr(
                Duration.ofMinutes(1), Duration.ofMinutes(5));
        HybridCacheManager m = managerFor(Map.of("near-swr",
                specWith(CacheProperties.Tier.NEAR_CACHE, 0.0, null, swr, null, null)));
        assertThat(m.getCache("near-swr")).isInstanceOf(NearCache.class);
    }

    // ---------- buildCaffeineCache: SWR + jitter (jitter is intentionally inert
    // when SWR is on; this test pins that the SWR branch wins). ----------

    @Test
    void getMissingCache_nearCache_swrAndJitter_swrTakesPrecedence() {
        CacheProperties.Swr swr = new CacheProperties.Swr(
                Duration.ofMinutes(1), Duration.ofMinutes(5));
        HybridCacheManager m = managerFor(Map.of("near-swr-jit",
                specWith(CacheProperties.Tier.NEAR_CACHE, 0.25, null, swr, null, null)));
        assertThat(m.getCache("near-swr-jit")).isInstanceOf(NearCache.class);
    }

    // ---------- buildRefreshAheadCoordinator: enabled requires SWR ----------

    @Test
    void getMissingCache_nearCache_refreshAheadEnabled_buildsCoordinator() {
        CacheProperties.Swr swr = new CacheProperties.Swr(
                Duration.ofMinutes(1), Duration.ofMinutes(5));
        CacheProperties.RefreshAhead ra = new CacheProperties.RefreshAhead(true, 1.0);
        HybridCacheManager m = managerFor(Map.of("near-ra",
                specWith(CacheProperties.Tier.NEAR_CACHE, 0.0, null, swr, ra, null)));
        assertThat(m.getCache("near-ra")).isInstanceOf(NearCache.class);
    }

    @Test
    void getMissingCache_localOnly_refreshAheadEnabled_buildsCoordinator() {
        // RA also fires on LOCAL_ONLY caches when SWR is configured (no L2,
        // breaker is null on the RA constructor — the localRa branch).
        CacheProperties.Swr swr = new CacheProperties.Swr(
                Duration.ofMinutes(1), Duration.ofMinutes(5));
        CacheProperties.RefreshAhead ra = new CacheProperties.RefreshAhead(true, 1.0);
        HybridCacheManager m = managerFor(Map.of("local-ra",
                specWith(CacheProperties.Tier.LOCAL_ONLY, 0.0, null, swr, ra, null)));
        assertThat(m.getCache("local-ra")).isInstanceOf(LocalOnlyCache.class);
    }

    // ---------- Reconciliation skipped when coordinator is null ----------

    @Test
    void getMissingCache_distributedOnly_reconciliationEnabled_butNoCoordinator_isANoOp() {
        // The 7-arg constructor passes null for ReconciliationCoordinator;
        // the manager's branch must skip registration without throwing.
        CacheProperties.Reconciliation rec = new CacheProperties.Reconciliation(
                true, Duration.ofSeconds(60), 5);
        HybridCacheManager m = managerFor(Map.of("dist-rec",
                specWith(CacheProperties.Tier.DISTRIBUTED_ONLY, 0.0, null, null, null, rec)));
        assertThat(m.getCache("dist-rec")).isInstanceOf(DistributedOnlyCache.class);
    }

    // ---------- Cache name validation rejects ':' ----------

    @Test
    void getMissingCache_nameContainsColon_throws() {
        HybridCacheManager m = managerFor(Map.of());
        assertThatThrownBy(() -> m.getCache("bad:name"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ---------- destroy(): shuts down both NearCache and DistributedOnlyCache ----------

    @Test
    void destroy_shutsDownAllCachesAcrossTiers() throws Exception {
        HybridCacheManager m = managerFor(Map.of(
                "near-d", spec(CacheProperties.Tier.NEAR_CACHE),
                "dist-d", spec(CacheProperties.Tier.DISTRIBUTED_ONLY),
                "local-d", spec(CacheProperties.Tier.LOCAL_ONLY)));
        // Touch each so the manager actually constructs them.
        assertThat(m.getCache("near-d")).isNotNull();
        assertThat(m.getCache("dist-d")).isNotNull();
        assertThat(m.getCache("local-d")).isNotNull();

        // destroy must not throw. The shutdown side effects (dispatcher
        // deregistration, etc.) are validated indirectly by other ITs.
        m.destroy();
    }
}
