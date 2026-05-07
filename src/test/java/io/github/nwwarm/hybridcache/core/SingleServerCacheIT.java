package io.github.nwwarm.hybridcache.core;

import io.github.nwwarm.hybridcache.config.CacheConfig;
import io.github.nwwarm.hybridcache.config.CacheProperties;
import io.github.nwwarm.hybridcache.testfixtures.RedisTestBase;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Topology test for {@link CacheProperties.Mode#SINGLE}. Builds the Redisson
 * client through the production {@link CacheConfig#redissonClient} path (not
 * the test-only helpers) so that the property → topology dispatch is exercised
 * end-to-end. The actual cache behavior assertions (put/get/evict, cross-node
 * invalidation) reuse the existing test helpers because cache semantics are
 * topology-agnostic.
 */
class SingleServerCacheIT extends RedisTestBase {

    private RedissonClient prodWiredClient;
    private RedissonClient secondClient;

    @BeforeEach
    void setUp() {
        prodWiredClient = buildProductionClient();
    }

    @AfterEach
    void tearDown() {
        if (prodWiredClient != null) prodWiredClient.shutdown();
        if (secondClient != null) secondClient.shutdown();
    }

    private RedissonClient buildProductionClient() {
        CacheProperties.Server server = new CacheProperties.Server(
                CacheProperties.Mode.SINGLE,
                "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379),
                null,
                null,
                null,
                null);
        CacheProperties props = new CacheProperties(
                server, null, null, "single-it",
                List.of("io.github.nwwarm."), null, null, null, false, null, null, null, null, null);
        return new CacheConfig().redissonClient(props);
    }

    @Test
    void productionWiredClient_startsAndPings() {
        assertThat(prodWiredClient).isNotNull();
        // Sanity round-trip through Redisson's bucket API to confirm the
        // client is actually connected to the container.
        prodWiredClient.getBucket("single-it:ping").set("pong");
        assertThat(prodWiredClient.<String>getBucket("single-it:ping").get())
                .isEqualTo("pong");
    }

    @Test
    void cacheableSemantics_firstReadPopulates_secondReadHits() {
        String name = "single-cacheable-" + System.nanoTime();
        NearCache cache = newNearCache(name, prodWiredClient, defaultBreaker(), "node-A");
        try {
            assertThat(cache.get("k")).isNull();
            cache.put("k", "v1");
            assertThat(nativeOf(cache).getIfPresent("k")).isEqualTo("v1");
            assertThat(cache.get("k", String.class)).isEqualTo("v1");
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void evict_invalidatesBothLayers() {
        String name = "single-evict-" + System.nanoTime();
        NearCache cache = newNearCache(name, prodWiredClient, defaultBreaker(), "node-A");
        try {
            cache.put("k", "v");
            cache.evict("k");
            assertThat(nativeOf(cache).getIfPresent("k")).isNull();
            assertThat(cache.get("k")).isNull();
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void crossNodeInvalidation_writeOnA_evictsL1OnB() {
        secondClient = buildProductionClient();
        String name = "single-xnode-" + System.nanoTime();
        NearCache a = newNearCache(name, prodWiredClient, defaultBreaker(), "node-A");
        NearCache b = newNearCache(name, secondClient, defaultBreaker(), "node-B");
        try {
            a.put("k", "v1");
            Awaitility.await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                    assertThat(b.get("k", String.class)).isEqualTo("v1"));

            a.put("k", "v2");
            Awaitility.await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                    assertThat(nativeOf(b).getIfPresent("k")).isNull());
            assertThat(b.get("k", String.class)).isEqualTo("v2");
        } finally {
            a.shutdown();
            b.shutdown();
        }
    }

    @Test
    void misconfiguration_singleWithoutAddress_failsAtPropertyConstruction() {
        // Acceptance criterion 2: misconfiguration must fail at startup, not at
        // first cache operation. The compact constructor throws before any
        // Redisson client is even built.
        assertThatThrownBy(() -> new CacheProperties.Server(
                CacheProperties.Mode.SINGLE, null, null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cache.server.address")
                .hasMessageContaining("SINGLE");
    }

    @Test
    void backwardsCompat_modeUnsetDefaultsToSingle() {
        CacheProperties.Server server = new CacheProperties.Server(
                null,
                "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379),
                null, null, null, null);
        assertThat(server.mode()).isEqualTo(CacheProperties.Mode.SINGLE);
        // And the production wiring works against this constructor shape.
        CacheProperties props = new CacheProperties(
                server, null, null, "compat-it",
                List.of("io.github.nwwarm."), null, null, null, false, null, null, null, null, null);
        RedissonClient client = new CacheConfig().redissonClient(props);
        try {
            client.getBucket("compat-it:ping").set("ok");
            assertThat(client.<String>getBucket("compat-it:ping").get()).isEqualTo("ok");
        } finally {
            client.shutdown();
        }
    }
}
