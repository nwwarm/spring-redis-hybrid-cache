package io.github.nwwarm.hybridcache.core;

import io.github.nwwarm.hybridcache.config.CacheConfig;
import io.github.nwwarm.hybridcache.config.CacheProperties;
import io.github.nwwarm.hybridcache.invalidation.InvalidationDispatcher;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.Container;

import java.io.File;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies the 0.3.0 hash-tag key layout produces the routing properties
 * the design relies on:
 *
 * <ul>
 *   <li>Value and lock for the same logical {@code (cache, key)} land on the
 *       same Cluster slot. This is what enables a future single-shard
 *       atomic evict-and-bump.</li>
 *   <li>Distinct keys in the same cache distribute across slots — i.e. the
 *       hash tag scopes per-key, not per-cache. Catches the catastrophic
 *       "everything routes to one shard" regression.</li>
 *   <li>A cache name containing a hash-tag delimiter is rejected at
 *       construction time, not silently mis-routed.</li>
 * </ul>
 *
 * <p>Separate from {@link ClusterCacheIT}, which tests behavioral
 * cluster semantics (invalidation propagation, MOVED redirect handling,
 * reshard recovery). This IT is concerned only with key-level routing.
 */
class ClusterSlotCollocationIT {

    private static final List<String> NODE_ADDRESSES = List.of(
            "redis://127.0.0.1:7001",
            "redis://127.0.0.1:7002",
            "redis://127.0.0.1:7003",
            "redis://127.0.0.1:7004",
            "redis://127.0.0.1:7005",
            "redis://127.0.0.1:7006");

    private static ComposeContainer COMPOSE;

    @BeforeAll
    static void startCluster() {
        COMPOSE = new ComposeContainer(
                new File("src/test/resources/cluster/docker-compose.yml"));
        COMPOSE.start();
        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(500))
                .until(ClusterSlotCollocationIT::clusterIsOkOnAllNodes);
    }

    @AfterAll
    static void stopCluster() {
        if (COMPOSE != null) COMPOSE.stop();
    }

    private static boolean clusterIsOkOnAllNodes() {
        for (int port = 7001; port <= 7006; port++) {
            try {
                Container.ExecResult res = COMPOSE.getContainerByServiceName("redis1")
                        .orElseThrow()
                        .execInContainer("redis-cli", "-p", String.valueOf(port),
                                "cluster", "info");
                String stdout = res.getStdout();
                if (!(stdout.contains("cluster_state:ok")
                        && stdout.contains("cluster_slots_assigned:16384")
                        && stdout.contains("cluster_slots_ok:16384")
                        && stdout.contains("cluster_size:3"))) {
                    return false;
                }
            } catch (Throwable e) {
                return false;
            }
        }
        return true;
    }

    private static int slotOf(String key) throws Exception {
        Container.ExecResult res = COMPOSE.getContainerByServiceName("redis1")
                .orElseThrow()
                .execInContainer("redis-cli", "-p", "7001", "CLUSTER", "KEYSLOT", key);
        return Integer.parseInt(res.getStdout().trim());
    }

    // ---------- Test 1: value + lock collocate on one slot ----------

    @Test
    void valueAndLock_forSameLogicalKey_landOnSameSlot() throws Exception {
        String value = CacheKeys.valueKey("products", "42", 0);
        String lock = CacheKeys.lockKey("products", "42");

        int valueSlot = slotOf(value);
        int lockSlot = slotOf(lock);

        assertThat(valueSlot)
                .as("value (%s) and lock (%s) must collocate", value, lock)
                .isEqualTo(lockSlot);
    }

    @Test
    void valueAtDifferentGenerations_landOnSameSlot() throws Exception {
        // After a clear() the generation bumps; the new value key for the
        // same logical entry must still hash to the same slot, so a future
        // atomic evict+bump can be a single-slot pipeline.
        int slotG0 = slotOf(CacheKeys.valueKey("products", "42", 0));
        int slotG7 = slotOf(CacheKeys.valueKey("products", "42", 7));
        int slotG999 = slotOf(CacheKeys.valueKey("products", "42", 999));

        assertThat(slotG0).isEqualTo(slotG7).isEqualTo(slotG999);
    }

    // ---------- Test 2: distinct keys spread across slots ----------

    @Test
    void distinctKeysInOneCache_distributeAcrossSlots() throws Exception {
        // 100 keys must produce many distinct slots. The threshold of 50
        // is loose enough to be non-flaky (CRC16 distribution + 16384 slots
        // gives well over 50 in expectation) but tight enough to catch the
        // "we accidentally tagged everything to one slot" regression.
        Set<Integer> slots = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            String key = CacheKeys.valueKey("products", "k-" + i, 0);
            slots.add(slotOf(key));
        }
        assertThat(slots)
                .as("100 distinct keys should hash to many distinct slots")
                .hasSizeGreaterThanOrEqualTo(50);
    }

    // ---------- Test 3: brace in cache name fails at startup ----------

    @Test
    void cacheNameContainingBrace_failsValidationAtConstruction() {
        // The validation lives in CacheKeys.validateCacheName, which is
        // called from HybridCacheManager.getMissingCache for any name
        // requested at runtime, and from the manager constructor for any
        // statically-configured name. A brace-shifted hash tag would route
        // every key in the cache to one shard — the "tenant{a}" example
        // from the design discussion. Catch it at startup, not in
        // production.
        RedissonClient client = buildProductionClient();
        try {
            CacheProperties props = new CacheProperties(
                    new CacheProperties.Server(
                            CacheProperties.Mode.CLUSTER, null, NODE_ADDRESSES,
                            null, null, null),
                    java.util.Map.of(),
                    null,
                    "cluster-it",
                    List.of("io.github.nwwarm."),
                    null, null, null, false, null, null, null, null, null);
            HybridCacheManager manager = new HybridCacheManager(
                    props, client,
                    io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry.ofDefaults(),
                    new InvalidationDispatcher(client, "cluster-it", new io.micrometer.core.instrument.simple.SimpleMeterRegistry()),
                    new io.micrometer.core.instrument.simple.SimpleMeterRegistry(),
                    new CodecResolver(null),
                    new KeyLogFormatter(false, "test"));

            assertThatThrownBy(() -> manager.getMissingCache("tenant{a}"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("tenant{a}")
                    .hasMessageContaining("hash tag");
        } finally {
            client.shutdown();
        }
    }

    private static RedissonClient buildProductionClient() {
        CacheProperties.Server server = new CacheProperties.Server(
                CacheProperties.Mode.CLUSTER, null, NODE_ADDRESSES,
                null, null, null);
        CacheProperties props = new CacheProperties(
                server, null, null, "cluster-it",
                List.of("io.github.nwwarm."), null, null, null, false, null, null, null, null, null);
        return new CacheConfig().redissonClient(props);
    }
}
