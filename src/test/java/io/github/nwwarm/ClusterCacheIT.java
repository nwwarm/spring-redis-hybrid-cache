package io.github.nwwarm;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.redisson.api.RKeys;
import org.redisson.api.RedissonClient;
import org.springframework.cache.caffeine.CaffeineCache;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.Container;

import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Topology test for {@link CacheProperties.Mode#CLUSTER}. Boots a six-node
 * cluster (3 primaries + 3 replicas) via Docker Compose, then exercises the
 * production {@link CacheConfig#redissonClient} path against it.
 *
 * <p>The cluster is started once per test class because cluster bootstrap is
 * the dominant cost (~10s). Within the class, tests use unique cache names so
 * state does not bleed between them.
 */
class ClusterCacheIT {

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
                new File("src/test/resources/cluster/docker-compose.yml"))
                .withLocalCompose(true);
        COMPOSE.start();
        // Poll until every node agrees the cluster is OK and reports 3 masters.
        // Waiting on a single node's `cluster_state:ok` is racy: the bootstrap
        // can finish on node 1 milliseconds before nodes 4/5/6 have processed
        // the topology updates, and the first Redisson SET on a slot owned by
        // a not-yet-converged node returns null silently.
        try {
            Awaitility.await()
                    .atMost(Duration.ofSeconds(60))
                    .pollInterval(Duration.ofMillis(500))
                    .until(ClusterCacheIT::clusterIsOkOnAllNodes);
        } catch (RuntimeException e) {
            int sample = Math.min(8, BOOT_DIAG.size());
            String tail = sample == 0
                    ? "(no diagnostics captured)"
                    : BOOT_DIAG.subList(BOOT_DIAG.size() - sample, BOOT_DIAG.size()).toString();
            throw new RuntimeException(
                    "cluster bootstrap timeout. last " + sample + " diagnostics: " + tail, e);
        }
    }

    private static boolean clusterIsOkOnAllNodes() {
        for (int port = 7001; port <= 7006; port++) {
            if (!nodeReportsOk(port)) return false;
        }
        return true;
    }

    private static boolean nodeReportsOk(int port) {
        try {
            Container.ExecResult res = COMPOSE.getContainerByServiceName("redis1")
                    .orElseThrow()
                    .execInContainer("redis-cli", "-p", String.valueOf(port), "cluster", "info");
            String stdout = res.getStdout();
            boolean ok = stdout.contains("cluster_state:ok")
                    && stdout.contains("cluster_slots_assigned:16384")
                    && stdout.contains("cluster_slots_ok:16384")
                    && stdout.contains("cluster_size:3");
            if (!ok) {
                BOOT_DIAG.add("port " + port + " not converged: " + abbrev(stdout));
            }
            return ok;
        } catch (Throwable e) {
            BOOT_DIAG.add("port " + port + " exception: "
                    + e.getClass().getSimpleName() + ": " + e.getMessage());
            return false;
        }
    }

    @AfterAll
    static void stopCluster() {
        if (COMPOSE != null) COMPOSE.stop();
    }

    private static String abbrev(String s) {
        if (s == null) return "<null>";
        s = s.replace("\n", " | ");
        return s.length() > 200 ? s.substring(0, 200) + "..." : s;
    }

    private static final java.util.List<String> BOOT_DIAG =
            java.util.Collections.synchronizedList(new java.util.ArrayList<>());

    private static RedissonClient buildProductionClient() {
        CacheProperties.Server server = new CacheProperties.Server(
                CacheProperties.Mode.CLUSTER,
                null,
                NODE_ADDRESSES,
                null,
                null,
                null);
        CacheProperties props = new CacheProperties(
                server, null, null, "cluster-it",
                List.of("io.github.nwwarm."), null, null, null, false, null);
        return new CacheConfig().redissonClient(props);
    }

    private static NearCache newNearCache(String name, RedissonClient client, String nodeId) {
        CacheProperties.CacheSpec spec = new CacheProperties.CacheSpec(
                CacheProperties.Tier.NEAR_CACHE,
                Duration.ofMinutes(10), 10_000,
                Duration.ofSeconds(2), Duration.ofSeconds(10),
                CacheProperties.Codec.JSON, null, null, null, 0.0, null);
        var caffeineNative = Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofMinutes(10))
                .maximumSize(spec.maximumSize())
                .recordStats()
                .build();
        CaffeineCache springCache = new CaffeineCache(name, caffeineNative, true);
        InvalidationDispatcher dispatcher = new InvalidationDispatcher(client, nodeId);
        CircuitBreaker breaker = CircuitBreakerRegistry.ofDefaults()
                .circuitBreaker("cluster-test-" + System.nanoTime());
        return new NearCache(springCache, spec, null, client, breaker, dispatcher,
                new SimpleMeterRegistry(), new KeyLogFormatter(false, "test"));
    }

    @Test
    void productionWiredClient_startsAgainstCluster() {
        RedissonClient client = buildProductionClient();
        try {
            client.getBucket("cluster-it:ping").set("pong");
            assertThat(client.<String>getBucket("cluster-it:ping").get())
                    .isEqualTo("pong");
        } finally {
            client.shutdown();
        }
    }

    @Test
    void cacheableSemantics_putAndGet() {
        RedissonClient client = buildProductionClient();
        String name = "cluster-cacheable-" + UUID.randomUUID().toString().substring(0, 8);
        NearCache cache = newNearCache(name, client, "node-A");
        try {
            cache.put("k", "v1");
            assertThat(nativeOf(cache).getIfPresent("k")).isEqualTo("v1");
            assertThat(cache.get("k", String.class)).isEqualTo("v1");
        } finally {
            cache.shutdown();
            client.shutdown();
        }
    }

    @Test
    void evict_invalidatesBothLayers() {
        RedissonClient client = buildProductionClient();
        String name = "cluster-evict-" + UUID.randomUUID().toString().substring(0, 8);
        NearCache cache = newNearCache(name, client, "node-A");
        try {
            cache.put("k", "v");
            cache.evict("k");
            assertThat(nativeOf(cache).getIfPresent("k")).isNull();
            assertThat(cache.get("k")).isNull();
        } finally {
            cache.shutdown();
            client.shutdown();
        }
    }

    @Test
    void crossNodeInvalidation_overCluster() {
        RedissonClient clientA = buildProductionClient();
        RedissonClient clientB = buildProductionClient();
        String name = "cluster-xnode-" + UUID.randomUUID().toString().substring(0, 8);
        NearCache a = newNearCache(name, clientA, "node-A");
        NearCache b = newNearCache(name, clientB, "node-B");
        try {
            a.put("k", "v1");
            Awaitility.await().atMost(Duration.ofSeconds(3)).untilAsserted(() ->
                    assertThat(b.get("k", String.class)).isEqualTo("v1"));

            a.put("k", "v2");
            Awaitility.await().atMost(Duration.ofSeconds(3)).untilAsserted(() ->
                    assertThat(nativeOf(b).getIfPresent("k")).isNull());
            assertThat(b.get("k", String.class)).isEqualTo("v2");
        } finally {
            a.shutdown();
            b.shutdown();
            clientA.shutdown();
            clientB.shutdown();
        }
    }

    /**
     * Forces a slot reshard mid-flight and verifies that subsequent cache
     * operations still succeed. Redisson is responsible for following MOVED
     * redirects to the new owning shard.
     */
    @Test
    void slotMigration_redissonFollowsMovedRedirects() throws Exception {
        RedissonClient client = buildProductionClient();
        String name = "cluster-reshard-" + UUID.randomUUID().toString().substring(0, 8);
        NearCache cache = newNearCache(name, client, "node-A");
        try {
            // Seed a few values across the cluster.
            for (int i = 0; i < 50; i++) {
                cache.put("k" + i, "v" + i);
            }

            // Resolve the source and destination master node IDs from any
            // cluster member, then issue an offline reshard via redis-cli.
            String nodes = COMPOSE.getContainerByServiceName("redis1")
                    .orElseThrow()
                    .execInContainer("redis-cli", "-p", "7001", "cluster", "nodes")
                    .getStdout();
            String fromId = firstMasterId(nodes, ":7001@");
            String toId = firstMasterId(nodes, ":7002@");
            assertThat(fromId).as("source master id").isNotBlank();
            assertThat(toId).as("destination master id").isNotBlank();

            Container.ExecResult reshard = COMPOSE.getContainerByServiceName("redis1")
                    .orElseThrow()
                    .execInContainer(
                            "redis-cli", "--cluster", "reshard", "127.0.0.1:7001",
                            "--cluster-from", fromId,
                            "--cluster-to", toId,
                            "--cluster-slots", "100",
                            "--cluster-yes");
            assertThat(reshard.getExitCode())
                    .as("reshard exit code; stderr=%s", reshard.getStderr())
                    .isZero();

            // After reshard, prior keys should still be reachable; new writes
            // should land on the new owners. Redisson follows MOVED transparently.
            for (int i = 0; i < 50; i++) {
                assertThat(cache.get("k" + i, String.class))
                        .as("post-reshard read of k%d", i)
                        .isEqualTo("v" + i);
            }
            for (int i = 50; i < 100; i++) {
                cache.put("k" + i, "v" + i);
            }
            for (int i = 50; i < 100; i++) {
                assertThat(cache.get("k" + i, String.class)).isEqualTo("v" + i);
            }
        } finally {
            cache.shutdown();
            client.shutdown();
        }
    }

    /**
     * {@code clearImmediate()} must SCAN every master and UNLINK locally
     * for each — a single-shard SCAN would silently leave keys behind on
     * the other 2/3 of the keyspace. Seeding 200 keys spreads across
     * slots well enough to land on every master with overwhelming
     * probability ((1 - (2/3)^200) ≈ 1).
     */
    @Test
    void clearImmediate_iteratesAllShards() {
        RedissonClient client = buildProductionClient();
        String name = "cluster-clear-imm-" + UUID.randomUUID().toString().substring(0, 8);
        NearCache cache = newNearCache(name, client, "node-A");
        try {
            for (int i = 0; i < 200; i++) {
                cache.put("k" + i, "v" + i);
            }

            RKeys keys = client.getKeys();
            String pattern = "{" + name + ":*}:v:*";
            long before = 0;
            for (String ignored : keys.getKeysByPattern(pattern)) before++;
            assertThat(before)
                    .as("sanity: 200 keys should materialise across the cluster")
                    .isGreaterThanOrEqualTo(200);

            cache.clearImmediate();

            long after = 0;
            for (String ignored : keys.getKeysByPattern(pattern)) after++;
            assertThat(after)
                    .as("eager UNLINK must reach every shard, not just the one"
                            + " owning the originating connection")
                    .isZero();
        } finally {
            cache.shutdown();
            client.shutdown();
        }
    }

    private static String firstMasterId(String clusterNodesOutput, String addressMarker) {
        for (String line : clusterNodesOutput.split("\n")) {
            if (line.contains(addressMarker) && line.contains("master")) {
                return line.split("\\s+")[0];
            }
        }
        return "";
    }

    @SuppressWarnings("unchecked")
    private static com.github.benmanes.caffeine.cache.Cache<Object, Object> nativeOf(NearCache cache) {
        return (com.github.benmanes.caffeine.cache.Cache<Object, Object>) cache.getNativeCache();
    }
}
