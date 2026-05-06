package io.github.nwwarm.hybridcache.core;

import io.github.nwwarm.hybridcache.testfixtures.RedisTestBase;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RKeys;
import org.redisson.api.RedissonClient;
import org.redisson.api.options.KeysScanOptions;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Eager clear via {@link HybridCache#clearImmediate()}. Verifies that:
 *
 * <ul>
 *   <li>After {@code clearImmediate()} returns, no value keys remain in
 *       Redis under the cache's prefix — i.e. the eager UNLINK actually
 *       ran (contrast with the regular {@code clear()} O(1) path, which
 *       leaves orphans for TTL to reclaim).</li>
 *   <li>A peer node's L1 is dropped as a result of the OP_CLEAR
 *       invalidation message published by {@code clearImmediate()},
 *       same as for {@code clear()}.</li>
 * </ul>
 *
 * <p>Cluster-wide multi-shard iteration is exercised separately by the
 * {@code clearImmediate_iteratesAllShards} test in {@link ClusterCacheIT}
 * — Redisson's {@code RKeys.unlinkByPattern} iterates per-master, and a
 * single-node Redis only verifies the happy path on one entry.
 */
class ClearImmediateIT extends RedisTestBase {

    private RedissonClient redissonA;
    private RedissonClient redissonB;
    private final String name = "clear-imm-" + System.nanoTime();

    @BeforeEach
    void setUp() {
        redissonA = newRedisson();
        redissonB = newRedisson();
    }

    @AfterEach
    void tearDown() {
        redissonA.shutdown();
        redissonB.shutdown();
    }

    @Test
    void nearCache_singleNode_unlinksAllValueKeys() {
        NearCache cache = newNearCache(name, redissonA, defaultBreaker(), "node-A");
        try {
            for (int i = 0; i < 100; i++) {
                cache.put("k" + i, "v" + i);
            }
            assertThat(countValueKeys(redissonA, name))
                    .as("sanity: all 100 entries materialised in Redis before clear")
                    .isGreaterThanOrEqualTo(100);

            cache.clearImmediate();

            assertThat(countValueKeys(redissonA, name))
                    .as("clearImmediate must UNLINK every old-generation value key")
                    .isZero();

            // Reads observe the new generation — nothing there.
            for (int i = 0; i < 100; i++) {
                assertThat(cache.get("k" + i))
                        .as("post-clearImmediate read of k%d must miss", i)
                        .isNull();
            }
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void distributedOnly_singleNode_unlinksAllValueKeys() {
        DistributedOnlyCache cache = newDistributedCache(name, redissonA, defaultBreaker());
        try {
            for (int i = 0; i < 100; i++) {
                cache.put("k" + i, "v" + i);
            }
            assertThat(countValueKeys(redissonA, name))
                    .as("sanity: all 100 entries materialised in Redis before clear")
                    .isGreaterThanOrEqualTo(100);

            cache.clearImmediate();

            assertThat(countValueKeys(redissonA, name))
                    .as("clearImmediate must UNLINK every old-generation value key")
                    .isZero();

            for (int i = 0; i < 100; i++) {
                assertThat(cache.get("k" + i)).isNull();
            }
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void nearCache_crossNode_peerDropsL1OnClearImmediate() {
        NearCache a = newNearCache(name, redissonA, defaultBreaker(), "node-A");
        NearCache b = newNearCache(name, redissonB, defaultBreaker(), "node-B");
        try {
            a.put("k", "v1");
            // Warm B's L1 via L2.
            Awaitility.await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                    assertThat(b.get("k", String.class)).isEqualTo("v1"));
            assertThat(nativeOf(b).getIfPresent("k")).isEqualTo("v1");

            a.clearImmediate();

            // OP_CLEAR clears B's L1 and forces its generation pointer to
            // refresh; B's read then lands in the new (empty) partition.
            Awaitility.await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
                assertThat(nativeOf(b).getIfPresent("k")).isNull();
                assertThat(b.get("k")).isNull();
            });
        } finally {
            a.shutdown();
            b.shutdown();
        }
    }

    @Test
    void distributedOnly_crossNode_peerSeesClearImmediateSubSecond() {
        DistributedOnlyCache a = newDistributedCache(name, redissonA, defaultBreaker(), "node-A");
        DistributedOnlyCache b = newDistributedCache(name, redissonB, defaultBreaker(), "node-B");
        try {
            a.put("k", "v1");
            assertThat(b.get("k", String.class)).isEqualTo("v1");

            a.clearImmediate();

            // The OP_CLEAR pub/sub message bumps B's generation pointer
            // before the 1s poll interval would have caught up.
            Awaitility.await()
                    .atMost(Duration.ofMillis(500))
                    .pollInterval(Duration.ofMillis(5))
                    .untilAsserted(() ->
                            assertThat(b.get("k", String.class))
                                    .as("B should observe A's clearImmediate before the 1s poll")
                                    .isNull());
        } finally {
            a.shutdown();
            b.shutdown();
        }
    }

    private static long countValueKeys(RedissonClient client, String cacheName) {
        // Match every value bucket for this cache, regardless of generation:
        //   {<cache>:<key>}:v:<gen>
        RKeys keys = client.getKeys();
        long count = 0;
        for (String ignored : keys.getKeys(KeysScanOptions.defaults().pattern("{" + cacheName + ":*}:v:*"))) {
            count++;
        }
        return count;
    }
}
