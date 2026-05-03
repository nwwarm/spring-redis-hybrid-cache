package io.github.nwwarm;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.redisson.api.RKeys;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * O(1) clear via the generation counter. Verifies:
 *
 * <ul>
 *   <li>clear() returns reads to null without iterating Redis (old keys orphan
 *       and decay via TTL — observable via direct Redis inspection).</li>
 *   <li>A remote node's reads return null within the generation refresh
 *       interval (or sooner via the OP_CLEAR pub/sub message).</li>
 * </ul>
 */
class GenerationClearIT extends RedisTestBase {

    private RedissonClient redissonA;
    private RedissonClient redissonB;
    private final String name = "gen-clear-" + System.nanoTime();

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
    void clearMakesReadsReturnNullAndOrphansOldKeysInRedis() {
        NearCache cache = newNearCache(name, redissonA, defaultBreaker(), "node-A");
        try {
            cache.put("k1", "v1");
            cache.put("k2", "v2");
            assertThat(cache.get("k1", String.class)).isEqualTo("v1");
            assertThat(cache.get("k2", String.class)).isEqualTo("v2");

            // Snapshot the keyspace before clear — there should be data-bearing keys.
            RKeys keys = redissonA.getKeys();
            long beforeKeys = countKeysFor(keys, name);
            assertThat(beforeKeys).isGreaterThanOrEqualTo(2);

            cache.clear();

            // Reads return null at the new generation. The lookup is O(1) — a
            // single bucket get against the new prefix. Old keys remain in Redis.
            assertThat(cache.get("k1")).isNull();
            assertThat(cache.get("k2")).isNull();

            long afterKeys = countKeysFor(keys, name);
            assertThat(afterKeys)
                    .as("old generation keys must orphan rather than be deleted")
                    .isGreaterThanOrEqualTo(beforeKeys);
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void remoteNodeSeesClearViaPubSubMessage() {
        NearCache a = newNearCache(name, redissonA, defaultBreaker(), "node-A");
        NearCache b = newNearCache(name, redissonB, defaultBreaker(), "node-B");
        try {
            a.put("k", "v1");
            // Pull v1 into B's L1 via L2.
            Awaitility.await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                    assertThat(b.get("k", String.class)).isEqualTo("v1"));
            assertThat(nativeOf(b).getIfPresent("k")).isEqualTo("v1");

            a.clear();

            // The OP_CLEAR pub/sub message clears B's L1 and refreshes its
            // generation pointer; B's read at the new generation finds nothing.
            Awaitility.await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
                assertThat(nativeOf(b).getIfPresent("k")).isNull();
                assertThat(b.get("k")).isNull();
            });
        } finally {
            a.shutdown();
            b.shutdown();
        }
    }

    private static long countKeysFor(RKeys keys, String cacheName) {
        // Hash-tagged value and lock keys ({<cache>:<key>}:...) do not start
        // with the cache name; the generation counter (<cache>:generation)
        // does. Match anything that contains "<cache>:" — covers all three
        // shapes the library produces.
        long count = 0;
        for (String key : keys.getKeysByPattern("*" + cacheName + ":*")) {
            count++;
        }
        return count;
    }
}
