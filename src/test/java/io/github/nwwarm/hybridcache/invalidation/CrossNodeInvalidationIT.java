package io.github.nwwarm.hybridcache.invalidation;

import io.github.nwwarm.hybridcache.core.NearCache;
import io.github.nwwarm.hybridcache.testfixtures.RedisTestBase;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Two NearCache instances backed by the same Redis. Verifies that a write on
 * node A propagates as an invalidation that drops B's L1 entry, and that B's
 * next read fetches the new value through L2.
 */
class CrossNodeInvalidationIT extends RedisTestBase {

    private RedissonClient redissonA;
    private RedissonClient redissonB;
    private NearCache cacheA;
    private NearCache cacheB;
    private final String name = "xnode-" + System.nanoTime();

    @BeforeEach
    void setUp() {
        redissonA = newRedisson();
        redissonB = newRedisson();
        cacheA = newNearCache(name, redissonA, defaultBreaker(), "node-A");
        cacheB = newNearCache(name, redissonB, defaultBreaker(), "node-B");
    }

    @AfterEach
    void tearDown() {
        cacheA.shutdown();
        cacheB.shutdown();
        redissonA.shutdown();
        redissonB.shutdown();
    }

    @Test
    void writeOnA_invalidatesL1OnB_andBReadsFromL2() {
        // Seed both nodes' L1 with a stale value via L2.
        cacheA.put("k", "v1");
        Awaitility.await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                assertThat(cacheB.get("k", String.class)).isEqualTo("v1"));
        // B now has v1 in its L1.
        assertThat(nativeOf(cacheB).getIfPresent("k")).isEqualTo("v1");

        // A writes a new value. This publishes an invalidation to B.
        cacheA.put("k", "v2");

        // Wait for B's L1 to be evicted by the pub/sub message.
        Awaitility.await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                assertThat(nativeOf(cacheB).getIfPresent("k")).isNull());

        // B's read goes through L2 and yields the new value, then repopulates L1.
        assertThat(cacheB.get("k", String.class)).isEqualTo("v2");
        assertThat(nativeOf(cacheB).getIfPresent("k")).isEqualTo("v2");
    }

    @Test
    void evictOnA_evictsL1OnB() {
        cacheA.put("k", "v1");
        Awaitility.await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                assertThat(cacheB.get("k", String.class)).isEqualTo("v1"));

        cacheA.evict("k");

        Awaitility.await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            assertThat(nativeOf(cacheB).getIfPresent("k")).isNull();
            assertThat(cacheB.get("k")).isNull();
        });
    }
}
