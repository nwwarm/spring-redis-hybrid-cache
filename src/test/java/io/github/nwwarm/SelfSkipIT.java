package io.github.nwwarm;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The originating node must skip its own invalidation messages — otherwise
 * every put would evict the L1 entry it just populated and the next read
 * would refetch from L2, defeating the L1.
 */
class SelfSkipIT extends RedisTestBase {

    private RedissonClient redisson;
    private NearCache cache;

    @BeforeEach
    void setUp() {
        redisson = newRedisson();
        cache = newNearCache("self-skip-" + System.nanoTime(),
                redisson, defaultBreaker(), "node-A");
    }

    @AfterEach
    void tearDown() {
        cache.shutdown();
        redisson.shutdown();
    }

    @Test
    void putDoesNotInvalidateOwnL1Entry() throws InterruptedException {
        cache.put("k", "v");
        assertThat(nativeOf(cache).getIfPresent("k")).isEqualTo("v");

        // Give pub/sub plenty of time to round-trip back to this node.
        // If self-skip were broken, the listener would evict L1 within ms.
        Thread.sleep(500);

        assertThat(nativeOf(cache).getIfPresent("k"))
                .as("own invalidation message must not evict own L1")
                .isEqualTo("v");
    }

    @Test
    void evictDoesNotRepopulateOwnL1Entry() throws InterruptedException {
        // After evict, L1 should be empty and stay empty even after the self
        // message is delivered.
        cache.put("k", "v");
        cache.evict("k");
        Thread.sleep(500);
        assertThat(nativeOf(cache).getIfPresent("k")).isNull();
    }
}
