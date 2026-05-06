package io.github.nwwarm;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.springframework.cache.Cache;

import static org.assertj.core.api.Assertions.assertThat;

/** Sanity check: put/get/evict on each tier. */
class BasicTierIT extends RedisTestBase {

    private RedissonClient redisson;

    @BeforeEach
    void setUp() {
        redisson = newRedisson();
    }

    @AfterEach
    void tearDown() {
        if (redisson != null) redisson.shutdown();
    }

    @Nested
    class LocalOnly {
        private LocalOnlyCache cache;

        @BeforeEach
        void init() { cache = newLocalCache("basic-local"); }

        @Test
        void putThenGetReturnsValue() {
            cache.put("k", "v");
            assertThat(cache.get("k")).isNotNull();
            assertThat(cache.get("k").get()).isEqualTo("v");
            assertThat(cache.get("k", String.class)).isEqualTo("v");
        }

        @Test
        void evictRemovesValue() {
            cache.put("k", "v");
            cache.evict("k");
            assertThat(cache.get("k")).isNull();
        }

        @Test
        void getMissReturnsNull() {
            assertThat(cache.get("absent")).isNull();
        }

        @Test
        void clearImmediateBehavesLikeClear() {
            // No distributed state to reconcile — clearImmediate must drop
            // every entry, same as clear(). The promotion of the method to
            // the HybridCache surface mustn't change behaviour for this tier.
            cache.put("k1", "v1");
            cache.put("k2", "v2");
            cache.clearImmediate();
            assertThat(cache.get("k1")).isNull();
            assertThat(cache.get("k2")).isNull();
        }
    }

    @Nested
    class DistributedOnly {
        private DistributedOnlyCache cache;

        @BeforeEach
        void init() {
            cache = newDistributedCache("basic-dist-" + System.nanoTime(),
                    redisson, defaultBreaker());
        }

        @Test
        void putThenGetReturnsValue() {
            cache.put("k", "v");
            Cache.ValueWrapper wrapper = cache.get("k");
            assertThat(wrapper).isNotNull();
            assertThat(wrapper.get()).isEqualTo("v");
        }

        @Test
        void evictRemovesValue() {
            cache.put("k", "v");
            cache.evict("k");
            assertThat(cache.get("k")).isNull();
        }

        @Test
        void getMissReturnsNull() {
            assertThat(cache.get("absent")).isNull();
        }
    }

    @Nested
    class NearCacheTier {
        private NearCache cache;

        @BeforeEach
        void init() {
            cache = newNearCache("basic-near-" + System.nanoTime(),
                    redisson, defaultBreaker(), "node-A");
        }

        @AfterEach
        void shutdown() { cache.shutdown(); }

        @Test
        void putThenGetReturnsValueFromL1() {
            cache.put("k", "v");
            // After put, the value is in L1 — verify by inspecting native cache directly.
            assertThat(nativeOf(cache).getIfPresent("k")).isEqualTo("v");
            assertThat(cache.get("k", String.class)).isEqualTo("v");
        }

        @Test
        void getFallsThroughToL2WhenL1Empty() {
            cache.put("k", "v");
            // Drop only L1; L2 still has it.
            nativeOf(cache).invalidate("k");
            assertThat(nativeOf(cache).getIfPresent("k")).isNull();

            // Read repopulates L1 from L2.
            assertThat(cache.get("k", String.class)).isEqualTo("v");
            assertThat(nativeOf(cache).getIfPresent("k")).isEqualTo("v");
        }

        @Test
        void evictRemovesFromBothLayers() {
            cache.put("k", "v");
            cache.evict("k");
            assertThat(nativeOf(cache).getIfPresent("k")).isNull();
            assertThat(cache.get("k")).isNull();
        }

        @Test
        void getMissReturnsNull() {
            assertThat(cache.get("absent")).isNull();
        }
    }
}
