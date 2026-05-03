package io.github.nwwarm.e2e;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression guard for the library's Spring wiring: a {@code @Cacheable}
 * call through Spring's real cache abstraction must populate L1, populate
 * L2, and a {@code @CacheEvict} must clear both.
 *
 * <p>Catches two classes of wiring bug that the unit-flavored suite cannot
 * see:
 * <ol>
 *   <li>Auto-config descriptor not discovered (asserts that
 *       {@link io.github.nwwarm.CacheConfig} is imported by
 *       {@code @SpringBootApplication}, exposing {@code RedissonClient},
 *       the {@code InvalidationDispatcher}, and the {@code CacheManager}).</li>
 *   <li>{@code @Cacheable} bypassing the library's tier dispatch (asserts
 *       that the resolved cache is the {@code NearCache} implementation
 *       rather than a plain Caffeine cache — verified via the L2 bucket
 *       being populated).</li>
 * </ol>
 */
class EndToEndWiringIT extends EndToEndTestBase {

    @Test
    void cacheableThroughSpringWiring_engagesL1AndL2_andEvictPropagates() {
        long before = productService.loaderCallCount();

        // 1. First call populates the cache via the loader.
        TestApplication.Product first = productService.findById(42L);
        assertThat(first).isEqualTo(new TestApplication.Product(42L, "name-42"));
        assertThat(productService.loaderCallCount())
                .as("first call should run the loader once")
                .isEqualTo(before + 1);

        // 2. Second call hits L1 — loader must not run again.
        TestApplication.Product second = productService.findById(42L);
        assertThat(second).isEqualTo(first);
        assertThat(productService.loaderCallCount())
                .as("second call should hit L1 and skip the loader")
                .isEqualTo(before + 1);

        // 3. Value must be present in Redis (L2). NearCache writes under
        //    "{<cacheName>:<key>}:v:<generation>" (hash-tagged so value and
        //    lock collocate on one Cluster slot); generation starts at 0.
        //    Use isExists() to avoid round-tripping the Product through the
        //    global codec — that's a codec concern, not a wiring concern.
        assertThat(redisson.getBucket("{products:42}:v:0").isExists())
                .as("L2 should hold the value — proves the NearCache tier is engaged")
                .isTrue();

        // 4. @CacheEvict must remove the value from Redis.
        productService.invalidate(42L);
        assertThat(redisson.getBucket("{products:42}:v:0").isExists())
                .as("@CacheEvict should clear L2")
                .isFalse();

        // 5. Next call re-runs the loader — proves the evict reached this node's L1.
        productService.findById(42L);
        assertThat(productService.loaderCallCount())
                .as("loader should run again after evict")
                .isEqualTo(before + 2);
    }
}
