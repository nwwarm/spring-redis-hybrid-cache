package io.github.nwwarm.hybridcache.it;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cross-node coherence over the full set of Spring cache key types.
 *
 * <p>{@link io.github.nwwarm.hybridcache.invalidation.InvalidationMessage#key()} is a {@code String} —
 * keys are stringified at every cache boundary. Without that, a {@code Long}
 * key would deserialize on the remote node as {@code Integer} and the
 * Caffeine eviction would silently miss; a {@code SimpleKey} would arrive as
 * a {@code LinkedHashMap} and never match. This test exercises each shape
 * end-to-end: write on A, await coherence on B, evict on A, await re-load
 * on B.
 */
class CrossNodeKeyTypesIT extends TwoContextEndToEndTestBase {

    private static final Duration COHERENCE_BUDGET = Duration.ofSeconds(3);

    // ---------- Long key ----------

    @Test
    void longKey_writeOnA_visibleOnB_thenEvictOnA_reloadsOnB() {
        long before = serviceA.loaderCallCount() + serviceB.loaderCallCount();

        TestApplication.Product fromA = serviceA.findById(42L);
        assertThat(fromA).isEqualTo(new TestApplication.Product(42L, "name-42"));

        Awaitility.await().atMost(COHERENCE_BUDGET).untilAsserted(() ->
                assertThat(serviceB.findById(42L)).isEqualTo(fromA));

        long bLoaderBefore = serviceB.loaderCallCount();

        serviceA.invalidate(42L);

        Awaitility.await().atMost(COHERENCE_BUDGET).untilAsserted(() -> {
            serviceB.findById(42L);
            assertThat(serviceB.loaderCallCount())
                    .as("B's loader must run again after A's evict propagates")
                    .isGreaterThan(bLoaderBefore);
        });

        // Sanity: total loader calls accumulated as expected (A once, B at
        // least twice — initial population, post-evict reload).
        assertThat(serviceA.loaderCallCount() + serviceB.loaderCallCount())
                .isGreaterThan(before + 1);
    }

    // ---------- String key ----------

    @Test
    void stringKey_writeOnA_visibleOnB_thenEvictOnA_reloadsOnB() {
        TestApplication.Product fromA = serviceA.findByName("widget");
        assertThat(fromA.name()).isEqualTo("widget");

        Awaitility.await().atMost(COHERENCE_BUDGET).untilAsserted(() ->
                assertThat(serviceB.findByName("widget")).isEqualTo(fromA));

        long bLoaderBefore = serviceB.loaderCallCount();

        serviceA.invalidateByName("widget");

        Awaitility.await().atMost(COHERENCE_BUDGET).untilAsserted(() -> {
            serviceB.findByName("widget");
            assertThat(serviceB.loaderCallCount()).isGreaterThan(bLoaderBefore);
        });
    }

    // ---------- UUID key ----------

    @Test
    void uuidKey_writeOnA_visibleOnB_thenEvictOnA_reloadsOnB() {
        UUID id = UUID.randomUUID();
        TestApplication.Product fromA = serviceA.findByUuid(id);

        Awaitility.await().atMost(COHERENCE_BUDGET).untilAsserted(() ->
                assertThat(serviceB.findByUuid(id)).isEqualTo(fromA));

        long bLoaderBefore = serviceB.loaderCallCount();

        serviceA.invalidateByUuid(id);

        Awaitility.await().atMost(COHERENCE_BUDGET).untilAsserted(() -> {
            serviceB.findByUuid(id);
            assertThat(serviceB.loaderCallCount()).isGreaterThan(bLoaderBefore);
        });
    }

    // ---------- SimpleKey, single-component ----------

    @Test
    void simpleKeySingleComponent_writeOnA_visibleOnB_thenEvictOnA_reloadsOnB() {
        TestApplication.Product fromA = serviceA.findBySimpleKey(7L);

        Awaitility.await().atMost(COHERENCE_BUDGET).untilAsserted(() ->
                assertThat(serviceB.findBySimpleKey(7L)).isEqualTo(fromA));

        long bLoaderBefore = serviceB.loaderCallCount();

        serviceA.invalidateBySimpleKey(7L);

        Awaitility.await().atMost(COHERENCE_BUDGET).untilAsserted(() -> {
            serviceB.findBySimpleKey(7L);
            assertThat(serviceB.loaderCallCount()).isGreaterThan(bLoaderBefore);
        });
    }

    // ---------- SimpleKey, multi-component ----------

    @Test
    void simpleKeyMultiComponent_writeOnA_visibleOnB_thenEvictOnA_reloadsOnB() {
        TestApplication.Product fromA = serviceA.findByRegion(99L, "EU");
        assertThat(fromA.name()).isEqualTo("EU-99");

        Awaitility.await().atMost(COHERENCE_BUDGET).untilAsserted(() ->
                assertThat(serviceB.findByRegion(99L, "EU")).isEqualTo(fromA));

        long bLoaderBefore = serviceB.loaderCallCount();

        serviceA.invalidateByRegion(99L, "EU");

        Awaitility.await().atMost(COHERENCE_BUDGET).untilAsserted(() -> {
            serviceB.findByRegion(99L, "EU");
            assertThat(serviceB.loaderCallCount()).isGreaterThan(bLoaderBefore);
        });
    }
}
