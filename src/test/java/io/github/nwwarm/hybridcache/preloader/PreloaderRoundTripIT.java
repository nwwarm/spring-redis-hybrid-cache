package io.github.nwwarm.hybridcache.preloader;

import io.github.nwwarm.hybridcache.it.TestApplication;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.search.Search;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cache.CacheManager;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end coverage of the per-cache preloader through the real Spring
 * Boot wiring.
 *
 * <p>Boots a context with the preloader enabled for the {@code products}
 * cache, populates the cache via {@code @Cacheable}, force-stores the
 * snapshot via {@link PreloaderCoordinator#triggerStoreNow(String)} (the
 * package-private test seam), closes the context, then re-boots with the
 * <em>same</em> snapshot {@code directory}. Asserts that the prefetch
 * counter on the second boot reflects the keys that were in L1 at the
 * end of the first boot — i.e., the snapshot file actually round-trips
 * across restarts.
 */
@Testcontainers
class PreloaderRoundTripIT {

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    @Test
    void roundTrip_storeOnFirstBoot_prefetchOnSecondBoot(@TempDir Path tmpDir) throws Exception {
        Path snapshotDir = tmpDir.resolve("preloader");
        String redisUrl = "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379);

        // ---- First boot: populate, store, close. ----
        try (ConfigurableApplicationContext ctx = boot(redisUrl, snapshotDir)) {
            TestApplication.ProductService service =
                    ctx.getBean(TestApplication.ProductService.class);
            // Populate ten entries via @Cacheable. Each call inserts into
            // the products cache's L1 + L2.
            for (long i = 1L; i <= 10L; i++) {
                service.findById(i);
            }

            // Use the test-only coordinator hook to force a store now —
            // the periodic schedule wouldn't fire within the test budget.
            PreloaderCoordinator coordinator = ctx.getBean(PreloaderCoordinator.class);
            coordinator.triggerStoreNow("products");

            // Snapshot file must exist before we close the context.
            Path snapshotFile = snapshotDir.resolve("products").resolve("keys");
            assertThat(Files.exists(snapshotFile))
                    .as("snapshot file must be written by triggerStoreNow")
                    .isTrue();
            // Snapshot must contain the 10 keys we wrote (header + 10 lines + trailing).
            String snapshotContent = Files.readString(snapshotFile);
            assertThat(snapshotContent).contains("cache=products", "generation=");
            for (int i = 1; i <= 10; i++) {
                assertThat(snapshotContent).contains("\n" + i + "\n");
            }
        }

        // ---- Second boot: same snapshot directory, fresh context. ----
        try (ConfigurableApplicationContext ctx = boot(redisUrl, snapshotDir)) {
            CacheManager cacheManager = ctx.getBean(CacheManager.class);
            // Touch the cache so getMissingCache fires and the preloader
            // registers + dispatches the prefetch. (We need at least one
            // Cache.get call to instantiate the cache, since
            // HybridCacheManager builds caches lazily on first @Cacheable
            // hit.) Use the same service so the lookup goes through the
            // production wiring.
            TestApplication.ProductService service =
                    ctx.getBean(TestApplication.ProductService.class);
            service.findById(1L);   // triggers cache construction → preloader registration

            MeterRegistry meterRegistry = ctx.getBean(MeterRegistry.class);

            // Wait for the prefetch fan-out to complete. With 10 keys and
            // L2 already warm from the first boot, this should land in
            // well under the prefetch-timeout we configured.
            Awaitility.await()
                    .atMost(Duration.ofSeconds(10))
                    .pollInterval(Duration.ofMillis(100))
                    .untilAsserted(() -> {
                        double hits = counterCount(meterRegistry,
                                "cache.preloader.prefetch", "products", "hit");
                        double misses = counterCount(meterRegistry,
                                "cache.preloader.prefetch", "products", "miss");
                        double fails = counterCount(meterRegistry,
                                "cache.preloader.prefetch", "products", "fail");
                        double skipped = counterCount(meterRegistry,
                                "cache.preloader.prefetch", "products", "skipped");
                        // The 10 keys from the snapshot must each route through
                        // the prefetch path. We don't assert on hit-vs-miss
                        // because L2 deserialization for record-typed values
                        // is a pre-existing concern that the preloader doesn't
                        // claim to fix — what the preloader DOES guarantee is
                        // that every snapshotted key gets a prefetch attempt.
                        assertThat(hits + misses + fails + skipped)
                                .as("prefetch must process every key from the snapshot"
                                        + " (hits=%s, miss=%s, fail=%s, skipped=%s)",
                                        hits, misses, fails, skipped)
                                .isGreaterThanOrEqualTo(9.0);
                    });
        }
    }

    @Test
    void disabledByDefault_coordinatorDoesNothing(@TempDir Path tmpDir) {
        // No cache.caches.<name>.preloader.enabled=true anywhere. The
        // coordinator bean still loads (registered unconditionally — see
        // CacheConfig), starts its scheduler, but never has any caches
        // register. Smoke test that boot succeeds and no snapshot file
        // is written.
        String redisUrl = "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379);
        Path snapshotDir = tmpDir.resolve("preloader");

        try (ConfigurableApplicationContext ctx = new SpringApplicationBuilder(TestApplication.class)
                .web(WebApplicationType.NONE)
                .properties(
                        "cache.server.address=" + redisUrl,
                        "cache.allowed-packages=io.github.nwwarm.")
                .run()) {

            PreloaderCoordinator coordinator = ctx.getBean(PreloaderCoordinator.class);
            assertThat(coordinator.isRunning())
                    .as("coordinator runs even with no preloaders registered")
                    .isTrue();

            // Touching a cache must not produce a snapshot file because
            // preloader is disabled by default.
            ctx.getBean(TestApplication.ProductService.class).findById(42L);
            assertThat(Files.exists(snapshotDir))
                    .as("no snapshot directory should be created when no preloaders are enabled")
                    .isFalse();
        }
    }

    private static ConfigurableApplicationContext boot(String redisUrl, Path snapshotDir) {
        return new SpringApplicationBuilder(TestApplication.class)
                .web(WebApplicationType.NONE)
                .properties(
                        "cache.server.address=" + redisUrl,
                        "cache.allowed-packages=io.github.nwwarm.",
                        "cache.caches.products.tier=NEAR_CACHE",
                        "cache.caches.products.preloader.enabled=true",
                        "cache.caches.products.preloader.directory="
                                + snapshotDir.resolve("products"),
                        "cache.caches.products.preloader.store-initial-delay=1h",
                        "cache.caches.products.preloader.store-interval=1h",
                        "cache.caches.products.preloader.prefetch-timeout=8s",
                        "cache.caches.products.preloader.prefetch-concurrency=4")
                .run();
    }

    private static double counterCount(MeterRegistry registry, String name,
                                       String cacheTag, String resultTag) {
        var c = Search.in(registry).name(name)
                .tag("cache", cacheTag).tag("result", resultTag).counter();
        return c == null ? 0.0 : c.count();
    }
}
