package io.github.nwwarm.hybridcache.core;

import io.github.nwwarm.hybridcache.it.TestApplication;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the {@link ReconciliationCoordinator} stops before
 * {@link RedissonClient} during Spring context shutdown — the same
 * guarantee {@link io.github.nwwarm.hybridcache.invalidation.InvalidationDispatcher}
 * relies on, since both beans share the
 * {@link ReconciliationCoordinator#LIFECYCLE_PHASE} value.
 *
 * <p>Mechanism: enable reconciliation on a near-cache and exercise the
 * publish path so the coordinator is registered. Close the context. Assert
 * the coordinator is no longer running, and that during teardown its
 * scheduler shut down without leaving stuck threads (catches the
 * "scheduler outlives Redisson" failure mode that item 5 of the
 * ## Reconciliation guardrails calls out).
 */
@Testcontainers
class ReconciliationShutdownOrderingIT {

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    @Test
    void contextClose_coordinatorStopsBeforeRedissonShutdown() {
        String address = "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379);

        ConfigurableApplicationContext ctx = new SpringApplicationBuilder(TestApplication.class)
                .web(WebApplicationType.NONE)
                .properties(
                        "cache.server.address=" + address,
                        "cache.allowed-packages=io.github.nwwarm.",
                        // Enable reconciliation on the products cache used by
                        // the test application's @Cacheable surface.
                        "cache.caches.products.tier=NEAR_CACHE",
                        "cache.caches.products.reconciliation.enabled=true",
                        "cache.caches.products.reconciliation.interval=200ms",
                        "cache.caches.products.reconciliation.miss-tolerance=0")
                .run();

        ReconciliationCoordinator coordinator =
                ctx.getBean(ReconciliationCoordinator.class);
        RedissonClient redisson = ctx.getBean(RedissonClient.class);
        TestApplication.ProductService svc =
                ctx.getBean(TestApplication.ProductService.class);

        // Trigger lazy cache construction so the cache registers itself
        // with the coordinator.
        svc.findById(1L);
        svc.findById(2L);

        assertThat(coordinator.isRunning())
                .as("coordinator should be auto-started by the LifecycleProcessor")
                .isTrue();
        assertThat(coordinator.registrationCount())
                .as("the products cache should have registered itself")
                .isGreaterThanOrEqualTo(1);
        assertThat(redisson.isShutdown()).isFalse();

        // Close the context. SmartLifecycle.stop() runs first; then the
        // bean-destruction phase disposes Redisson.
        ctx.close();

        assertThat(coordinator.isRunning()).isFalse();
        assertThat(redisson.isShutdown())
                .as("Redisson is destroyed by its destroyMethod after lifecycle stop")
                .isTrue();
        assertThat(coordinator.registrationCount())
                .as("registrations are cleared on stop()")
                .isZero();
    }

    @Test
    void contextClose_isClean_evenWithoutAnyReconcilingCache() {
        // Coordinator is registered unconditionally (mirroring the preloader);
        // make sure context shutdown is still clean when no cache opted in.
        String address = "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379);
        ConfigurableApplicationContext ctx = new SpringApplicationBuilder(TestApplication.class)
                .web(WebApplicationType.NONE)
                .properties(
                        "cache.server.address=" + address,
                        "cache.allowed-packages=io.github.nwwarm.")
                .run();
        ReconciliationCoordinator coordinator =
                ctx.getBean(ReconciliationCoordinator.class);
        assertThat(coordinator.registrationCount()).isZero();
        ctx.close();
        assertThat(coordinator.isRunning()).isFalse();
    }
}
