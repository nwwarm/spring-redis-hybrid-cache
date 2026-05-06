package io.github.nwwarm;

import io.github.nwwarm.e2e.TestApplication;
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
 * Verifies the {@link InvalidationDispatcher} stops <em>before</em>
 * {@link RedissonClient} during a real Spring Boot context shutdown.
 *
 * <p>Spring's contract: {@code SmartLifecycle.stop()} runs during
 * {@code LifecycleProcessor.onClose()}, which precedes the bean-destruction
 * phase that disposes ordinary beans. {@link RedissonClient} is destroyed
 * via its {@code destroyMethod}, so it always tears down in the destruction
 * phase. This test exercises the production wiring end-to-end and asserts
 * the diagnostic flag captured at stop time —
 * {@link InvalidationDispatcher#wasRedissonAliveAtStop()} —
 * is {@code true}, meaning Redisson was still up the moment the
 * dispatcher's {@code stop()} ran.
 *
 * <p>Boot is via {@link SpringApplicationBuilder} rather than
 * {@code @SpringBootTest} so the test owns the lifecycle entirely. The
 * test closes the context inside the method body and asserts on captured
 * post-close state; that pattern collides with Spring's
 * {@code TestContextManager} when run through {@code @SpringBootTest}
 * (the framework keeps acting on the now-closed context during its own
 * after-method hooks).
 */
@Testcontainers
class DispatcherShutdownOrderingIT {

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    @Test
    void contextClose_dispatcherStopsBeforeRedissonShutdown() {
        String address = "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379);

        ConfigurableApplicationContext ctx = new SpringApplicationBuilder(TestApplication.class)
                .web(WebApplicationType.NONE)
                .properties(
                        "cache.server.address=" + address,
                        "cache.allowed-packages=io.github.nwwarm.")
                .run();

        InvalidationDispatcher dispatcher = ctx.getBean(InvalidationDispatcher.class);
        RedissonClient redisson = ctx.getBean(RedissonClient.class);

        // Pre-conditions: both beans are wired and live.
        assertThat(dispatcher.isRunning())
                .as("dispatcher should be auto-started by the LifecycleProcessor")
                .isTrue();
        assertThat(redisson.isShutdown()).isFalse();
        assertThat(dispatcher.wasRedissonAliveAtStop())
                .as("stop() has not run yet")
                .isNull();

        ctx.close();

        // Post-conditions: lifecycle stop ran, then bean destruction ran.
        assertThat(dispatcher.isRunning()).isFalse();
        assertThat(redisson.isShutdown())
                .as("Redisson should be shut down by the destroy method")
                .isTrue();

        // The actual ordering claim: at the moment dispatcher.stop() ran,
        // Redisson was still alive. If this is false, our SmartLifecycle
        // phase (or the contract's lifecycle-vs-destruction split) is
        // not delivering the ordering guarantee we documented.
        assertThat(dispatcher.wasRedissonAliveAtStop())
                .as("dispatcher.stop() must run before Redisson's destroyMethod")
                .isTrue();
    }
}
