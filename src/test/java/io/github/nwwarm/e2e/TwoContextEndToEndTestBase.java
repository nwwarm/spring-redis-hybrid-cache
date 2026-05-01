package io.github.nwwarm.e2e;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.redisson.api.RedissonClient;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Boots two independent Spring Boot contexts pointing at the same
 * Testcontainers Redis. Each context simulates an application instance with
 * a distinct {@code cache.node-id} so cross-node invalidation messages are
 * not self-skipped.
 *
 * <p>This base is structural — tests that exercise it (cross-node L1
 * invalidation, generation-based clear propagation, etc.) come in
 * subsequent prompts.
 *
 * <p>Lifecycle: contexts are started in {@code @BeforeEach} and closed in
 * {@code @AfterEach} so each test gets fresh dispatcher subscriptions and a
 * clean generation counter.
 */
@Testcontainers
public abstract class TwoContextEndToEndTestBase {

    @Container
    protected static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    protected ConfigurableApplicationContext contextA;
    protected ConfigurableApplicationContext contextB;

    protected TestApplication.ProductService serviceA;
    protected TestApplication.ProductService serviceB;

    protected RedissonClient redissonA;
    protected RedissonClient redissonB;

    @BeforeEach
    void startContexts() {
        contextA = startContext("node-A");
        contextB = startContext("node-B");

        serviceA = contextA.getBean(TestApplication.ProductService.class);
        serviceB = contextB.getBean(TestApplication.ProductService.class);

        redissonA = contextA.getBean(RedissonClient.class);
        redissonB = contextB.getBean(RedissonClient.class);
    }

    @AfterEach
    void closeContexts() {
        if (contextB != null) contextB.close();
        if (contextA != null) contextA.close();
    }

    private ConfigurableApplicationContext startContext(String nodeId) {
        return new SpringApplicationBuilder(TestApplication.class)
                .web(WebApplicationType.NONE)
                .properties(
                        "cache.server.address=redis://"
                                + REDIS.getHost() + ":" + REDIS.getMappedPort(6379),
                        "cache.allowed-packages=io.github.nwwarm",
                        "cache.node-id=" + nodeId,
                        // Avoid logging clobber when two contexts run in the same JVM.
                        "spring.main.register-shutdown-hook=false")
                .run();
    }
}
