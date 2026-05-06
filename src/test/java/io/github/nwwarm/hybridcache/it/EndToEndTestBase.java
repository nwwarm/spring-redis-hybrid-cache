package io.github.nwwarm.hybridcache.it;

import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cache.CacheManager;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Boots a real Spring Boot context against a Testcontainers Redis. Subclasses
 * inherit the autowired {@link TestApplication.ProductService},
 * {@link RedissonClient}, and {@link CacheManager} and exercise the library
 * through Spring's actual cache wiring rather than constructing
 * {@code NearCache} directly.
 *
 * <p>Single-server topology only. Cluster and Sentinel end-to-end coverage
 * lives in their own bases (out of scope for this prompt).
 */
@SpringBootTest(
        classes = TestApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers
public abstract class EndToEndTestBase {

    @Container
    protected static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void cacheProperties(DynamicPropertyRegistry registry) {
        registry.add("cache.server.address",
                () -> "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379));
        // Restrict the JSON codec's polymorphic type validator to the test package
        // so the Product record can round-trip through L2.
        registry.add("cache.allowed-packages", () -> "io.github.nwwarm.");
    }

    @Autowired
    protected TestApplication.ProductService productService;

    @Autowired
    protected RedissonClient redisson;

    @Autowired
    protected CacheManager cacheManager;
}
