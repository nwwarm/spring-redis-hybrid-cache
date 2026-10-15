package io.github.nwwarm.hybridcache.it;

import io.github.nwwarm.hybridcache.config.CacheConfig;
import io.github.nwwarm.hybridcache.core.HybridCacheManager;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cache.CacheManager;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression guard: a consuming application's own {@code @Configuration}
 * class is always processed before any auto-configuration, so an
 * application-supplied {@code CacheManager} bean legitimately makes
 * {@link CacheConfig#cacheManager} back off via its own
 * {@code @ConditionalOnMissingBean(CacheManager.class)} — that part is
 * working as designed. But {@link CacheConfig#hybridCacheHealthIndicator}
 * unconditionally requires a {@link HybridCacheManager}, with no check that
 * {@code cacheManager()} actually produced one, so this ordinary, supported
 * override currently takes down the whole {@code ApplicationContext} instead
 * of just leaving the health indicator unregistered.
 *
 * <p>This is not a contrived edge case: it is exactly the shape of a real
 * incident, where a test-only {@code @Configuration} class with its own
 * stub {@code CacheManager} bean was unintentionally visible to an
 * unrelated {@code @SpringBootTest}, and the resulting crash's stack trace
 * pointed at {@code HybridCacheManager} autowiring, not at the real cause.
 */
@Testcontainers
class CacheManagerOverrideDoesNotCrashContextIT {

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    @Configuration
    static class CompetingCacheManagerConfig {
        @Bean
        CacheManager cacheManager() {
            return new ConcurrentMapCacheManager();
        }
    }

    private ApplicationContextRunner contextRunner() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(CacheConfig.class))
                .withUserConfiguration(CompetingCacheManagerConfig.class)
                .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
                .withPropertyValues(
                        "cache.server.address=redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379),
                        "cache.allowed-packages=io.github.nwwarm.");
    }

    @Test
    void applicationSuppliedCacheManagerOverride_doesNotCrashTheContext() {
        contextRunner().run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(CacheManager.class))
                    .as("the application's own explicit bean must win — that part already works correctly")
                    .isInstanceOf(ConcurrentMapCacheManager.class);
        });
    }
}
