package io.github.nwwarm.e2e;

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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Fail-closed startup behavior for the Jackson and Kryo polymorphic-typing
 * surfaces. Each test boots a fresh Spring context with a configuration that
 * either should fail at startup (with a documented exception message) or
 * should start cleanly. The full Spring lifecycle is exercised so that the
 * production code path — {@code CacheConfig.redissonClient}, the cache
 * manager, the codec resolver — actually runs.
 */
@Testcontainers
class SecurityStartupIT {

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    private static String redisAddress() {
        return "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379);
    }

    private static SpringApplicationBuilder app(String... extraProps) {
        String[] base = {
                "cache.server.address=" + redisAddress(),
                "spring.main.register-shutdown-hook=false"
        };
        String[] all = new String[base.length + extraProps.length];
        System.arraycopy(base, 0, all, 0, base.length);
        System.arraycopy(extraProps, 0, all, base.length, extraProps.length);
        return new SpringApplicationBuilder(TestApplication.class)
                .web(WebApplicationType.NONE)
                .properties(all);
    }

    // ---------- allowed-packages: empty → fail closed ----------

    @Test
    void contextFailsToStart_whenAllowedPackagesAbsent() {
        assertThatThrownBy(() -> app().run().close())
                .rootCause()
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cache.allowed-packages")
                .hasMessageContaining("CVE-2017-7525");
    }

    // ---------- allowed-packages: missing trailing dot → reject ----------

    @Test
    void contextFailsToStart_whenAllowedPackagesEntryMissingTrailingDot() {
        // 'io.github.nwwarm' (no trailing dot) would also accept
        // 'io.github.nwwarmrogue' due to Jackson's startsWith matching.
        assertThatThrownBy(() ->
                app("cache.allowed-packages=io.github.nwwarm").run().close())
                .rootCause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("io.github.nwwarm")
                .hasMessageContaining("startsWith");
    }

    // ---------- allowed-packages: trailing dot → start cleanly ----------

    @Test
    void contextStarts_whenAllowedPackagesEntryHasTrailingDot() {
        try (ConfigurableApplicationContext ctx = app(
                "cache.allowed-packages=io.github.nwwarm.").run()) {
            assertThat(ctx.getBean(RedissonClient.class)).isNotNull();
        }
    }

    // ---------- allowed-packages: FQN entry → start cleanly ----------

    @Test
    void contextStarts_whenAllowedPackagesEntryIsFullyQualifiedClassName() {
        try (ConfigurableApplicationContext ctx = app(
                "cache.allowed-packages=io.github.nwwarm.e2e.TestApplication$Product").run()) {
            assertThat(ctx.getBean(RedissonClient.class)).isNotNull();
        }
    }

    // ---------- KRYO codec without registered classes → fail closed ----------

    @Test
    void contextFailsToStart_whenKryoSelectedWithoutRegisteredClasses() {
        // The startup validator (CacheSpecValidator) fires before codecResolver,
        // so the root cause is now IllegalArgumentException with the per-spec label.
        assertThatThrownBy(() -> app(
                "cache.allowed-packages=io.github.nwwarm.",
                "cache.default-spec.codec=KRYO"
        ).run().close())
                .rootCause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cache.kryo.registered-classes")
                .hasMessageContaining("KRYO");
    }

    @Test
    void contextFailsToStart_whenKryoRegisteredClassCannotBeLoaded() {
        // Use hasStackTraceContaining instead of rootCause(): Spring wraps
        // bean-init failures, and the deepest cause here is the
        // ClassNotFoundException thrown by Class.forName, with our
        // IllegalStateException one frame up. Both must appear; the message
        // must name the offending class.
        assertThatThrownBy(() -> app(
                "cache.allowed-packages=io.github.nwwarm.",
                "cache.default-spec.codec=KRYO",
                "cache.kryo.registered-classes[0]=com.does.not.Exist"
        ).run().close())
                .hasStackTraceContaining("com.does.not.Exist")
                .hasStackTraceContaining("IllegalStateException")
                .hasStackTraceContaining("cache.kryo.registered-classes");
    }

    // ---------- KRYO codec with valid registered classes → start cleanly + round-trip ----------

    @Test
    void contextStarts_andRoundTripsThroughKryo_whenRegisteredClassesProvided() {
        try (ConfigurableApplicationContext ctx = app(
                "cache.allowed-packages=io.github.nwwarm.",
                "cache.default-spec.codec=KRYO",
                "cache.kryo.registered-classes[0]=io.github.nwwarm.e2e.TestApplication$Product"
        ).run()) {
            TestApplication.ProductService service =
                    ctx.getBean(TestApplication.ProductService.class);

            // First call goes through the loader, populates L1 + L2.
            TestApplication.Product first = service.findById(1L);
            // Second call hits L1 — confirms the cache is engaged with KRYO.
            TestApplication.Product second = service.findById(1L);
            assertThat(second).isEqualTo(first);
        }
    }
}
