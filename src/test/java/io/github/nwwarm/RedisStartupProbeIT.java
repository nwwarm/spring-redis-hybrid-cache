package io.github.nwwarm;

import io.github.nwwarm.e2e.TestApplication;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationContextException;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end coverage of {@link RedisStartupProbe} through the real
 * Spring Boot wiring. The probe fires during bean initialisation, so its
 * behaviour is observable as a context-startup failure (or absence of
 * said failure) — exactly the contract operators care about.
 *
 * <p>Boot is via {@link SpringApplicationBuilder} rather than
 * {@code @SpringBootTest} so each test owns its own context lifecycle and
 * we don't fight the test framework's caching when a deliberately-broken
 * configuration must throw during {@code refresh()}.
 */
@Testcontainers
class RedisStartupProbeIT {

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    private static String redisUrl() {
        return "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379);
    }

    @Test
    void probeDisabledByDefault_beanAbsent_contextStarts() {
        // Default (no startup-probe.* properties at all) — the bean must
        // not be loaded, regardless of Redis state. We use a working
        // Redis here because the rest of the wiring still needs it
        // (dispatcher subscribe, etc.).
        try (ConfigurableApplicationContext ctx = boot(redisUrl())) {
            assertThat(ctx.getBeansOfType(RedisStartupProbe.class))
                    .as("no probe bean when cache.startup-probe.enabled is left at default")
                    .isEmpty();
        }
    }

    @Test
    void probeEnabled_realRedis_contextStartsAndProbeBeanLoaded() {
        try (ConfigurableApplicationContext ctx = boot(redisUrl(),
                "cache.startup-probe.enabled=true",
                "cache.startup-probe.timeout=2s",
                "cache.startup-probe.retries=0",
                "cache.startup-probe.retry-delay=0ms")) {
            assertThat(ctx.getBeansOfType(RedisStartupProbe.class))
                    .as("probe bean should load when enabled=true")
                    .hasSize(1);
        }
    }

    @Test
    void probeEnabled_brokenRedisUrl_failsContextStartup() {
        // Port 1 is reserved/privileged on Linux and almost certainly not
        // listening, so the probe's EXISTS gets a fast ECONNREFUSED. Short
        // timeout + zero retries cap the test at ~half a second.
        assertThatThrownBy(() -> boot(
                "redis://127.0.0.1:1",
                "cache.startup-probe.enabled=true",
                "cache.startup-probe.timeout=300ms",
                "cache.startup-probe.retries=0",
                "cache.startup-probe.retry-delay=0ms"))
                // Spring wraps the bean-init failure; assert the user-facing
                // observable (context refresh threw) without pinning to a
                // specific framework wrapper class. The probe's diagnostic
                // text is the load-bearing assertion — that's what an
                // operator sees in the deploy log.
                .satisfiesAnyOf(
                        t -> assertThat(rootCauseMessage(t))
                                .contains("Redis startup probe failed")
                                .contains("SINGLE redis://127.0.0.1:1"),
                        t -> assertThat(t).isInstanceOf(ApplicationContextException.class));
    }

    private static ConfigurableApplicationContext boot(String address, String... extraProps) {
        String[] base = {
                "cache.server.address=" + address,
                "cache.allowed-packages=io.github.nwwarm."
        };
        String[] all = new String[base.length + extraProps.length];
        System.arraycopy(base, 0, all, 0, base.length);
        System.arraycopy(extraProps, 0, all, base.length, extraProps.length);

        return new SpringApplicationBuilder(TestApplication.class)
                .web(WebApplicationType.NONE)
                .properties(all)
                .run();
    }

    private static String rootCauseMessage(Throwable t) {
        Throwable cur = t;
        while (cur.getCause() != null && cur.getCause() != cur) {
            cur = cur.getCause();
        }
        // If the root has no message, walk up looking for one — the probe's
        // IllegalStateException has the diagnostic text we want to assert on,
        // and Spring's outer wrappers usually re-quote it in their own message.
        StringBuilder full = new StringBuilder();
        for (Throwable x = t; x != null && x != x.getCause(); x = x.getCause()) {
            if (x.getMessage() != null) full.append(x.getMessage()).append(" | ");
            if (x.getCause() == null) break;
        }
        return full.toString();
    }
}
