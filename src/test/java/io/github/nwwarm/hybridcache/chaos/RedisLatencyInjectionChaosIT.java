package io.github.nwwarm.hybridcache.chaos;

import eu.rekawek.toxiproxy.model.ToxicDirection;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.redisson.api.RedissonClient;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Latency-injection chaos.
 *
 * <p>Covers §4 row "Redis becomes slow": the per-cache breaker's
 * slow-call detection trips when Redis latency exceeds the threshold,
 * and the breaker recovers when latency returns to normal. Asserts
 * cache state is not corrupted across the open / half-open / closed
 * transitions.
 *
 * <p>Three scenarios injected via Toxiproxy {@code latency} toxic:
 * 100ms, 500ms, 2s per Redis command. Threshold tuned via the
 * fast-fail breaker config (slow-call threshold 800ms, slow-call rate
 * 80%).
 */
class RedisLatencyInjectionChaosIT extends ChaosTestBase {

    private RedissonClient client;
    private CircuitBreaker breaker;

    @BeforeAll
    static void setupAll() throws Exception {
        setupProxy();
    }

    @BeforeEach
    void setup() {
        client = newClient();
        // Mirror RedisTestBase.fastFailBreaker for parity with the
        // production-config integration tests.
        breaker = io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry.of(
                io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.custom()
                        .slidingWindowSize(5)
                        .minimumNumberOfCalls(3)
                        .failureRateThreshold(50f)
                        .slowCallDurationThreshold(Duration.ofMillis(800))
                        .slowCallRateThreshold(80f)
                        .waitDurationInOpenState(Duration.ofSeconds(2))
                        .permittedNumberOfCallsInHalfOpenState(2)
                        .build())
                .circuitBreaker("chaos-latency-" + System.nanoTime());
    }

    @AfterEach
    void teardown() throws Exception {
        if (redisProxy != null) {
            for (var t : redisProxy.toxics().getAll()) t.remove();
        }
        if (client != null) client.shutdown();
    }

    @ParameterizedTest
    @ValueSource(longs = {100L, 500L, 2_000L})
    void latency_injection_trips_breaker(long latencyMs) throws Exception {
        // Inject downstream (response) latency so client wait-time crosses
        // the slow-call threshold reliably.
        redisProxy.toxics().latency("slow", ToxicDirection.DOWNSTREAM, latencyMs).setJitter(0);

        // Hit Redis a handful of times so the sliding window fills with
        // slow calls; the breaker should open at slowCallRateThreshold.
        for (int i = 0; i < 10 && breaker.getState() != CircuitBreaker.State.OPEN; i++) {
            try {
                breaker.executeSupplier(() -> client.getBucket("chaos:k:" + System.nanoTime()).get());
            } catch (Throwable ignored) { /* slow call, may not throw */ }
        }

        if (latencyMs >= 1_000) {
            assertThat(breaker.getState())
                    .as("Breaker should open under sustained slow calls at %dms", latencyMs)
                    .isEqualTo(CircuitBreaker.State.OPEN);
        }

        // Recovery: drop the toxic, advance past wait-duration-in-open-state,
        // verify breaker closes after probe calls succeed.
        redisProxy.toxics().get("slow").remove();
        if (breaker.getState() == CircuitBreaker.State.OPEN) {
            await()
                    .atMost(Duration.ofSeconds(10))
                    .ignoreException(CallNotPermittedException.class)
                    .untilAsserted(() -> {
                        breaker.executeSupplier(() -> client.getBucket("chaos:probe").get());
                        breaker.executeSupplier(() -> client.getBucket("chaos:probe").get());
                        assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
            });
        }
    }

    @Test
    void cache_state_intact_after_recovery() throws Exception {
        // Seed via a clean path before chaos kicks in.
        client.<String>getBucket("chaos:keep").set("v");

        redisProxy.toxics().latency("slow", ToxicDirection.DOWNSTREAM, 1_500L).setJitter(0);
        for (int i = 0; i < 8; i++) {
            try {
                breaker.executeSupplier(() -> client.getBucket("chaos:k:" + System.nanoTime()).get());
            } catch (Throwable ignored) { }
        }
        redisProxy.toxics().get("slow").remove();

        await()
                .atMost(Duration.ofSeconds(10))
                .ignoreException(CallNotPermittedException.class)
                .untilAsserted(() -> {
                    String v = breaker.executeSupplier(() -> (String) client.getBucket("chaos:keep").get());
                    assertThat(v).isEqualTo("v");
        });
    }
}
