package io.github.nwwarm;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Simulates Redis becoming unresponsive via Testcontainers' {@code pause()} —
 * SIGSTOP on the container, which freezes the Redis process so client calls
 * hang and time out. Verifies:
 *
 * <ul>
 *   <li>L1 continues to serve cached entries.</li>
 *   <li>The circuit breaker opens after enough slow/failed calls.</li>
 *   <li>{@code unpause()} restores the breaker and L2 reads work again.</li>
 * </ul>
 *
 * <p>The Redisson client is configured with short timeouts and one retry, and
 * the breaker uses a small sliding window with a 2-second open state, so the
 * full transition completes well within the test timeout.
 */
class GracefulDegradationIT extends RedisTestBase {

    private RedissonClient redisson;
    private CircuitBreaker breaker;
    private NearCache cache;

    @BeforeEach
    void setUp() {
        redisson = newFailFastRedisson();
        breaker = fastFailBreaker();
        cache = newNearCache("degrade-" + System.nanoTime(),
                redisson, breaker, "node-A",
                CacheProperties.Codec.JSON,
                new io.micrometer.core.instrument.simple.SimpleMeterRegistry(),
                Duration.ofMinutes(10),
                Duration.ofMillis(200),     // short lock-wait so loaders fall through fast
                Duration.ofSeconds(5));
    }

    @AfterEach
    void tearDown() {
        cache.shutdown();
        if (redisson != null && !redisson.isShutdown()) redisson.shutdown();
        // Always unpause in case a test failed mid-way.
        if (REDIS.isRunning()) {
            try { REDIS.getDockerClient().unpauseContainerCmd(REDIS.getContainerId()).exec(); }
            catch (Exception ignored) { /* container may already be unpaused */ }
        }
    }

    @Test
    void pausedRedis_l1Serves_breakerOpens_recoveryAfterUnpause() {
        // 1. Healthy state: write a value and prime L1.
        cache.put("hot", "v");
        assertThat(cache.get("hot", String.class)).isEqualTo("v");
        assertThat(nativeOf(cache).getIfPresent("hot")).isEqualTo("v");

        // 2. Freeze Redis. Subsequent L2 calls will time out.
        REDIS.getDockerClient().pauseContainerCmd(REDIS.getContainerId()).exec();

        // 3. L1 still serves the cached entry — pause doesn't touch local state.
        assertThat(nativeOf(cache).getIfPresent("hot")).isEqualTo("v");

        // 4. Drive enough cold reads to trip the breaker. With minimumNumberOfCalls=3
        //    and a 500ms read-timeout, this completes in a few seconds.
        for (int i = 0; i < 10; i++) {
            cache.get("missing-" + i);
        }

        Awaitility.await().atMost(Duration.ofSeconds(8)).untilAsserted(() ->
                assertThat(breaker.getState())
                        .isIn(CircuitBreaker.State.OPEN, CircuitBreaker.State.FORCED_OPEN));

        // 5. With the breaker open, L2 ops short-circuit. Cold reads return null
        //    instantly; cached L1 entries still resolve.
        assertThat(cache.get("still-missing")).isNull();
        assertThat(nativeOf(cache).getIfPresent("hot")).isEqualTo("v");

        // 6. Unpause Redis. Wait for the breaker's open window (~2s) to elapse,
        //    then drive a few calls to close it via the half-open trial.
        REDIS.getDockerClient().unpauseContainerCmd(REDIS.getContainerId()).exec();

        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            // Trigger trials. Successful calls in HALF_OPEN close the breaker.
            cache.get("warmup-" + System.nanoTime());
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        });

        // 7. Full recovery — L2 writes and reads work again on a fresh key.
        cache.put("post-recovery", "ok");
        nativeOf(cache).invalidate("post-recovery");   // force L2 round-trip
        assertThat(cache.get("post-recovery", String.class)).isEqualTo("ok");
    }
}
