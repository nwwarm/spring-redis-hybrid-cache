package io.github.nwwarm.hybridcache.probe;

import io.github.nwwarm.hybridcache.config.CacheProperties;
import org.junit.jupiter.api.Test;
import org.redisson.api.RBucket;
import org.redisson.api.RFuture;
import org.redisson.api.RedissonClient;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Pure-Mockito coverage of {@link RedisStartupProbe} — no Redis container.
 * Verifies retry counting, topology naming in error messages, the
 * LOCAL_ONLY skip path, and the single-attempt success path.
 *
 * <p>The probe wraps {@code RBucket.isExistsAsync()} in a timeout-bounded
 * await on the underlying {@link CompletableFuture}; we substitute that
 * future to simulate either a clean response or a deterministic failure.
 */
class RedisStartupProbeTest {

    @Test
    void probe_succeeds_onFirstAttempt() {
        AtomicInteger attempts = new AtomicInteger();
        RedissonClient redisson = mockRedissonReturning(() -> {
            attempts.incrementAndGet();
            return CompletableFuture.completedFuture(true);
        });
        CacheProperties props = propsWithProbe(
                new CacheProperties.StartupProbe(true, Duration.ofSeconds(1), 2, Duration.ofMillis(10)),
                CacheProperties.Tier.NEAR_CACHE);

        RedisStartupProbe probe = new RedisStartupProbe(redisson, props);
        assertThat(catchThrowable(probe::afterPropertiesSet)).isNull();

        assertThat(attempts.get()).isEqualTo(1);
        verify(redisson, times(1)).getBucket(anyString());
    }

    @Test
    @SuppressWarnings("unchecked")
    void probe_failsAfterRetries_messageNamesTopologyAndTimeout() {
        AtomicInteger attempts = new AtomicInteger();
        RedissonClient redisson = mockRedissonReturning(() -> {
            attempts.incrementAndGet();
            CompletableFuture<Boolean> failed = new CompletableFuture<>();
            failed.completeExceptionally(new RuntimeException("simulated Redis outage"));
            return failed;
        });
        CacheProperties props = propsWithProbe(
                new CacheProperties.StartupProbe(true, Duration.ofMillis(50), 2, Duration.ofMillis(5)),
                CacheProperties.Tier.NEAR_CACHE);

        RedisStartupProbe probe = new RedisStartupProbe(redisson, props);

        assertThatThrownBy(probe::afterPropertiesSet)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Redis startup probe failed after 3 attempt(s)")
                .hasMessageContaining("SINGLE redis://localhost:6379")
                .hasMessageContaining("PT0.05S")             // timeout
                .hasMessageContaining("PT0.005S")            // retry-delay
                .hasMessageContaining("simulated Redis outage")
                .hasCauseInstanceOf(java.util.concurrent.ExecutionException.class);

        assertThat(attempts.get())
                .as("should make retries+1 = 3 attempts before failing")
                .isEqualTo(3);
    }

    @Test
    void probe_skips_whenAllCachesAreLocalOnly() {
        // Even though enabled=true, every cache is LOCAL_ONLY so Redis is not
        // on the request path — the probe must no-op and not even touch
        // RedissonClient. (If it did, an unconfigured mock would NPE; that
        // doubles as the assertion that no Redis call was issued.)
        RedissonClient redisson = mock(RedissonClient.class);

        CacheProperties.CacheSpec localOnly = new CacheProperties.CacheSpec(
                CacheProperties.Tier.LOCAL_ONLY,
                Duration.ofMinutes(10), 10_000,
                Duration.ofSeconds(2), Duration.ofSeconds(10),
                CacheProperties.Codec.JSON, null, null, null, 0.0, null, null, null, null, null);
        CacheProperties props = new CacheProperties(
                singleServer(),
                Map.of("a", localOnly, "b", localOnly),
                localOnly,
                "node",
                List.of("io.github.nwwarm."),
                null, null, null, false, null,
                new CacheProperties.StartupProbe(true,
                        Duration.ofSeconds(1), 0, Duration.ZERO),
                null, null, null);

        RedisStartupProbe probe = new RedisStartupProbe(redisson, props);
        assertThat(catchThrowable(probe::afterPropertiesSet)).isNull();
        verify(redisson, never()).getBucket(anyString());
    }

    @Test
    void probe_runs_whenDefaultSpecLocalOnly_butExplicitCacheUsesRedis() {
        AtomicInteger attempts = new AtomicInteger();
        RedissonClient redisson = mockRedissonReturning(() -> {
            attempts.incrementAndGet();
            return CompletableFuture.completedFuture(false);
        });
        CacheProperties.CacheSpec localOnly = new CacheProperties.CacheSpec(
                CacheProperties.Tier.LOCAL_ONLY,
                Duration.ofMinutes(10), 10_000,
                Duration.ofSeconds(2), Duration.ofSeconds(10),
                CacheProperties.Codec.JSON, null, null, null, 0.0, null, null, null, null, null);
        CacheProperties.CacheSpec nearCache = new CacheProperties.CacheSpec(
                CacheProperties.Tier.NEAR_CACHE,
                Duration.ofMinutes(10), 10_000,
                Duration.ofSeconds(2), Duration.ofSeconds(10),
                CacheProperties.Codec.JSON, null, null, null, 0.0, null, null, null, null, null);
        CacheProperties props = new CacheProperties(
                singleServer(),
                Map.of("hot", nearCache),    // one cache uses Redis
                localOnly,                    // default-spec is LOCAL_ONLY
                "node",
                List.of("io.github.nwwarm."),
                null, null, null, false, null,
                new CacheProperties.StartupProbe(true,
                        Duration.ofSeconds(1), 0, Duration.ZERO),
                null, null, null);

        RedisStartupProbe probe = new RedisStartupProbe(redisson, props);
        assertThat(catchThrowable(probe::afterPropertiesSet)).isNull();
        assertThat(attempts.get()).isEqualTo(1);
    }

    @Test
    void probe_failureMessage_namesClusterTopology() {
        RedissonClient redisson = mockRedissonReturning(() -> {
            CompletableFuture<Boolean> failed = new CompletableFuture<>();
            failed.completeExceptionally(new RuntimeException("nope"));
            return failed;
        });
        CacheProperties props = new CacheProperties(
                new CacheProperties.Server(
                        CacheProperties.Mode.CLUSTER, null,
                        List.of("redis://a:6379", "redis://b:6379", "redis://c:6379"),
                        null, null, null),
                Map.of(),
                null, "node",
                List.of("io.github.nwwarm."),
                null, null, null, false, null,
                new CacheProperties.StartupProbe(true, Duration.ofMillis(50), 0, Duration.ZERO),
                null, null, null);

        RedisStartupProbe probe = new RedisStartupProbe(redisson, props);
        assertThatThrownBy(probe::afterPropertiesSet)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("CLUSTER (3 seed node(s))");
    }

    @Test
    void probe_failureMessage_namesSentinelTopology() {
        RedissonClient redisson = mockRedissonReturning(() -> {
            CompletableFuture<Boolean> failed = new CompletableFuture<>();
            failed.completeExceptionally(new RuntimeException("nope"));
            return failed;
        });
        CacheProperties props = new CacheProperties(
                new CacheProperties.Server(
                        CacheProperties.Mode.SENTINEL, null,
                        List.of("redis://s1:26379", "redis://s2:26379"),
                        "mymaster", null, null),
                Map.of(),
                null, "node",
                List.of("io.github.nwwarm."),
                null, null, null, false, null,
                new CacheProperties.StartupProbe(true, Duration.ofMillis(50), 0, Duration.ZERO),
                null, null, null);

        RedisStartupProbe probe = new RedisStartupProbe(redisson, props);
        assertThatThrownBy(probe::afterPropertiesSet)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("SENTINEL master=mymaster")
                .hasMessageContaining("(2 sentinel(s))");
    }

    // ---------- helpers ----------

    @SuppressWarnings("unchecked")
    private static RedissonClient mockRedissonReturning(
            java.util.function.Supplier<CompletableFuture<Boolean>> futureSupplier) {
        RedissonClient redisson = mock(RedissonClient.class);
        RBucket<Object> bucket = mock(RBucket.class);
        when(redisson.getBucket(anyString())).thenReturn(bucket);
        when(bucket.isExistsAsync()).thenAnswer(inv -> {
            // Fresh future per call so retries don't share completion state.
            CompletableFuture<Boolean> cf = futureSupplier.get();
            // The probe only uses RFuture.toCompletableFuture(), so a thin
            // mock satisfying that single call is enough — no need to drag
            // in a full RFuture implementation.
            RFuture<Boolean> rfuture = mock(RFuture.class);
            when(rfuture.toCompletableFuture()).thenReturn(cf);
            return rfuture;
        });
        return redisson;
    }

    private static CacheProperties propsWithProbe(CacheProperties.StartupProbe probe,
                                                  CacheProperties.Tier tier) {
        CacheProperties.CacheSpec defaultSpec = new CacheProperties.CacheSpec(
                tier,
                Duration.ofMinutes(10), 10_000,
                Duration.ofSeconds(2), Duration.ofSeconds(10),
                CacheProperties.Codec.JSON, null, null, null, 0.0, null, null, null, null, null);
        return new CacheProperties(
                singleServer(),
                Map.of(),
                defaultSpec,
                "node",
                List.of("io.github.nwwarm."),
                null, null, null, false, null,
                probe, null, null, null);
    }

    private static CacheProperties.Server singleServer() {
        return new CacheProperties.Server(
                CacheProperties.Mode.SINGLE, "redis://localhost:6379",
                null, null, null, null);
    }

    private static Throwable catchThrowable(ThrowingRunnable r) {
        try {
            r.run();
            return null;
        } catch (Throwable t) {
            return t;
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Throwable;
    }
}
