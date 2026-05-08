package io.github.nwwarm.hybridcache.core;

import io.github.nwwarm.hybridcache.testfixtures.RedisTestBase;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SWR + async parity. The SWR contract (stale read returns immediately,
 * refresh fires asynchronously) holds for the {@link NearCache#retrieve}
 * path — same metric family, same in-flight collapse, same loader-gate
 * semantics. This test pins the parity per § 10 0.5.0 / async section.
 */
class SwrAsyncIT extends RedisTestBase {

    private RedissonClient redisson;
    private SimpleMeterRegistry meterRegistry;
    private RefreshExecutor refreshExecutor;

    @BeforeEach
    void setUp() {
        redisson = newRedisson();
        meterRegistry = new SimpleMeterRegistry();
        refreshExecutor = new RefreshExecutor(2, meterRegistry);
    }

    @AfterEach
    void tearDown() {
        try {
            refreshExecutor.stop();
        } finally {
            if (redisson != null) redisson.shutdown();
        }
    }

    @Test
    void asyncStaleRead_returnsCompletedFutureImmediately_dispatchesRefresh() throws Exception {
        CircuitBreaker breaker = defaultBreaker();
        NearCache cache = newSwrAsyncNearCache("swr-async-stale", breaker,
                Duration.ofMillis(50),    // freshFor — short so we age past it
                Duration.ofSeconds(30));  // staleFor — wide so the test doesn't race eviction

        cache.put("k", "v0");
        Thread.sleep(100);  // age past freshFor

        AtomicInteger loaderCalls = new AtomicInteger();
        CountDownLatch refreshLanded = new CountDownLatch(1);
        long readStartNs = System.nanoTime();
        CompletableFuture<String> f = cache.retrieve("k", () -> {
            int n = loaderCalls.incrementAndGet();
            refreshLanded.countDown();
            return CompletableFuture.completedFuture("v" + n);
        });
        // The async stale-window read must return synchronously with the
        // stale value — the refresh runs on the executor.
        assertThat(f.get(2, TimeUnit.SECONDS)).isEqualTo("v0");
        long readDurationNs = System.nanoTime() - readStartNs;
        // Read returned with the stale value well before the loader
        // completed (the refresh is asynchronous). The stale-return
        // should land in << 100ms even on a slow CI box.
        assertThat(readDurationNs).isLessThan(Duration.ofMillis(500).toNanos());

        assertThat(refreshLanded.await(2, TimeUnit.SECONDS)).isTrue();
        // Wait for the refresh metric to tick.
        await(() -> refreshesCompleted() == 1);
        assertThat(staleReturns()).isEqualTo(1);
        assertThat(refreshesCompleted()).isEqualTo(1);
    }

    @Test
    void asyncFreshRead_noRefreshDispatched() throws Exception {
        CircuitBreaker breaker = defaultBreaker();
        NearCache cache = newSwrAsyncNearCache("swr-async-fresh", breaker,
                Duration.ofMinutes(5),
                Duration.ofMinutes(10));
        cache.put("k", "v0");

        AtomicInteger loaderCalls = new AtomicInteger();
        CompletableFuture<String> f = cache.retrieve("k", () -> {
            loaderCalls.incrementAndGet();
            return CompletableFuture.completedFuture("should-not-call");
        });
        assertThat(f.get(2, TimeUnit.SECONDS)).isEqualTo("v0");
        assertThat(loaderCalls.get()).isZero();
        assertThat(staleReturns()).isZero();
        assertThat(refreshesStarted()).isZero();
    }

    private NearCache newSwrAsyncNearCache(String name, CircuitBreaker breaker,
                                            Duration freshFor, Duration staleFor) {
        io.github.nwwarm.hybridcache.config.CacheProperties.CacheSpec spec =
                new io.github.nwwarm.hybridcache.config.CacheProperties.CacheSpec(
                        io.github.nwwarm.hybridcache.config.CacheProperties.Tier.NEAR_CACHE,
                        Duration.ofMinutes(10), 10_000,
                        Duration.ofSeconds(2), Duration.ofSeconds(10),
                        io.github.nwwarm.hybridcache.config.CacheProperties.Codec.JSON,
                        null, null, null, 0.0, null, null, null,
                        new io.github.nwwarm.hybridcache.config.CacheProperties.Swr(freshFor, staleFor),
                        null);

        SwrSidecar sidecar = new SwrSidecar(name, freshFor.toNanos(),
                refreshExecutor, breaker, meterRegistry);

        com.github.benmanes.caffeine.cache.Cache<Object, Object> caffeineNative =
                com.github.benmanes.caffeine.cache.Caffeine.newBuilder()
                        .expireAfterWrite(staleFor)
                        .maximumSize(spec.maximumSize())
                        .recordStats()
                        .executor(Runnable::run)
                        .removalListener((k, v, cause) -> {
                            if (k != null
                                    && cause != com.github.benmanes.caffeine.cache.RemovalCause.REPLACED) {
                                sidecar.onRemoval(k.toString());
                            }
                        })
                        .build();
        org.springframework.cache.caffeine.CaffeineCache springCache =
                new org.springframework.cache.caffeine.CaffeineCache(name, caffeineNative, true);
        io.github.nwwarm.hybridcache.invalidation.InvalidationDispatcher dispatcher =
                new io.github.nwwarm.hybridcache.invalidation.InvalidationDispatcher(
                        redisson, "node-" + System.nanoTime(), meterRegistry);
        return new NearCache(springCache, spec, resolveTestCodec(spec.codec()),
                redisson, breaker, dispatcher, meterRegistry,
                new KeyLogFormatter(false, "test"), sidecar, null, null,
                refreshExecutor.asExecutor());
    }

    private double staleReturns() {
        return meterRegistry.find("cache.swr.stale_returns").counter().count();
    }

    private double refreshesStarted() {
        return meterRegistry.find("cache.swr.refreshes")
                .tag("status", "started").counter().count();
    }

    private double refreshesCompleted() {
        return meterRegistry.find("cache.swr.refreshes")
                .tag("status", "completed").counter().count();
    }

    private static void await(java.util.function.BooleanSupplier cond) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadline) {
            if (cond.getAsBoolean()) return;
            Thread.sleep(20);
        }
        throw new AssertionError("condition not met within 5s");
    }
}
