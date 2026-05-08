package io.github.nwwarm.hybridcache.core;

import io.github.nwwarm.hybridcache.testfixtures.RedisTestBase;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.springframework.cache.Cache;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link NearCache#retrieve(Object)} and
 * {@link NearCache#retrieve(Object, java.util.function.Supplier)} against
 * a real Redis (Testcontainers).
 *
 * <p>Covers the async chain end-to-end: L1 fast path, L2 hit via
 * {@code RBucket.getAsync}, cold-load via {@code tryLockAsync} +
 * loader hop, async-path metrics parity with the sync path.
 */
class NearCacheAsyncIT extends RedisTestBase {

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
    void retrieveNoLoader_l1Hit_completesSynchronouslyOnCallingThread() throws Exception {
        NearCache cache = newAsyncNearCache("near-async-l1");
        cache.put("k", "v");

        CompletableFuture<?> f = cache.retrieve("k");
        // Already done — no thread switch.
        assertThat(f.isDone()).isTrue();
        Object result = f.get(0, TimeUnit.MILLISECONDS);
        assertThat(result).isInstanceOf(Cache.ValueWrapper.class);
        assertThat(((Cache.ValueWrapper) result).get()).isEqualTo("v");
    }

    @Test
    void retrieveNoLoader_l1Miss_l2Hit_populatesL1() throws Exception {
        NearCache cache = newAsyncNearCache("near-async-l2");
        // Seed L2 via put(), then drop L1 by hand so the next read goes to L2.
        cache.put("k", "from-l2");
        ((com.github.benmanes.caffeine.cache.Cache<?, ?>) cache.getNativeCache()).invalidateAll();

        CompletableFuture<?> f = cache.retrieve("k");
        Object result = f.get(2, TimeUnit.SECONDS);
        assertThat(result).isInstanceOf(Cache.ValueWrapper.class);
        assertThat(((Cache.ValueWrapper) result).get()).isEqualTo("from-l2");
        // L1 should now hold the value.
        Cache.ValueWrapper l1 = (Cache.ValueWrapper) ((java.util.concurrent.CompletableFuture<?>)
                cache.retrieve("k")).get(0, TimeUnit.MILLISECONDS);
        assertThat(l1).isNotNull();
        assertThat(l1.get()).isEqualTo("from-l2");
    }

    @Test
    void retrieveNoLoader_l1Miss_l2Miss_returnsCompletedNull() throws Exception {
        NearCache cache = newAsyncNearCache("near-async-double-miss");
        CompletableFuture<?> f = cache.retrieve("nope");
        assertThat(f.get(2, TimeUnit.SECONDS)).isNull();
    }

    @Test
    void retrieveWithLoader_coldLoad_runsLoaderOffNetty_writesL1AndL2() throws Exception {
        NearCache cache = newAsyncNearCache("near-async-coldload");
        AtomicInteger loaderCalls = new AtomicInteger();
        String[] loaderThread = new String[1];

        CompletableFuture<String> f = cache.retrieve("k", () -> {
            loaderCalls.incrementAndGet();
            loaderThread[0] = Thread.currentThread().getName();
            return CompletableFuture.completedFuture("v0");
        });
        assertThat(f.get(5, TimeUnit.SECONDS)).isEqualTo("v0");
        assertThat(loaderCalls.get()).isEqualTo(1);
        // Guardrail item 2: loader runs on the refresh executor, NOT a
        // Redisson Netty thread. Refresh-executor thread name is
        // "hybrid-cache-refresh-*"; Netty would be "redisson-netty-*"
        // or "nioEventLoopGroup-*".
        assertThat(loaderThread[0]).startsWith("hybrid-cache-refresh-");

        // Subsequent retrieve hits L1 (no loader call).
        CompletableFuture<String> hit = cache.retrieve("k", () -> {
            loaderCalls.incrementAndGet();
            return CompletableFuture.completedFuture("should-not-run");
        });
        assertThat(hit.get(2, TimeUnit.SECONDS)).isEqualTo("v0");
        assertThat(loaderCalls.get()).isEqualTo(1);
    }

    @Test
    void retrieveWithLoader_concurrentSameKey_singleFlightOneLoaderCall() throws Exception {
        NearCache cache = newAsyncNearCache("near-async-singleflight");
        AtomicInteger loaderCalls = new AtomicInteger();
        CountDownLatch loaderEntered = new CountDownLatch(1);
        CountDownLatch loaderRelease = new CountDownLatch(1);

        int n = 50;
        @SuppressWarnings("unchecked")
        CompletableFuture<String>[] futures = new CompletableFuture[n];
        for (int i = 0; i < n; i++) {
            futures[i] = cache.retrieve("hot", () -> {
                int call = loaderCalls.incrementAndGet();
                loaderEntered.countDown();
                CompletableFuture<String> later = new CompletableFuture<>();
                CompletableFuture.runAsync(() -> {
                    try {
                        loaderRelease.await();
                    } catch (InterruptedException ignored) {}
                    later.complete("v" + call);
                });
                return later;
            });
        }
        assertThat(loaderEntered.await(5, TimeUnit.SECONDS)).isTrue();
        loaderRelease.countDown();
        for (CompletableFuture<String> f : futures) {
            assertThat(f.get(10, TimeUnit.SECONDS)).isEqualTo("v1");
        }
        assertThat(loaderCalls.get())
                .as("per-JVM single-flight collapses concurrent retrievers to one loader call")
                .isEqualTo(1);
    }

    @Test
    void retrieveWithLoader_loaderFails_failsWithValueRetrievalException() {
        NearCache cache = newAsyncNearCache("near-async-loaderfail");
        CompletableFuture<String> f = cache.retrieve("k", () -> {
            CompletableFuture<String> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IllegalStateException("loader-blew-up"));
            return failed;
        });
        Throwable thrown = null;
        try {
            f.get(5, TimeUnit.SECONDS);
        } catch (Throwable t) {
            thrown = t;
        }
        assertThat(thrown).isNotNull();
        Throwable cause = thrown;
        while (cause != null && !(cause instanceof Cache.ValueRetrievalException)) {
            cause = cause.getCause();
        }
        assertThat(cause)
                .as("async loader failure surfaces as ValueRetrievalException — guardrail item 3")
                .isInstanceOf(Cache.ValueRetrievalException.class);
    }

    @Test
    void asyncMetricsParity_withSyncPath() throws Exception {
        // The same metric tags fire on the async path. Drive both paths
        // and assert both increments land.
        NearCache cache = newAsyncNearCache("near-async-metrics");
        // Sync hit/miss
        cache.get("k", () -> "v");        // miss + load + put (async path: cold-load via async)
        cache.get("k");                    // hit
        // Async path
        cache.retrieve("k").get(2, TimeUnit.SECONDS);  // L1 hit, no L2 metric
        cache.retrieve("k2", () -> CompletableFuture.completedFuture("v2"))
                .get(5, TimeUnit.SECONDS);

        // We don't assert exact counts (the sync path's first miss also
        // hits L2 once for the recheck under the lock), only that both
        // counters fired at least once — that is, the async path emits
        // the same metric family as the sync path.
        double l2Hits = meterRegistry.find("cache.l2.gets").tag("result", "hit").counter().count();
        double l2Misses = meterRegistry.find("cache.l2.gets").tag("result", "miss").counter().count();
        assertThat(l2Misses).isPositive();
        // L2 hit counts when a read found a value; the second
        // retrieve("k2") seeds L2 under the lock, no L2 hit recorded
        // there. The sync get("k") second call is an L1 hit, so no L2
        // metric. Hits will be 0 unless a separate test re-reads after
        // population — drive that explicitly.
        cache.retrieve("k").get(2, TimeUnit.SECONDS);  // still L1 hit
        // Force an L2 hit: drop L1, then retrieve from L2.
        ((com.github.benmanes.caffeine.cache.Cache<?, ?>) cache.getNativeCache()).invalidateAll();
        cache.retrieve("k").get(2, TimeUnit.SECONDS);
        double l2HitsAfter = meterRegistry.find("cache.l2.gets")
                .tag("result", "hit").counter().count();
        assertThat(l2HitsAfter).isPositive();
    }

    private NearCache newAsyncNearCache(String name) {
        CircuitBreaker breaker = defaultBreaker();
        // Use the standard fixture but pass the refreshExecutor through
        // the full constructor. RedisTestBase.newNearCache uses a
        // constructor that doesn't carry refreshExecutor, so we extend
        // that helper inline here.
        com.github.benmanes.caffeine.cache.Cache<Object, Object> caffeineNative =
                com.github.benmanes.caffeine.cache.Caffeine.newBuilder()
                        .expireAfterWrite(Duration.ofMinutes(10))
                        .maximumSize(10_000)
                        .recordStats()
                        .build();
        org.springframework.cache.caffeine.CaffeineCache springCache =
                new org.springframework.cache.caffeine.CaffeineCache(name, caffeineNative, true);
        io.github.nwwarm.hybridcache.invalidation.InvalidationDispatcher dispatcher =
                new io.github.nwwarm.hybridcache.invalidation.InvalidationDispatcher(
                        redisson, "node-" + System.nanoTime(), meterRegistry);
        io.github.nwwarm.hybridcache.config.CacheProperties.CacheSpec spec =
                new io.github.nwwarm.hybridcache.config.CacheProperties.CacheSpec(
                        io.github.nwwarm.hybridcache.config.CacheProperties.Tier.NEAR_CACHE,
                        Duration.ofMinutes(10), 10_000,
                        Duration.ofSeconds(2), Duration.ofSeconds(10),
                        io.github.nwwarm.hybridcache.config.CacheProperties.Codec.JSON,
                        null, null, null, 0.0, null, null, null, null, null);
        return new NearCache(
                springCache, spec, resolveTestCodec(spec.codec()), redisson,
                breaker, dispatcher, meterRegistry,
                new KeyLogFormatter(false, "test"),
                null, null, null, refreshExecutor.asExecutor());
    }
}
