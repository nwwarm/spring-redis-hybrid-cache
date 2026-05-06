package io.github.nwwarm;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.search.Search;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.springframework.cache.caffeine.CaffeineCache;
import org.springframework.cache.support.NullValue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Per-cache concurrency cap on loader calls.
 *
 * <p>Configured via {@code cache.specs.<name>.max-concurrent-loaders}; the
 * acquire timeout uses {@code cache.specs.<name>.loader-acquire-timeout}
 * (defaults to {@code lock-wait} when unset). The cap protects the source of
 * truth in two scenarios where the distributed lock can't:
 *
 * <ul>
 *   <li>Redis is unreachable / the breaker is open — the headline case.</li>
 *   <li>{@code LOCAL_ONLY} caches, which have no cross-node coordination by
 *       design.</li>
 * </ul>
 *
 * <p>It also serves as defense-in-depth in normal operation.
 */
class LoaderConcurrencyCapIT extends RedisTestBase {

    private RedissonClient redisson;

    @BeforeEach
    void setUp() {
        redisson = newRedisson();
    }

    @AfterEach
    void tearDown() {
        if (redisson != null && !redisson.isShutdown()) redisson.shutdown();
        // Defensive: a paused Redis would break sibling tests. Unpause if
        // any test paused the shared container and then failed mid-way.
        if (REDIS.isRunning()) {
            try { REDIS.getDockerClient().unpauseContainerCmd(REDIS.getContainerId()).exec(); }
            catch (Exception ignored) { /* not paused */ }
        }
    }

    // ---------- Test 1: cap holds under load ----------

    @Test
    void nearCache_capLimitsPeakLoaderConcurrency() throws Exception {
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        NearCache cache = buildNearCache("cap-near-" + System.nanoTime(),
                /*max=*/ 2, /*acquire=*/ Duration.ofSeconds(30), meters);

        try {
            ConcurrencyTracker tracker = new ConcurrencyTracker();
            int requests = 10;
            ExecutorService pool = Executors.newFixedThreadPool(requests);
            CountDownLatch ready = new CountDownLatch(requests);
            CountDownLatch go = new CountDownLatch(1);

            try {
                List<Future<Object>> futures = new ArrayList<>();
                for (int i = 0; i < requests; i++) {
                    final int n = i;
                    futures.add(pool.submit(() -> {
                        ready.countDown();
                        go.await();
                        return cache.get("k-" + n, () -> tracker.runSlow(75));
                    }));
                }
                ready.await(5, TimeUnit.SECONDS);
                go.countDown();
                for (Future<Object> f : futures) f.get(15, TimeUnit.SECONDS);
            } finally {
                pool.shutdownNow();
            }

            assertThat(tracker.peak()).as("peak concurrent loaders").isLessThanOrEqualTo(2);
            assertThat(tracker.totalCalls()).isEqualTo(requests);
            assertThat(rejectionCount(meters, cache.getName())).isEqualTo(0.0);
        } finally {
            cache.shutdown();
        }
    }

    // ---------- Test 2: rejection thrown as the typed exception ----------

    @Test
    void nearCache_rejectsWhenAcquireTimesOut_typedException() throws Exception {
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        // Slow loaders + tiny acquire timeout: most requests will wait briefly,
        // fail to get a permit, and surface LoaderRejectedException directly.
        NearCache cache = buildNearCache("cap-rej-" + System.nanoTime(),
                /*max=*/ 2, /*acquire=*/ Duration.ofMillis(50), meters);

        try {
            int requests = 10;
            ExecutorService pool = Executors.newFixedThreadPool(requests);
            CountDownLatch ready = new CountDownLatch(requests);
            CountDownLatch go = new CountDownLatch(1);

            List<Future<Object>> futures = new ArrayList<>();
            try {
                for (int i = 0; i < requests; i++) {
                    final int n = i;
                    futures.add(pool.submit(() -> {
                        ready.countDown();
                        go.await();
                        return cache.get("rej-" + n, () -> {
                            // Slow enough that subsequent requests time out
                            // waiting for a permit.
                            Thread.sleep(500);
                            return "v";
                        });
                    }));
                }
                ready.await(5, TimeUnit.SECONDS);
                go.countDown();

                int rejections = 0;
                int succeeded = 0;
                for (Future<Object> f : futures) {
                    try {
                        f.get(15, TimeUnit.SECONDS);
                        succeeded++;
                    } catch (java.util.concurrent.ExecutionException ee) {
                        // Critical: the exact type must reach the caller.
                        // If a CompletionException wraps it, the unwrap is
                        // broken and this assertion catches it.
                        Throwable inner = ee.getCause();
                        assertThat(inner)
                                .as("exception class reaching the caller")
                                .isInstanceOf(LoaderRejectedException.class);
                        // And distinct from Spring's loader-failure type.
                        assertThat(inner)
                                .isNotInstanceOf(org.springframework.cache.Cache.ValueRetrievalException.class);
                        rejections++;
                    }
                }

                assertThat(rejections).as("at least some requests rejected").isGreaterThan(0);
                assertThat(succeeded).as("at least the cap-many requests succeed").isGreaterThanOrEqualTo(2);
                assertThat(rejectionCount(meters, cache.getName()))
                        .as("rejection counter increments per rejection")
                        .isEqualTo((double) rejections);
            } finally {
                pool.shutdownNow();
            }
        } finally {
            cache.shutdown();
        }
    }

    // ---------- Test 3: backward compat — no config, no overhead ----------

    @Test
    void nearCache_unconfigured_behavesAsBefore_andCachesValue() {
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        NearCache cache = buildNearCache("cap-unset-" + System.nanoTime(),
                /*max=*/ null, /*acquire=*/ null, meters);

        try {
            AtomicInteger calls = new AtomicInteger();
            String v1 = cache.get("k", () -> { calls.incrementAndGet(); return "v"; });
            String v2 = cache.get("k", () -> { calls.incrementAndGet(); return "v"; });
            assertThat(v1).isEqualTo("v");
            assertThat(v2).isEqualTo("v");
            assertThat(calls.get()).as("loader runs once; cache served the second call").isEqualTo(1);

            // No metrics registered when unconfigured.
            assertThat(Search.in(meters).name("cache.loaders.permits.available").gauge())
                    .as("no permit gauge when cap unset").isNull();
            assertThat(Search.in(meters).name("cache.loaders.rejections").counter())
                    .as("no rejection counter when cap unset").isNull();
        } finally {
            cache.shutdown();
        }
    }

    // ---------- Test 4: Redis down / breaker open — headline scenario ----------

    @Test
    void nearCache_breakerOpen_capStillProtectsSource() throws Exception {
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        // Use a fast-fail breaker + fail-fast Redisson so the breaker actually
        // opens during the test, simulating the "Redis down" runtime state.
        RedissonClient ff = newFailFastRedisson();
        CircuitBreaker breaker = fastFailBreaker();

        NearCache cache = buildNearCache("cap-breaker-" + System.nanoTime(),
                /*max=*/ 2, /*acquire=*/ Duration.ofSeconds(30),
                meters, ff, breaker, Duration.ofMillis(50)); // tiny lockWait so lock falls through fast

        try {
            // Drive Redis into "unreachable" via container pause.
            REDIS.getDockerClient().pauseContainerCmd(REDIS.getContainerId()).exec();
            // Trip the breaker: a few cold reads against paused Redis are
            // recorded as slow/failed and the breaker transitions to OPEN.
            for (int i = 0; i < 8; i++) cache.get("warmup-" + i);
            Awaitility.await().atMost(Duration.ofSeconds(8)).untilAsserted(() ->
                    assertThat(breaker.getState())
                            .isIn(CircuitBreaker.State.OPEN, CircuitBreaker.State.FORCED_OPEN));

            // With the breaker open AND Redis paused, distributed lock
            // acquisition fails (no Redis call permitted). The semaphore is
            // the only thing standing between concurrent threads and the
            // source. Verify the cap holds.
            ConcurrencyTracker tracker = new ConcurrencyTracker();
            int requests = 10;
            ExecutorService pool = Executors.newFixedThreadPool(requests);
            CountDownLatch ready = new CountDownLatch(requests);
            CountDownLatch go = new CountDownLatch(1);

            try {
                List<Future<Object>> futures = new ArrayList<>();
                for (int i = 0; i < requests; i++) {
                    final int n = i;
                    futures.add(pool.submit(() -> {
                        ready.countDown();
                        go.await();
                        return cache.get("breaker-k-" + n, () -> tracker.runSlow(75));
                    }));
                }
                ready.await(5, TimeUnit.SECONDS);
                go.countDown();
                for (Future<Object> f : futures) f.get(20, TimeUnit.SECONDS);
            } finally {
                pool.shutdownNow();
            }

            assertThat(tracker.peak())
                    .as("semaphore must cap loader concurrency even with breaker open")
                    .isLessThanOrEqualTo(2);
        } finally {
            // Always unpause before shutting down so the cleanup path doesn't hang.
            try { REDIS.getDockerClient().unpauseContainerCmd(REDIS.getContainerId()).exec(); }
            catch (Exception ignored) { /* may already be unpaused */ }
            cache.shutdown();
            ff.shutdown();
        }
    }

    // ---------- Test 5: LOCAL_ONLY tier ----------

    @Test
    void localOnly_capLimitsPerJvmStampede() throws Exception {
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        LocalOnlyCache cache = buildLocalCache("cap-local-" + System.nanoTime(),
                /*max=*/ 2, /*acquire=*/ Duration.ofSeconds(30), meters);

        ConcurrencyTracker tracker = new ConcurrencyTracker();
        int requests = 10;
        ExecutorService pool = Executors.newFixedThreadPool(requests);
        CountDownLatch ready = new CountDownLatch(requests);
        CountDownLatch go = new CountDownLatch(1);

        try {
            List<Future<Object>> futures = new ArrayList<>();
            for (int i = 0; i < requests; i++) {
                final int n = i;
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    go.await();
                    return cache.get("k-" + n, () -> tracker.runSlow(75));
                }));
            }
            ready.await(5, TimeUnit.SECONDS);
            go.countDown();
            for (Future<Object> f : futures) f.get(15, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }

        assertThat(tracker.peak()).as("LOCAL_ONLY peak loader concurrency")
                .isLessThanOrEqualTo(2);
    }

    // ---------- Test 5b: rejection-not-cached — LOCAL_ONLY ----------

    @Test
    void localOnly_rejectionIsNotCached_subsequentCallSucceeds() throws Exception {
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        // max=1, very short acquire timeout. Block the single permit by
        // running a slow loader in another thread, then a second request
        // should be rejected. Once the slow loader completes and releases,
        // a third request on the same key must run the loader and return
        // a value — the rejection must not have been cached.
        LocalOnlyCache cache = buildLocalCache("cap-local-rej-" + System.nanoTime(),
                /*max=*/ 1, /*acquire=*/ Duration.ofMillis(50), meters);

        CountDownLatch slowStarted = new CountDownLatch(1);
        CountDownLatch slowRelease = new CountDownLatch(1);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<Object> blocker = pool.submit(() -> cache.get("k", () -> {
                slowStarted.countDown();
                slowRelease.await();   // hold the permit
                return "v-from-blocker";
            }));
            assertThat(slowStarted.await(5, TimeUnit.SECONDS)).isTrue();

            // Permit unavailable; this acquire times out and rejects.
            assertThatThrownBy(() ->
                    cache.get("rej-k", () -> "would-be"))
                    .as("rejected request surfaces typed exception")
                    .isInstanceOf(LoaderRejectedException.class);

            // Release the blocker so the permit returns.
            slowRelease.countDown();
            assertThat(blocker.get(5, TimeUnit.SECONDS)).isEqualTo("v-from-blocker");

            // Now the same key that was rejected must succeed — Caffeine does
            // NOT cache exceptions from compute, so the next call retries.
            String retried = cache.get("rej-k", () -> "v-after");
            assertThat(retried)
                    .as("rejection must not be cached; retry runs loader")
                    .isEqualTo("v-after");
        } finally {
            pool.shutdownNow();
        }
    }

    // ---------- Test 5c: rejection-not-cached — NearCache ----------

    @Test
    void nearCache_rejectionIsNotCached_subsequentCallSucceeds() throws Exception {
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        NearCache cache = buildNearCache("cap-near-rej-" + System.nanoTime(),
                /*max=*/ 1, /*acquire=*/ Duration.ofMillis(50), meters);

        CountDownLatch slowStarted = new CountDownLatch(1);
        CountDownLatch slowRelease = new CountDownLatch(1);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<Object> blocker = pool.submit(() -> cache.get("k", () -> {
                slowStarted.countDown();
                slowRelease.await();
                return "v-from-blocker";
            }));
            assertThat(slowStarted.await(5, TimeUnit.SECONDS)).isTrue();

            assertThatThrownBy(() ->
                    cache.get("rej-k", () -> "would-be"))
                    .as("rejected request surfaces typed exception")
                    .isInstanceOf(LoaderRejectedException.class);

            slowRelease.countDown();
            assertThat(blocker.get(5, TimeUnit.SECONDS)).isEqualTo("v-from-blocker");

            // Caffeine's Cache.get(K, Function) does not cache exceptions
            // from the mapping function, so the rejected key has no cached
            // result; the retry runs the loader successfully.
            String retried = cache.get("rej-k", () -> "v-after");
            assertThat(retried)
                    .as("rejection must not be cached; retry runs loader")
                    .isEqualTo("v-after");
        } finally {
            pool.shutdownNow();
            cache.shutdown();
        }
    }

    // ---------- helpers ----------

    /**
     * Counts loader concurrency by atomic enter/exit. Loader sleeps so
     * concurrent calls actually overlap — without the sleep, peak would
     * underestimate.
     */
    private static final class ConcurrencyTracker {
        private final AtomicInteger inFlight = new AtomicInteger();
        private final AtomicInteger peak = new AtomicInteger();
        private final AtomicInteger total = new AtomicInteger();

        Object runSlow(long sleepMs) throws InterruptedException {
            int now = inFlight.incrementAndGet();
            peak.accumulateAndGet(now, Math::max);
            total.incrementAndGet();
            try {
                Thread.sleep(sleepMs);
                return "v";
            } finally {
                inFlight.decrementAndGet();
            }
        }

        int peak() { return peak.get(); }
        int totalCalls() { return total.get(); }
    }

    private static double rejectionCount(MeterRegistry registry, String cacheName) {
        var counter = Search.in(registry)
                .name("cache.loaders.rejections")
                .tag("cache", cacheName)
                .counter();
        return counter == null ? 0.0 : counter.count();
    }

    private NearCache buildNearCache(String name,
                                     Integer maxConcurrentLoaders,
                                     Duration loaderAcquireTimeout,
                                     MeterRegistry meters) {
        return buildNearCache(name, maxConcurrentLoaders, loaderAcquireTimeout,
                meters, redisson, defaultBreaker(), Duration.ofSeconds(2));
    }

    private NearCache buildNearCache(String name,
                                     Integer maxConcurrentLoaders,
                                     Duration loaderAcquireTimeout,
                                     MeterRegistry meters,
                                     RedissonClient client,
                                     CircuitBreaker breaker,
                                     Duration lockWait) {
        CacheProperties.CacheSpec spec = new CacheProperties.CacheSpec(
                CacheProperties.Tier.NEAR_CACHE,
                Duration.ofMinutes(10), 10_000,
                lockWait, Duration.ofSeconds(10),
                CacheProperties.Codec.JSON, null,
                maxConcurrentLoaders, loaderAcquireTimeout, 0.0, null);
        com.github.benmanes.caffeine.cache.Cache<Object, Object> nativeCache = Caffeine.newBuilder()
                .expireAfterWrite(spec.ttl())
                .maximumSize(spec.maximumSize())
                .recordStats()
                .build();
        CaffeineCache springCache = new CaffeineCache(name, nativeCache, true);
        InvalidationDispatcher dispatcher = new InvalidationDispatcher(client, "node-" + name, meters);
        return new NearCache(springCache, spec, null, client, breaker, dispatcher,
                meters, new KeyLogFormatter(false, "test"));
    }

    private LocalOnlyCache buildLocalCache(String name,
                                           Integer maxConcurrentLoaders,
                                           Duration loaderAcquireTimeout,
                                           MeterRegistry meters) {
        com.github.benmanes.caffeine.cache.Cache<Object, Object> nativeCache = Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofMinutes(10))
                .maximumSize(10_000)
                .recordStats()
                .build();
        CaffeineCache springCache = new CaffeineCache(name, nativeCache, true);
        return new LocalOnlyCache(springCache, maxConcurrentLoaders, loaderAcquireTimeout, meters);
    }

    // Suppress unused import warning on NullValue (left available for future
    // negative-cache tests on this surface; not used in the current suite).
    @SuppressWarnings("unused")
    private static final Class<?> KEEP_NULLVALUE = NullValue.class;
}
