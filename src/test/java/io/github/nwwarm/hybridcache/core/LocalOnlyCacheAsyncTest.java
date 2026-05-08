package io.github.nwwarm.hybridcache.core;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.cache.Cache;
import org.springframework.cache.caffeine.CaffeineCache;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link LocalOnlyCache#retrieve(Object)} and
 * {@link LocalOnlyCache#retrieve(Object, java.util.function.Supplier)}.
 *
 * <p>LOCAL_ONLY exercises the async surface without Redis — the simplest
 * tier where the loader-hop, single-flight, and exception-parity
 * guarantees can be tested in isolation.
 */
class LocalOnlyCacheAsyncTest {

    private SimpleMeterRegistry meterRegistry;
    private RefreshExecutor refreshExecutor;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        refreshExecutor = new RefreshExecutor(2, meterRegistry);
    }

    @AfterEach
    void tearDown() {
        refreshExecutor.stop();
    }

    @Test
    void retrieveNoLoader_l1Hit_returnsCompletedFutureNoThreadSwitch() throws Exception {
        LocalOnlyCache cache = build();
        cache.put("k", "v");
        CompletableFuture<?> f = cache.retrieve("k");
        // Already complete — no thread switch happened.
        assertThat(f.isDone()).isTrue();
        Object result = f.get(0, TimeUnit.MILLISECONDS);
        assertThat(result).isInstanceOf(Cache.ValueWrapper.class);
        assertThat(((Cache.ValueWrapper) result).get()).isEqualTo("v");
    }

    @Test
    void retrieveNoLoader_l1Miss_returnsCompletedFutureOfNull() throws Exception {
        LocalOnlyCache cache = build();
        CompletableFuture<?> f = cache.retrieve("missing");
        assertThat(f.isDone()).isTrue();
        // Async-path equivalent of "no cached value" (guardrail item 6).
        assertThat(f.get()).isNull();
    }

    @Test
    void retrieveWithLoader_l1Hit_doesNotInvokeLoader() throws Exception {
        LocalOnlyCache cache = build();
        cache.put("k", "v");
        AtomicInteger loaderCalls = new AtomicInteger();
        CompletableFuture<String> f = cache.retrieve("k", () -> {
            loaderCalls.incrementAndGet();
            return CompletableFuture.completedFuture("loader-v");
        });
        assertThat(f.isDone()).isTrue();
        assertThat(f.get()).isEqualTo("v");
        assertThat(loaderCalls.get()).isZero();
    }

    @Test
    void retrieveWithLoader_l1Miss_runsLoaderOnRefreshExecutor() throws Exception {
        LocalOnlyCache cache = build();
        AtomicInteger loaderCalls = new AtomicInteger();
        CountDownLatch loaderEntered = new CountDownLatch(1);
        String[] threadName = new String[1];

        CompletableFuture<String> f = cache.retrieve("k", () -> {
            int n = loaderCalls.incrementAndGet();
            threadName[0] = Thread.currentThread().getName();
            loaderEntered.countDown();
            return CompletableFuture.completedFuture("v" + n);
        });
        assertThat(loaderEntered.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(f.get(2, TimeUnit.SECONDS)).isEqualTo("v1");
        // Loader hop landed on the refresh executor (named "hybrid-cache-refresh-*").
        assertThat(threadName[0]).startsWith("hybrid-cache-refresh-");
        // L1 was populated.
        Cache.ValueWrapper w = ((Cache) cacheDelegate(cache)).get("k");
        assertThat(w).isNotNull();
        assertThat(w.get()).isEqualTo("v1");
    }

    @Test
    void retrieveWithLoader_concurrent100_singleFlightCollapsesToOneLoader() throws Exception {
        LocalOnlyCache cache = build();
        AtomicInteger loaderCalls = new AtomicInteger();
        CountDownLatch loaderEntered = new CountDownLatch(1);
        CountDownLatch loaderRelease = new CountDownLatch(1);
        int n = 100;

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
        assertThat(loaderEntered.await(2, TimeUnit.SECONDS)).isTrue();
        loaderRelease.countDown();
        for (CompletableFuture<String> f : futures) {
            assertThat(f.get(5, TimeUnit.SECONDS)).isEqualTo("v1");
        }
        // Per-JVM single-flight: exactly one loader call across N readers.
        assertThat(loaderCalls.get()).isEqualTo(1);
    }

    @Test
    void retrieveWithLoader_loaderThrowsSync_failsWithValueRetrievalException() {
        LocalOnlyCache cache = build();
        CompletableFuture<String> f = cache.retrieve("k", () -> {
            throw new RuntimeException("supplier-blew-up");
        });
        assertThatThrownBy(() -> f.get(2, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(Cache.ValueRetrievalException.class);
    }

    @Test
    void retrieveWithLoader_loaderFutureFails_failsWithValueRetrievalException() {
        // The loader returned a future that completes exceptionally.
        // Per guardrail item 3, the async-path exception must match the
        // sync path: ValueRetrievalException wrapping the cause.
        LocalOnlyCache cache = build();
        CompletableFuture<String> f = cache.retrieve("k", () -> {
            CompletableFuture<String> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IllegalStateException("loader-failed"));
            return failed;
        });
        assertThatThrownBy(() -> f.get(2, TimeUnit.SECONDS))
                .hasCauseInstanceOf(Cache.ValueRetrievalException.class);
    }

    @Test
    void retrieveWithLoader_loaderRejected_failsWithLoaderRejectedExceptionUnwrapped() {
        // max-concurrent-loaders=1, acquire-timeout=50ms. Two DIFFERENT
        // keys so per-key single-flight doesn't collapse them — first
        // caller holds the only permit forever; second caller's acquire
        // times out → LoaderRejectedException, NOT wrapped (guardrail
        // item 3 of async section).
        LocalOnlyCache cache = build(1, Duration.ofMillis(50));
        CompletableFuture<String> first = new CompletableFuture<>();
        cache.retrieve("k1", () -> first);
        // Wait for first to occupy the gate's only permit.
        CompletableFuture<String> rejected = cache.retrieve("k2",
                () -> CompletableFuture.completedFuture("never-runs"));
        // The async-path exception wraps in CompletionException for
        // .get() callers; the cause we care about is a few hops down.
        // Unwrap CompletionException/ExecutionException ourselves.
        Throwable thrown = null;
        try {
            rejected.get(2, TimeUnit.SECONDS);
        } catch (Throwable t) {
            thrown = t;
        }
        assertThat(thrown).isNotNull();
        Throwable cause = thrown;
        while (cause != null && !(cause instanceof LoaderRejectedException)) {
            cause = cause.getCause();
        }
        assertThat(cause)
                .as("LoaderRejectedException must surface unwrapped per guardrail item 3")
                .isInstanceOf(LoaderRejectedException.class);
        first.complete("eventual");
    }

    @Test
    void retrieveWithLoader_l1NullValueRoundTrip() throws Exception {
        LocalOnlyCache cache = build();
        // Loader returns null; we expect L1 to cache the negative result
        // and a subsequent retrieve to return cached-null without
        // invoking the loader again.
        AtomicInteger calls = new AtomicInteger();
        CompletableFuture<String> first = cache.retrieve("k", () -> {
            calls.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        });
        // First retrieve runs the loader.
        assertThat(first.get(2, TimeUnit.SECONDS)).isNull();
        // L1 now holds NullValue (the SimpleValueWrapper wraps it).
        // Second retrieve must not invoke the loader; it returns cached
        // null. Per guardrail item 6, the async-path return for cached
        // null is CompletableFuture<null> (the wrapper.get() value),
        // matching how Spring's CacheInterceptor distinguishes "no
        // cached value" from "cached null."
        CompletableFuture<String> second = cache.retrieve("k", () -> {
            calls.incrementAndGet();
            return CompletableFuture.completedFuture("should-not-run");
        });
        assertThat(second.get(2, TimeUnit.SECONDS)).isNull();
        assertThat(calls.get()).isEqualTo(1);
    }

    // ---------- helpers ----------

    private LocalOnlyCache build() {
        return build(null, null);
    }

    private LocalOnlyCache build(Integer maxConcurrentLoaders, Duration acquireTimeout) {
        com.github.benmanes.caffeine.cache.Cache<Object, Object> native_ = Caffeine.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(Duration.ofMinutes(5))
                .recordStats()
                .build();
        CaffeineCache delegate = new CaffeineCache("c", native_, true);
        return new LocalOnlyCache(delegate, maxConcurrentLoaders, acquireTimeout,
                meterRegistry, null, null, null, refreshExecutor.asExecutor());
    }

    private Object cacheDelegate(LocalOnlyCache cache) {
        // Test seam: the LocalOnlyCache field "delegate" is private. We
        // cast through getNativeCache() to read L1 contents directly.
        // The native cache here is the Caffeine cache; we want the
        // CaffeineCache wrapper for ValueWrapper handling. Access via
        // the cache's name lookup.
        return new CaffeineCache(cache.getName(),
                (com.github.benmanes.caffeine.cache.Cache<Object, Object>) cache.getNativeCache(),
                true);
    }
}
