package io.github.nwwarm.hybridcache.core;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.cache.caffeine.CaffeineCache;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SWR integration on the {@link LocalOnlyCache} tier. No Redis dependency;
 * exercises the read-path branching, refresh dispatch, and the
 * RemovalListener bridge end-to-end on a real Caffeine cache.
 *
 * <p>The fresh window is set to a short duration (1ms here) so tests can
 * land in the stale window deterministically by sleeping past it. The
 * stale-for / Caffeine expireAfterWrite is set wide (1m) so physical
 * eviction does not fire mid-test — the goal is to test the
 * fresh→stale→refresh transition, not the past-stale-for fallthrough,
 * which is just "no L1 entry" and is covered separately.
 */
class LocalOnlyCacheSwrTest {

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
    void freshRead_returnsFresh_noRefreshDispatched() throws Exception {
        SwrSidecar sidecar = new SwrSidecar("c", Duration.ofMinutes(5).toNanos(),
                refreshExecutor, null, meterRegistry);
        LocalOnlyCache cache = build(sidecar, Duration.ofMinutes(5));
        AtomicInteger loaderCalls = new AtomicInteger();

        cache.put("k", "v0");
        // Read inside the fresh window — loader must not be invoked, no
        // refresh dispatch, no stale-return increment.
        String got = cache.get("k", () -> {
            loaderCalls.incrementAndGet();
            return "loader-v";
        });
        assertThat(got).isEqualTo("v0");
        assertThat(loaderCalls.get()).isZero();
        assertThat(staleReturns()).isZero();
        assertThat(refreshesStarted()).isZero();
    }

    @Test
    void staleRead_returnsStale_dispatchesRefreshOnce() throws Exception {
        SwrSidecar sidecar = new SwrSidecar("c", Duration.ofMillis(1).toNanos(),
                refreshExecutor, null, meterRegistry);
        LocalOnlyCache cache = build(sidecar, Duration.ofMinutes(1));

        AtomicInteger loaderCalls = new AtomicInteger();
        CountDownLatch refreshLanded = new CountDownLatch(1);

        cache.put("k", "v0");
        Thread.sleep(20);  // past freshFor=1ms

        // Stale-window read — return synchronously with the stale value
        // AND dispatch one async refresh.
        String got = cache.get("k", () -> {
            int n = loaderCalls.incrementAndGet();
            refreshLanded.countDown();
            return "v" + n;
        });
        assertThat(got).isEqualTo("v0");  // stale value returned
        assertThat(refreshLanded.await(2, TimeUnit.SECONDS)).isTrue();
        // Wait for the refresh to land in the cache. Use plain get(key)
        // (no loader) so the poll itself does not dispatch additional
        // refreshes — calling get(key, loader) on a still-stale entry
        // would race the in-flight check and could double the refresh
        // count depending on timing.
        await(() -> {
            org.springframework.cache.Cache.ValueWrapper w = cache.get("k");
            return w != null && "v1".equals(w.get());
        });
        assertThat(loaderCalls.get()).isEqualTo(1);
        assertThat(staleReturns()).isEqualTo(1);
        assertThat(refreshesCompleted()).isEqualTo(1);
    }

    @Test
    void pastStaleFor_caffeineEvicts_loaderRunsSynchronously() throws Exception {
        // staleFor = 50ms; sleeping 100ms guarantees Caffeine has physically
        // evicted the entry. The next read sees no L1, so we fall through to
        // the loader on the synchronous path — not an async refresh.
        SwrSidecar sidecar = new SwrSidecar("c", Duration.ofMillis(10).toNanos(),
                refreshExecutor, null, meterRegistry);
        LocalOnlyCache cache = build(sidecar, Duration.ofMillis(50));

        cache.put("k", "v0");
        Thread.sleep(100);

        AtomicInteger loaderCalls = new AtomicInteger();
        String got = cache.get("k", () -> {
            loaderCalls.incrementAndGet();
            return "fresh-v";
        });
        assertThat(got).isEqualTo("fresh-v");
        // Loader ran synchronously — no async refresh involved.
        assertThat(loaderCalls.get()).isEqualTo(1);
        assertThat(staleReturns()).isZero();
        assertThat(refreshesStarted()).isZero();
    }

    @Test
    void put_recordsSidecarBeforeValueVisible() {
        SwrSidecar sidecar = new SwrSidecar("c", Duration.ofMinutes(5).toNanos(),
                refreshExecutor, null, meterRegistry);
        LocalOnlyCache cache = build(sidecar, Duration.ofMinutes(5));

        cache.put("k", "v");
        // After put, the sidecar entry exists and classifies as FRESH.
        assertThat(sidecar.sidecarSize()).isEqualTo(1);
        assertThat(sidecar.classify("k")).isEqualTo(SwrSidecar.Classification.FRESH);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private LocalOnlyCache build(SwrSidecar sidecar, Duration staleFor) {
        // Wire the same RemovalListener composition HybridCacheManager uses:
        // skip REPLACED, call sidecar.onRemoval for everything else.
        com.github.benmanes.caffeine.cache.Cache<Object, Object> native_ = Caffeine.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(staleFor)
                .executor(Runnable::run)
                .removalListener((k, v, cause) -> {
                    if (k != null && cause != RemovalCause.REPLACED) {
                        sidecar.onRemoval(k.toString());
                    }
                })
                .build();
        CaffeineCache delegate = new CaffeineCache("c", native_, true);
        return new LocalOnlyCache(delegate, null, null, meterRegistry, sidecar);
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
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (System.nanoTime() < deadline) {
            if (cond.getAsBoolean()) return;
            Thread.sleep(5);
        }
        throw new AssertionError("condition not met within 2s");
    }
}
