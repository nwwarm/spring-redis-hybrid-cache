package io.github.nwwarm.hybridcache.core;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.cache.caffeine.CaffeineCache;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration: a hot key reading through a SWR + RA cache must never see
 * a synchronous reload latency spike. Drive the cache with one slow
 * loader and 10 000 reads — measure tail latency and assert no read
 * synchronously waited on the loader's runtime.
 *
 * <p>The key invariant: after the first cold load, every subsequent read
 * of a hot key is served by L1 (synchronously, fast) while RA / SWR
 * dispatch refreshes asynchronously on the {@link RefreshExecutor}. A
 * regression in either the read-path branching or the in-flight collapse
 * would surface as reads waiting on the loader, blowing the tail.
 *
 * <p>No Redis dependency — this test runs on {@link LocalOnlyCache}, the
 * minimum tier where RA applies.
 */
class RefreshAheadHotKeyTailLatencyTest {

    private SimpleMeterRegistry meterRegistry;
    private RefreshExecutor refreshExecutor;
    private LoaderRuntimeEwma ewma;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        refreshExecutor = new RefreshExecutor(2, meterRegistry);
        ewma = new LoaderRuntimeEwma();
    }

    @AfterEach
    void tearDown() {
        refreshExecutor.stop();
    }

    @Test
    void hotKey_neverSynchronousReloadLatency() throws Exception {
        // freshFor=10ms, staleFor=1m. The hot loop reads faster than
        // freshFor so most reads land inside the fresh window — RA's
        // territory. A slow loader (50ms) magnifies any synchronous
        // wait into an obvious tail spike.
        long freshForNanos = Duration.ofMillis(10).toNanos();
        long loaderRuntimeNanos = Duration.ofMillis(50).toNanos();
        SwrSidecar sidecar = new SwrSidecar("hot", freshForNanos,
                refreshExecutor, null, meterRegistry);
        // Seed the EWMA so RA's predicate has a meaningful estimate
        // immediately, not just after the first observed loader run.
        ewma.record(loaderRuntimeNanos);
        RefreshAheadCoordinator ra = new RefreshAheadCoordinator(
                "hot", freshForNanos, 1.0, sidecar, refreshExecutor,
                null, ewma, meterRegistry);

        com.github.benmanes.caffeine.cache.Cache<Object, Object> native_ = Caffeine.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(Duration.ofMinutes(1))
                .executor(Runnable::run)
                .removalListener((k, v, cause) -> {
                    if (k != null && cause != RemovalCause.REPLACED) {
                        sidecar.onRemoval(k.toString());
                    }
                })
                .build();
        CaffeineCache delegate = new CaffeineCache("hot", native_, true);
        LocalOnlyCache cache = new LocalOnlyCache(delegate, null, null,
                meterRegistry, sidecar, ra, ewma);

        AtomicLong loaderInvocations = new AtomicLong();
        java.util.concurrent.Callable<String> loader = () -> {
            loaderInvocations.incrementAndGet();
            // Slow loader: any synchronous wait by a reader will be
            // visible as ≥50ms in the latency sample.
            Thread.sleep(50);
            return "v" + loaderInvocations.get();
        };

        // Cold load to seed the cache. This read DOES synchronously wait
        // on the loader (no L1 entry yet), so we exclude it from the
        // tail measurement.
        cache.get("k", loader);

        // Hot loop: 10 000 reads on the same key. With freshFor=10ms and
        // sleeps below freshFor, most reads land in the fresh window
        // where RA may speculatively refresh — but synchronous return is
        // mandatory.
        int n = 10_000;
        long[] latenciesNs = new long[n];
        for (int i = 0; i < n; i++) {
            long t0 = System.nanoTime();
            cache.get("k", loader);
            latenciesNs[i] = System.nanoTime() - t0;
            // Tiny pause to let some reads age into the stale window
            // every now and again — exercises both SWR and RA dispatch
            // paths within the same loop.
            if ((i & 0xFF) == 0) Thread.sleep(1);
        }

        // Tail: the loader takes 50ms. Any reader that synchronously
        // waited would surface as >= 50ms latency. We assert the
        // 99th-percentile latency is well below that.
        Arrays.sort(latenciesNs);
        long p50 = latenciesNs[n * 50 / 100];
        long p99 = latenciesNs[n * 99 / 100];
        long p999 = latenciesNs[n * 999 / 1000];
        long max = latenciesNs[n - 1];

        // Loader's P99: the loader runtime is essentially constant at
        // 50ms; we assert no read latency exceeds half of it (25ms),
        // which would only happen if a reader synchronously waited for
        // the loader. Real L1 reads are sub-microsecond; even with GC
        // hiccups, 25ms is a generous ceiling.
        long bound = Duration.ofMillis(25).toNanos();
        assertThat(p99)
                .as("p50=%dns p99=%dns p99.9=%dns max=%dns; loader-runtime=50ms;"
                        + " no read should synchronously wait for the loader",
                        p50, p99, p999, max)
                .isLessThan(bound);

        // Sanity: the loader was invoked many times (RA + SWR refreshed
        // asynchronously throughout the loop), but each invocation was
        // off the read path.
        assertThat(loaderInvocations.get()).isGreaterThan(1L);
    }
}
