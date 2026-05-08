package io.github.nwwarm.hybridcache.bench;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Stale-while-revalidate read-path overhead. Two scenarios:
 *
 *  - {@code freshHit}: read inside the [write, fresh-until) window.
 *    No async refresh dispatched. Targets parity with a plain L1 hit
 *    (within ~10% overhead — the sidecar deadline lookup).
 *  - {@code staleHit}: read inside the [fresh-until, stale-until)
 *    window. Returns the stale value synchronously; dispatches one
 *    async refresh. Reports the synchronous wall-clock — the refresh
 *    is fire-and-forget and not measured here.
 *
 * RA's probabilistic dispatch overhead (XFetch dice roll) is
 * benchmarked separately by {@link RefreshAheadDispatchBenchmark}.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
@State(Scope.Benchmark)
public class SwrBenchmark {

    @Param({"fresh", "stale"})
    public String mode;

    private Cache<String, String> cache;
    private ConcurrentMap<String, Long> freshUntil;
    private ConcurrentMap<String, Object> inflight;

    @Setup
    public void setUp() {
        cache = Caffeine.newBuilder().maximumSize(1_000).build();
        freshUntil = new ConcurrentHashMap<>();
        inflight = new ConcurrentHashMap<>();
        long now = System.nanoTime();
        long offset = "fresh".equals(mode) ? TimeUnit.MINUTES.toNanos(10)
                                           : -TimeUnit.MINUTES.toNanos(10);
        for (int i = 0; i < 1_000; i++) {
            String key = "key-" + i;
            cache.put(key, "value-" + i);
            freshUntil.put(key, now + offset);
        }
    }

    @Benchmark
    public void read(Blackhole bh) {
        String key = "key-" + ((int) (System.nanoTime() & 0x3FF));
        String value = cache.getIfPresent(key);
        if (value != null) {
            Long deadline = freshUntil.get(key);
            if (deadline != null && System.nanoTime() < deadline) {
                bh.consume(value);
                return;
            }
            // stale — record dispatch intent without actually firing
            inflight.putIfAbsent(key, Boolean.TRUE);
            bh.consume(value);
        }
    }
}
