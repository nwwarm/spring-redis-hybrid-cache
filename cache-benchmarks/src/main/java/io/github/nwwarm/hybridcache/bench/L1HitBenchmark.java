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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

/**
 * L1 (Caffeine) hit latency. Two thread modes:
 *  - single-threaded baseline (target sub-100ns)
 *  - 10-thread contention (target sub-200ns)
 *
 * Targets are published in DESIGN.md §11 / Production baselines and
 * re-measured on every release. Regression past the baseline by more
 * than 20% blocks 1.0.0.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
@State(Scope.Benchmark)
public class L1HitBenchmark {

    @Param({"single-threaded", "ten-threaded"})
    public String mode;

    private Cache<String, String> cache;
    private String[] keys;

    @Setup
    public void setUp() {
        cache = Caffeine.newBuilder().maximumSize(10_000).build();
        keys = new String[10_000];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = "key-" + i;
            cache.put(keys[i], "value-" + i);
        }
    }

    @Benchmark
    @Threads(1)
    public void singleThreaded(Blackhole bh) {
        if (!"single-threaded".equals(mode)) return;
        bh.consume(cache.getIfPresent(keys[(int) (System.nanoTime() & 0x1FFF)]));
    }

    @Benchmark
    @Threads(10)
    public void tenThreaded(Blackhole bh) {
        if (!"ten-threaded".equals(mode)) return;
        bh.consume(cache.getIfPresent(keys[(int) (System.nanoTime() & 0x1FFF)]));
    }
}
