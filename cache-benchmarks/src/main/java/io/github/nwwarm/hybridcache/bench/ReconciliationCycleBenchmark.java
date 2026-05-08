package io.github.nwwarm.hybridcache.bench;

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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.TimeUnit;

/**
 * Reconciliation cycle cost as a function of cache count. Each
 * cycle issues one {@code RAtomicLong.get()} per cache plus a
 * comparison against a local {@code AtomicLong}. The Redis cost
 * dominates in production; this benchmark isolates the comparison
 * arithmetic so a regression in the local hot-path is visible
 * without confounding network latency.
 *
 * The {@code 1M} cache count is intentionally larger than any
 * sane deployment — it is the "worst case in the cycle scheduler"
 * stress test.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
@State(Scope.Benchmark)
public class ReconciliationCycleBenchmark {

    @Param({"1000", "10000", "100000", "1000000"})
    public int caches;

    private AtomicLong[] localSeq;
    private long[] redisSeq;

    @Setup
    public void setUp() {
        localSeq = new AtomicLong[caches];
        redisSeq = new long[caches];
        for (int i = 0; i < caches; i++) {
            localSeq[i] = new AtomicLong(100);
            redisSeq[i] = 100 + (i % 7);
        }
    }

    @Benchmark
    public void cycle(Blackhole bh) {
        long misses = 0;
        for (int i = 0; i < caches; i++) {
            long delta = redisSeq[i] - localSeq[i].get();
            if (delta > 5) misses++;
        }
        bh.consume(misses);
    }
}
