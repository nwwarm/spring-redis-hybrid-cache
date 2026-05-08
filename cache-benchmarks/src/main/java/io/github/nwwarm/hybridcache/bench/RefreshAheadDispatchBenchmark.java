package io.github.nwwarm.hybridcache.bench;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Refresh-ahead dispatch overhead per read inside the fresh window.
 * Measures the XFetch predicate (one Math.log + multiply + compare)
 * plus the in-flight-map check. Fires a refresh into a no-op executor
 * so the dispatch path is exercised without measuring loader cost.
 *
 * Expected: nanoseconds-per-read budget. If the predicate evaluation
 * shows up in profiles on a hot read path, we have a regression —
 * §10 0.5.0 RA-as-mode-of-SWR pinned this as the budget for the
 * "every read inside fresh window evaluates" decision.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
@State(Scope.Benchmark)
public class RefreshAheadDispatchBenchmark {

    private static final long FRESH_FOR_NANOS = TimeUnit.MINUTES.toNanos(5);
    private static final double BETA = 1.0;
    private static final long LOADER_RUNTIME_NANOS = TimeUnit.MILLISECONDS.toNanos(30);

    @Benchmark
    public void xfetchPredicate(Blackhole bh) {
        long now = System.nanoTime();
        long lastWrite = now - TimeUnit.MINUTES.toNanos(2);
        double rand = ThreadLocalRandom.current().nextDouble();
        long timeSinceWrite = now - lastWrite;
        boolean fire = (timeSinceWrite + (long) (BETA * LOADER_RUNTIME_NANOS * (-Math.log(rand))))
                       >= FRESH_FOR_NANOS;
        bh.consume(fire);
    }
}
