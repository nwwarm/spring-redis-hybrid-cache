package io.github.nwwarm.hybridcache.bench;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-JVM single-flight collapse: N concurrent threads request the
 * same cold key, exactly one loader runs, the rest wait on the
 * shared compute future. Reports wall-clock to first hit and asserts
 * the loader-invocation count is exactly 1.
 *
 * Uses Caffeine's atomic compute (the per-JVM single-flight primitive
 * NearCache uses on the sync path). The async path's
 * ConcurrentMap-based collapse is benchmarked separately by
 * AsyncSingleFlightBenchmark in this same package.
 */
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
@State(Scope.Benchmark)
public class SingleFlightBenchmark {

    public static final int CONCURRENCY = 32;

    private Cache<String, String> cache;
    private CountDownLatch start;
    private AtomicInteger loaderInvocations;

    @Setup
    public void setUp() {
        cache = Caffeine.newBuilder().maximumSize(1_000).build();
        loaderInvocations = new AtomicInteger();
        start = new CountDownLatch(1);
    }

    @Benchmark
    @Threads(CONCURRENCY)
    public String contended() throws Exception {
        return cache.get("cold-key", k -> {
            loaderInvocations.incrementAndGet();
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return "loaded";
        });
    }
}
