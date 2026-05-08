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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Async {@code Cache.retrieve(...)} overhead. Three scenarios:
 *
 *  - {@code l1Hit}: hits the L1 fast path; returns
 *    {@code CompletableFuture.completedFuture(value)} synchronously.
 *  - {@code l2Hit}: simulates an async L2 hit by chaining through a
 *    completed RFuture stand-in; reports the chain composition cost.
 *  - {@code loaderHop}: cold key, hops to an executor for the loader;
 *    reports wall-clock to first hit including the executor hand-off.
 *
 * Netty event-loop occupancy is not exercised here — the
 * {@link io.github.nwwarm.hybridcache.core} {@code AsyncBlockHoundIT}
 * is the regression guard for "loader never runs on Netty."
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
@State(Scope.Benchmark)
public class AsyncRetrieveBenchmark {

    @Param({"l1Hit", "l2Hit", "loaderHop"})
    public String scenario;

    private ConcurrentMap<String, String> store;
    private ConcurrentMap<String, CompletableFuture<Object>> inflight;
    private ExecutorService refreshExecutor;

    @Setup
    public void setUp() {
        store = new ConcurrentHashMap<>();
        inflight = new ConcurrentHashMap<>();
        refreshExecutor = Executors.newFixedThreadPool(4);
        for (int i = 0; i < 1_000; i++) {
            store.put("key-" + i, "value-" + i);
        }
    }

    @Benchmark
    public Object retrieve(Blackhole bh) throws Exception {
        String key = "key-" + ((int) (System.nanoTime() & 0x3FF));
        switch (scenario) {
            case "l1Hit":
                return CompletableFuture.completedFuture(store.get(key));
            case "l2Hit":
                return CompletableFuture
                        .completedFuture(store.get(key))
                        .thenApply(v -> v);
            case "loaderHop":
                CompletableFuture<Object> mine = new CompletableFuture<>();
                CompletableFuture<Object> theirs = inflight.putIfAbsent(key, mine);
                CompletableFuture<Object> winner = theirs == null ? mine : theirs;
                if (theirs == null) {
                    CompletableFuture
                            .supplyAsync(() -> (Object) ("loaded-" + key), refreshExecutor)
                            .whenComplete((v, ex) -> {
                                try {
                                    if (ex != null) mine.completeExceptionally(ex);
                                    else mine.complete(v);
                                } finally {
                                    inflight.remove(key, mine);
                                }
                            });
                }
                return winner.get(1, TimeUnit.SECONDS);
            default:
                throw new IllegalStateException(scenario);
        }
    }
}
