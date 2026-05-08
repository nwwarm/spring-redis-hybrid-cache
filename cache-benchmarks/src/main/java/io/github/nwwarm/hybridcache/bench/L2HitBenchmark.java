package io.github.nwwarm.hybridcache.bench;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.concurrent.TimeUnit;

/**
 * L2 (Redis) hit latency against a Testcontainers Redis. Reports P50
 * and P99 — JMH's percentile profiler is enabled via the run command:
 *
 *   java -jar benchmarks.jar L2HitBenchmark -prof gc -prof '"perfasm"'
 *
 * Wall-clock per round-trip measured here gates the published L2-hit
 * baseline in DESIGN.md §11. Numbers vary with hardware, network, and
 * Redis version; every release re-measures on the CI runner of record
 * (documented in §11) before publishing the baseline.
 */
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
@Fork(1)
@State(Scope.Benchmark)
public class L2HitBenchmark {

    private GenericContainer<?> redis;
    private RedissonClient client;
    private String[] keys;

    @Setup
    public void setUp() {
        redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
                .withExposedPorts(6379);
        redis.start();
        Config cfg = new Config();
        cfg.useSingleServer().setAddress(
                "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379));
        client = Redisson.create(cfg);
        keys = new String[1_000];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = "bench:l2:" + i;
            client.<String>getBucket(keys[i]).set("value-" + i);
        }
    }

    @TearDown
    public void tearDown() {
        if (client != null) client.shutdown();
        if (redis != null) redis.stop();
    }

    @Benchmark
    public void l2Get(Blackhole bh) {
        RBucket<String> bucket = client.getBucket(keys[(int) (System.nanoTime() & 0x3FF)]);
        bh.consume(bucket.get());
    }
}
