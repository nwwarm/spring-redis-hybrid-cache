package io.github.nwwarm.hybridcache.core;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.nwwarm.hybridcache.config.CacheProperties;
import io.github.nwwarm.hybridcache.invalidation.InvalidationDispatcher;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.mockito.Answers;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.springframework.cache.caffeine.CaffeineCache;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Proves that {@link NearCache#currentGeneration()} and
 * {@link DistributedOnlyCache#currentGeneration()} issue exactly one Redis
 * call when N ≥ 16 threads simultaneously decide a generation refresh is due.
 *
 * <p>Mechanism: the mock {@code distributedGeneration.get()} sleeps 50 ms to
 * widen the contention window. Without the CAS fix every thread passes the
 * staleness check and calls {@code get()} — the test would count N calls.
 * With the CAS fix only the winner enters the Redis call; all others return
 * the cached local generation.
 *
 * <p>Repeated 100× to guard against scheduler-driven accidental serialization
 * masking the bug on a single run.
 */
class GenerationRefreshConcurrencyTest {

    private static final int THREADS = 32;

    private RedissonClient redisson;
    private AtomicInteger getCallCount;
    private RAtomicLong mockDistributedGeneration;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setup() {
        redisson = mock(RedissonClient.class, Answers.RETURNS_DEEP_STUBS);

        getCallCount = new AtomicInteger(0);
        mockDistributedGeneration = mock(RAtomicLong.class);

        when(mockDistributedGeneration.get()).thenAnswer(inv -> {
            getCallCount.incrementAndGet();
            // Simulate Redis latency so all THREADS are in-flight together;
            // without the CAS guard they would all enter this block.
            Thread.sleep(50);
            return 0L;
        });
        // incrementAndGet is used only by bumpGeneration / clear — not under test here.
        when(mockDistributedGeneration.incrementAndGet()).thenReturn(1L);

        when(redisson.getAtomicLong(anyString())).thenReturn(mockDistributedGeneration);

        // Bucket reads return null (cache miss) — irrelevant for this test.
        @SuppressWarnings("rawtypes")
        RBucket nullBucket = mock(RBucket.class);
        when(nullBucket.get()).thenReturn(null);
        when(redisson.<Object>getBucket(anyString())).thenReturn(nullBucket);
        when(redisson.<Object>getBucket(anyString(), any())).thenReturn(nullBucket);
    }

    @RepeatedTest(100)
    void nearCache_exactlyOneGenerationRead_whenRefreshDue() throws Exception {
        CircuitBreaker breaker = CircuitBreakerRegistry.ofDefaults().circuitBreaker("test");
        InvalidationDispatcher dispatcher = new InvalidationDispatcher(redisson, "node", new SimpleMeterRegistry());

        NearCache cache = new NearCache(
                caffeineCache("near-test"),
                spec(), null, redisson, breaker, dispatcher, new SimpleMeterRegistry(),
                new KeyLogFormatter(false, "test"));

        // Construction called distributedGeneration.get() once (initializeGeneration).
        // Reset the counter and force a refresh to be due before the concurrent phase.
        getCallCount.set(0);
        cache.forceRefreshDue();

        runConcurrently(THREADS, () -> cache.get("k"));

        assertThat(getCallCount.get())
                .as("expected exactly 1 Redis generation read across %d concurrent threads", THREADS)
                .isEqualTo(1);

        cache.shutdown();
    }

    @RepeatedTest(100)
    void distributedOnlyCache_exactlyOneGenerationRead_whenRefreshDue() throws Exception {
        CircuitBreaker breaker = CircuitBreakerRegistry.ofDefaults().circuitBreaker("test-dist");

        InvalidationDispatcher dispatcher = new InvalidationDispatcher(redisson, "test-node", new SimpleMeterRegistry());
        DistributedOnlyCache cache = new DistributedOnlyCache(
                "dist-test", spec(), null, redisson, breaker, dispatcher, new SimpleMeterRegistry(),
                new KeyLogFormatter(false, "test"));

        getCallCount.set(0);
        cache.forceRefreshDue();

        runConcurrently(THREADS, () -> cache.get("k"));

        assertThat(getCallCount.get())
                .as("expected exactly 1 Redis generation read across %d concurrent threads", THREADS)
                .isEqualTo(1);
    }

    private static void runConcurrently(int n, Runnable task) throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(n);
        ExecutorService pool = Executors.newFixedThreadPool(n);
        List<Future<?>> futures = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            futures.add(pool.submit(() -> {
                try {
                    barrier.await(); // synchronize: all threads start simultaneously
                    task.run();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.SECONDS);
        for (Future<?> f : futures) {
            f.get(); // propagate any assertion errors from threads
        }
    }

    private static CaffeineCache caffeineCache(String name) {
        com.github.benmanes.caffeine.cache.Cache<Object, Object> native_ =
                Caffeine.newBuilder().maximumSize(100).recordStats().build();
        return new CaffeineCache(name, native_, true);
    }

    private static CacheProperties.CacheSpec spec() {
        return new CacheProperties.CacheSpec(
                CacheProperties.Tier.NEAR_CACHE,
                Duration.ofMinutes(10), 100,
                Duration.ofSeconds(2), Duration.ofSeconds(10),
                CacheProperties.Codec.JSON, null, null, null, 0.0, null, null, null, null);
    }
}
