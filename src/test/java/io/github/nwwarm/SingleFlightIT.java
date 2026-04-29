package io.github.nwwarm;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies herd protection on the loader path.
 *
 * <p>{@link NearCache#get(Object, java.util.concurrent.Callable)} provides two
 * tiers of protection: Caffeine's atomic compute (per-JVM) and a Redisson
 * RLock (cross-node). Under contention the loader must run exactly once.
 */
class SingleFlightIT extends RedisTestBase {

    private RedissonClient redisson;

    @BeforeEach
    void setUp() {
        redisson = newRedisson();
    }

    @AfterEach
    void tearDown() {
        if (redisson != null) redisson.shutdown();
    }

    @Test
    void singleNode_concurrentLoadersOnColdKey_runLoaderExactlyOnce() throws Exception {
        NearCache cache = newNearCache("sf-single-" + System.nanoTime(),
                redisson, defaultBreaker(), "node-A");

        try {
            int threads = 32;
            AtomicInteger loaderCalls = new AtomicInteger();
            CountDownLatch ready = new CountDownLatch(threads);
            CountDownLatch go = new CountDownLatch(1);

            ExecutorService exec = Executors.newFixedThreadPool(threads);
            try {
                List<Future<String>> futures = new ArrayList<>();
                for (int i = 0; i < threads; i++) {
                    futures.add(exec.submit(() -> {
                        ready.countDown();
                        go.await();
                        return cache.get("cold", () -> {
                            loaderCalls.incrementAndGet();
                            // Simulate slow loader so all threads pile up.
                            Thread.sleep(150);
                            return "loaded";
                        });
                    }));
                }
                ready.await(5, TimeUnit.SECONDS);
                go.countDown();

                for (Future<String> f : futures) {
                    assertThat(f.get(10, TimeUnit.SECONDS)).isEqualTo("loaded");
                }
            } finally {
                exec.shutdownNow();
            }
            assertThat(loaderCalls.get()).isEqualTo(1);
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void twoNodes_concurrentLoadersOnColdKey_runLoaderExactlyOnce() throws Exception {
        String name = "sf-multi-" + System.nanoTime();
        RedissonClient redissonB = newRedisson();
        NearCache a = newNearCache(name, redisson, defaultBreaker(), "node-A");
        NearCache b = newNearCache(name, redissonB, defaultBreaker(), "node-B");

        try {
            int threadsPerNode = 8;
            AtomicInteger loaderCalls = new AtomicInteger();
            CountDownLatch ready = new CountDownLatch(threadsPerNode * 2);
            CountDownLatch go = new CountDownLatch(1);

            ExecutorService exec = Executors.newFixedThreadPool(threadsPerNode * 2);
            try {
                List<Future<String>> futures = new ArrayList<>();
                for (int i = 0; i < threadsPerNode; i++) {
                    futures.add(exec.submit(() -> loadOn(a, "cold", loaderCalls, ready, go)));
                    futures.add(exec.submit(() -> loadOn(b, "cold", loaderCalls, ready, go)));
                }
                ready.await(5, TimeUnit.SECONDS);
                go.countDown();

                for (Future<String> f : futures) {
                    assertThat(f.get(15, TimeUnit.SECONDS)).isEqualTo("loaded");
                }
            } finally {
                exec.shutdownNow();
            }

            // Cross-node single-flight: the second node re-checks L2 after the
            // distributed lock is released and must find the value the first
            // node wrote, skipping its own loader call.
            assertThat(loaderCalls.get())
                    .as("loader must run exactly once across both nodes")
                    .isEqualTo(1);
        } finally {
            a.shutdown();
            b.shutdown();
            redissonB.shutdown();
        }
    }

    private static String loadOn(NearCache cache,
                                 String key,
                                 AtomicInteger loaderCalls,
                                 CountDownLatch ready,
                                 CountDownLatch go) throws Exception {
        ready.countDown();
        go.await();
        return cache.get(key, () -> {
            loaderCalls.incrementAndGet();
            Thread.sleep(300);   // loader must outlast pub/sub propagation + lock contention
            return "loaded";
        });
    }
}
