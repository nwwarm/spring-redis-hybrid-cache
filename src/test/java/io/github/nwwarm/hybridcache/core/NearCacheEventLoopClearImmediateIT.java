package io.github.nwwarm.hybridcache.core;

import io.github.nwwarm.hybridcache.testfixtures.RedisTestBase;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.channel.EventLoopGroup;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression guard for the second half of the 1.0.1 sync-on-event-loop fix:
 * {@code NearCache.clearImmediate} (and its mirror in
 * {@code DistributedOnlyCache}) used to call sync
 * {@code RAtomicLong.incrementAndGet} and {@code RKeys.unlinkByPattern}.
 * Reachable via Spring reactive {@code @CacheEvict(allEntries=true)} on a
 * Mono-returning method whose preceding stage completed on a Netty I/O
 * thread; under load the {@code clearImmediate()} continuation runs inline
 * on the I/O thread and the sync ops would throw
 * {@code IllegalStateException("Sync methods can't be invoked from
 * async/rx/reactive listeners")}.
 *
 * <p>The 1.0.1 fix routes both calls through their async variants
 * ({@code incrementAndGetAsync} + {@code unlinkByPatternAsync}) composed
 * into the existing {@code awaitUnlessOnEventLoop} chain — so the only
 * Redisson calls made from a Netty thread are async by construction.
 *
 * <p>How this test lands on a Redisson event-loop thread: by submitting
 * the {@code clearImmediate()} call directly onto Redisson's
 * {@link EventLoopGroup}. This guarantees the runnable executes on an I/O
 * thread (vs. chaining on an async future, where the local-Redis ack
 * races the {@code .thenAccept} attachment and the continuation may end
 * up running on the calling thread).
 *
 * <p>With the pre-1.0.1 code, {@code clearImmediate} would throw
 * {@code IllegalStateException} immediately and {@code l2Failures} /
 * generation state would never advance. With the fix, the async chain
 * dispatches successfully, the generation bump completes, and
 * {@code localGeneration} advances asynchronously.
 */
class NearCacheEventLoopClearImmediateIT extends RedisTestBase {

    private RedissonClient redisson;
    private SimpleMeterRegistry meterRegistry;

    @BeforeEach
    void setUp() {
        redisson = newRedisson();
        meterRegistry = new SimpleMeterRegistry();
    }

    @AfterEach
    void tearDown() {
        if (redisson != null) redisson.shutdown();
    }

    @Test
    void nearCache_clearImmediate_invokedFromRedissonEventLoop_doesNotThrow() throws Exception {
        NearCache cache = newNearCache("evt-loop-clear-imm-near-" + System.nanoTime(),
                redisson, defaultBreaker(), "node-A",
                io.github.nwwarm.hybridcache.config.CacheProperties.Codec.JSON,
                meterRegistry);
        try {
            // Warm a few entries on a regular thread so clearImmediate has
            // something to UNLINK; this also gives us a baseline localGeneration.
            for (int i = 0; i < 5; i++) {
                cache.put("k" + i, "v" + i);
            }
            long genBefore = cache.localGeneration();

            assertOnEventLoop(() -> cache.clearImmediate());

            // Sync ops on a Netty thread would have thrown; the fix uses
            // async variants. Awaitility because the bump completes on a
            // future callback, not on the calling event-loop thread.
            Awaitility.await()
                    .atMost(Duration.ofSeconds(5))
                    .pollInterval(Duration.ofMillis(50))
                    .untilAsserted(() ->
                            assertThat(cache.localGeneration())
                                    .as("clearImmediate's async incrementAndGet must"
                                            + " advance localGeneration via the future"
                                            + " callback when invoked from an event-loop"
                                            + " thread")
                                    .isGreaterThan(genBefore));

            // Defense in depth: no L2 failure metered. With the pre-fix sync
            // code, IllegalStateException would have surfaced and the chain's
            // exception handler would have incremented this counter.
            double l2Failures = meterRegistry.find("cache.l2.failures")
                    .tag("cache", cache.getName())
                    .counter()
                    .count();
            assertThat(l2Failures)
                    .as("event-loop-invoked clearImmediate must not record any L2"
                            + " failure — a positive count means a sync Redisson op"
                            + " threw IllegalStateException")
                    .isZero();
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void distributedOnlyCache_clearImmediate_invokedFromRedissonEventLoop_doesNotThrow() throws Exception {
        DistributedOnlyCache cache = newDistributedCache(
                "evt-loop-clear-imm-dist-" + System.nanoTime(),
                redisson, defaultBreaker(), "node-A");
        try {
            for (int i = 0; i < 5; i++) {
                cache.put("k" + i, "v" + i);
            }
            long genBefore = cache.localGeneration();

            assertOnEventLoop(() -> cache.clearImmediate());

            Awaitility.await()
                    .atMost(Duration.ofSeconds(5))
                    .pollInterval(Duration.ofMillis(50))
                    .untilAsserted(() ->
                            assertThat(cache.localGeneration())
                                    .as("clearImmediate's async incrementAndGet must"
                                            + " advance localGeneration via the future"
                                            + " callback when invoked from an event-loop"
                                            + " thread")
                                    .isGreaterThan(genBefore));

            // DistributedOnlyCache uses a different metric name
            // (cache.distributed.failures) but exposes no public test seam
            // for it; the localGeneration assertion above already proves
            // the bump did not throw on the I/O thread.
        } finally {
            cache.shutdown();
        }
    }

    /**
     * Submit the runnable onto Redisson's {@link EventLoopGroup} and assert
     * (1) it actually ran on a Netty thread (sanity — the bug only fires
     * there) and (2) it completed without an exception.
     */
    private void assertOnEventLoop(Runnable r) throws Exception {
        EventLoopGroup eventLoopGroup = ((Redisson) redisson)
                .getServiceManager()
                .getGroup();

        AtomicReference<String> threadName = new AtomicReference<>();
        AtomicReference<Throwable> captured = new AtomicReference<>();

        CompletableFuture<Void> onEventLoop = CompletableFuture.runAsync(() -> {
            threadName.set(Thread.currentThread().getName());
            try {
                r.run();
            } catch (Throwable t) {
                captured.set(t);
            }
        }, eventLoopGroup);

        onEventLoop.get(5, TimeUnit.SECONDS);

        String thr = threadName.get();
        assertThat(thr)
                .as("test sanity: runnable must execute on a Redisson event-loop"
                        + " thread for this to actually exercise the"
                        + " sync-on-event-loop bug")
                .satisfiesAnyOf(
                        name -> assertThat(name).startsWith("redisson-netty-"),
                        name -> assertThat(name).startsWith("nioEventLoopGroup-"));

        assertThat(captured.get())
                .as("clearImmediate invoked on an event-loop thread must not"
                        + " throw IllegalStateException; sync"
                        + " incrementAndGet / unlinkByPattern were the 1.0.1"
                        + " bug — the fix uses async variants composed into"
                        + " the CompletionStage chain")
                .isNull();
    }
}
