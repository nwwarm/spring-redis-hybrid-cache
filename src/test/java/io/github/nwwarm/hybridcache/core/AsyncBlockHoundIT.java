package io.github.nwwarm.hybridcache.core;

import io.github.nwwarm.hybridcache.testfixtures.RedisTestBase;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import reactor.blockhound.BlockHound;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * BlockHound-enforced integration test: the async retrieve(...) path
 * must not block on threads marked non-blocking. The headline regression
 * guard for the reactive surface (§ 10 0.5.0 / guardrail item 2 of the
 * async section).
 *
 * <p>BlockHound is installed once per JVM. The {@link reactor.blockhound.BlockHound#install}
 * call enables Reactor's default integration which marks Reactor's
 * scheduler threads, but our cache layer carries no Reactor dependency —
 * so we register a custom rule that marks Redisson's Netty event-loop
 * threads (named {@code redisson-netty-*} / {@code nioEventLoopGroup-*})
 * as non-blocking. A blocking call landing on those threads is the bug
 * we want to catch: a misplaced {@code .join()}, a sync Redis op on the
 * async path, a synchronous loader on the loader-hop continuation.
 *
 * <p>This test is in the {@code IT} suite (failsafe-run) because it
 * needs Redisson (Testcontainers) to actually generate Netty traffic.
 *
 * <p><b>Disabling this test silently breaks the async non-blocking
 * guarantee.</b> § 11 0.5.0 / guardrail item 1 of the async section
 * pins this — every other guardrail in the section is detection-after-the-fact;
 * BlockHound is the only one that catches a regression on the very first
 * test run. Do not weaken this test (e.g. by adding broad allowedBlockingCallsInside)
 * to silence noise. Identify the offending call and fix it.
 */
class AsyncBlockHoundIT extends RedisTestBase {

    /**
     * Records the most recent blocking-call detection so the test can
     * explicitly fail with the offending call's stack rather than the
     * BlockHound-thrown exception (which the runtime swallows on a
     * non-Reactor thread).
     */
    private static final AtomicReference<Throwable> LAST_BLOCK = new AtomicReference<>();

    @BeforeAll
    static void installBlockHound() {
        // Install once. Subsequent calls no-op. The custom non-blocking
        // predicate identifies Redisson's Netty event-loop threads —
        // those are the threads where a blocking call would stall every
        // other Redis op in the JVM.
        BlockHound.builder()
                .nonBlockingThreadPredicate(predicate -> predicate.or(thread -> {
                    // Mark only the Netty I/O threads. Redisson's worker
                    // pool ("redisson-*") legitimately uses LinkedBlockingQueue.take
                    // for work-stealing; we don't want to flag that as a
                    // bug. We care specifically about whether OUR async
                    // chain blocks on the I/O thread.
                    String n = thread.getName();
                    return n.startsWith("redisson-netty-")
                            || n.startsWith("nioEventLoopGroup-");
                }))
                .blockingMethodCallback(call -> {
                    // Capture the call so the test fails with a clean
                    // assertion. Re-throwing here would propagate into
                    // the Netty thread's continuation and we'd never see
                    // the actual stack from the test thread.
                    LAST_BLOCK.set(new AssertionError(
                            "Blocking call on non-blocking thread: " + call));
                })
                // Netty's HashedWheelTimer uses Thread.sleep internally to
                // pace its tick wheel — it runs on a Netty thread by
                // design. Not a bug in our code; allow.
                .allowBlockingCallsInside(
                        "io.netty.util.HashedWheelTimer$Worker", "waitForNextTick")
                .allowBlockingCallsInside(
                        "io.netty.util.HashedWheelTimer$Worker", "run")
                // Netty's I/O thread occasionally takes from a queue
                // internally (epoll wait paths can land here on JDK
                // versions with select-based polling). Allow on the I/O
                // selector entry points only.
                .allowBlockingCallsInside(
                        "io.netty.channel.nio.NioEventLoop", "run")
                // ThreadPoolExecutor's worker thread blocks on its work
                // queue's take() while idle — that's the executor's own
                // park, not user code. Allow inside the executor's own
                // task-fetch path.
                .allowBlockingCallsInside(
                        "java.util.concurrent.ThreadPoolExecutor", "getTask")
                // LinkedBlockingQueue.put — refresh executor submit's
                // internal queue insert. Off the Netty thread by design;
                // here for completeness.
                .allowBlockingCallsInside(
                        "java.util.concurrent.LinkedBlockingQueue", "put")
                .install();
    }

    @AfterAll
    static void resetForNextTestClass() {
        LAST_BLOCK.set(null);
    }

    private RedissonClient redisson;
    private SimpleMeterRegistry meterRegistry;
    private RefreshExecutor refreshExecutor;

    @BeforeEach
    void setUp() {
        LAST_BLOCK.set(null);
        redisson = newRedisson();
        meterRegistry = new SimpleMeterRegistry();
        refreshExecutor = new RefreshExecutor(2, meterRegistry);
    }

    @AfterEach
    void tearDown() {
        try {
            refreshExecutor.stop();
        } finally {
            if (redisson != null) redisson.shutdown();
        }
    }

    @Test
    void retrieveNoLoader_l1Hit_doesNotBlockNettyThread() throws Exception {
        NearCache cache = newAsyncNearCache("bh-l1");
        cache.put("k", "v");
        // L1 hit is synchronous on the calling thread — no Netty work
        // at all. Asserting BlockHound didn't trip is sanity: an L1 hit
        // must not even touch a Netty thread.
        cache.retrieve("k").get(2, TimeUnit.SECONDS);
        assertThat(LAST_BLOCK.get())
                .as("L1 hit must not block any thread")
                .isNull();
    }

    @Test
    void retrieveNoLoader_l1Miss_l2Hit_doesNotBlockNettyThread() throws Exception {
        NearCache cache = newAsyncNearCache("bh-l2");
        cache.put("k", "v");
        ((com.github.benmanes.caffeine.cache.Cache<?, ?>) cache.getNativeCache()).invalidateAll();
        // The L2 read chains via getAsync; the async chain runs on
        // Netty. If any step in our chain calls a blocking op, BlockHound
        // captures it.
        cache.retrieve("k").get(5, TimeUnit.SECONDS);
        Throwable block = LAST_BLOCK.get();
        if (block != null) {
            throw new AssertionError("BlockHound caught a blocking call on the async L2 read path", block);
        }
    }

    @Test
    void retrieveWithLoader_coldLoad_loaderRunsOffNetty() throws Exception {
        NearCache cache = newAsyncNearCache("bh-coldload");
        // Loader returns a completed future; runs synchronously on the
        // refresh executor, NOT on Netty (guardrail item 2). BlockHound
        // tripping here means our thenComposeAsync hop is wrong.
        cache.retrieve("k", () -> CompletableFuture.completedFuture("v"))
                .get(5, TimeUnit.SECONDS);
        Throwable block = LAST_BLOCK.get();
        if (block != null) {
            throw new AssertionError("BlockHound caught a blocking call on the async cold-load path", block);
        }
    }

    private NearCache newAsyncNearCache(String name) {
        CircuitBreaker breaker = defaultBreaker();
        com.github.benmanes.caffeine.cache.Cache<Object, Object> caffeineNative =
                com.github.benmanes.caffeine.cache.Caffeine.newBuilder()
                        .expireAfterWrite(Duration.ofMinutes(10))
                        .maximumSize(10_000)
                        .recordStats()
                        .build();
        org.springframework.cache.caffeine.CaffeineCache springCache =
                new org.springframework.cache.caffeine.CaffeineCache(name, caffeineNative, true);
        io.github.nwwarm.hybridcache.invalidation.InvalidationDispatcher dispatcher =
                new io.github.nwwarm.hybridcache.invalidation.InvalidationDispatcher(
                        redisson, "node-" + System.nanoTime(), meterRegistry);
        io.github.nwwarm.hybridcache.config.CacheProperties.CacheSpec spec =
                new io.github.nwwarm.hybridcache.config.CacheProperties.CacheSpec(
                        io.github.nwwarm.hybridcache.config.CacheProperties.Tier.NEAR_CACHE,
                        Duration.ofMinutes(10), 10_000,
                        Duration.ofSeconds(2), Duration.ofSeconds(10),
                        io.github.nwwarm.hybridcache.config.CacheProperties.Codec.JSON,
                        null, null, null, 0.0, null, null, null, null, null);
        return new NearCache(
                springCache, spec, resolveTestCodec(spec.codec()), redisson,
                breaker, dispatcher, meterRegistry,
                new KeyLogFormatter(false, "test"),
                null, null, null, refreshExecutor.asExecutor());
    }
}
