package io.github.nwwarm.hybridcache.core;

import io.github.nwwarm.hybridcache.testfixtures.RedisTestBase;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.channel.EventLoopGroup;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.springframework.cache.Cache;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression guard for the 1.0.0 → 1.0.1 sync-on-event-loop bug.
 *
 * <p>Surfaced by a 24-hour 1000 req/s soak: under load a fraction of
 * {@code NearCache.put(...)} calls landed on a Redisson Netty
 * event-loop thread because the preceding Redis op's
 * {@code CompletableFuture} completed on the I/O thread, and the
 * Spring reactive cache integration's continuation ran inline.
 * Inside {@code put} the (old) sync {@code RBucket.set(...)} from
 * {@code writeToL2(...)} then threw {@code IllegalStateException
 * ("Sync methods can't be invoked from async/rx/reactive listeners")},
 * which was caught by {@code writeToL2}'s exception handler and
 * silently logged as an L2 failure. Net effect: L2 was not written;
 * cross-node coherence degraded; metrics flooded with
 * {@code cache.l2.failures}.
 *
 * <p>The fix in 1.0.1 routes {@code put} / {@code evict} / {@code clear}
 * through the existing async helpers ({@code writeToL2Async},
 * {@code deleteFromL2Async}, {@code bumpGenerationAsync},
 * {@code publishInvalidationAsync}), so the only Redisson calls made
 * from a Reactor / event-loop thread are async by construction.
 *
 * <p><b>How the test lands on a Redisson event-loop thread:</b> by
 * subscribing to an async Redis op's completion stage. Redisson
 * completes those futures on its Netty event loop, and the
 * {@code thenAccept} continuation we attach therefore runs on
 * that I/O thread. Inside the continuation we drive the cache layer
 * exactly as Spring's reactive cache machinery would.
 *
 * <p>With the pre-1.0.1 code the assertions on L2 contents fail
 * (L2 never receives the value because the sync call throws and is
 * swallowed). With the 1.0.1 fix they pass.
 */
class NearCacheEventLoopWriteIT extends RedisTestBase {

    private RedissonClient redisson;
    private SimpleMeterRegistry meterRegistry;
    private RefreshExecutor refreshExecutor;

    @BeforeEach
    void setUp() {
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
    void put_invokedFromRedissonEventLoop_writesL2WithoutThrowing() throws Exception {
        NearCache cache = newAsyncNearCache("evt-loop-put");
        String key = "evt-put-key";
        String value = "evt-put-value";

        // Land on a Redisson Netty event-loop thread by scheduling the
        // put() directly onto Redisson's EventLoopGroup. This is more
        // reliable than chaining on a CompletionStage callback — a
        // local Redis ack races the .thenAccept attachment and the
        // continuation may end up running on the calling thread instead
        // of Netty. Going through the group's execute() guarantees the
        // runnable executes on an I/O thread, which is exactly where
        // the soak surfaced the bug (Spring's reactive cache adapter +
        // Reactor's MonoCompletionStage ran the put() continuation on
        // the same I/O thread that completed the preceding Redis op).
        EventLoopGroup eventLoopGroup = ((Redisson) redisson)
                .getServiceManager()
                .getGroup();

        AtomicReference<String> callbackThreadName = new AtomicReference<>();
        AtomicReference<Throwable> capturedFromPut = new AtomicReference<>();

        CompletableFuture<Void> onEventLoop = CompletableFuture.runAsync(() -> {
            callbackThreadName.set(Thread.currentThread().getName());
            try {
                cache.put(key, value);
            } catch (Throwable t) {
                capturedFromPut.set(t);
            }
        }, eventLoopGroup);

        onEventLoop.get(5, TimeUnit.SECONDS);

        // Sanity: we actually ran on a Netty I/O thread (the bug
        // requires this — Redisson's IllegalStateException only fires
        // on "redisson-netty-*" / "nioEventLoopGroup-*" threads).
        String thr = callbackThreadName.get();
        assertThat(thr)
                .as("test sanity: runnable must execute on a Redisson event-loop thread "
                        + "for this to actually exercise the sync-on-event-loop bug")
                .satisfiesAnyOf(
                        name -> assertThat(name).startsWith("redisson-netty-"),
                        name -> assertThat(name).startsWith("nioEventLoopGroup-"));

        assertThat(capturedFromPut.get())
                .as("put() invoked on an event-loop thread must not throw "
                        + "IllegalStateException; sync Redisson ops in writeToL2 / "
                        + "publishInvalidation were the 1.0.0 bug — the 1.0.1 fix "
                        + "uses async variants composed into the CompletionStage chain")
                .isNull();

        // Headline assertion: L2 actually received the value. With the
        // sync-on-event-loop bug, RBucket.set threw before the SET
        // reached Redis, writeToL2's catch incremented cache.l2.failures
        // and returned, and L2 stayed empty. With the fix, setAsync
        // schedules the SET on the event loop and completes
        // asynchronously.
        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> {
                    // Bypass L1 so the read goes to L2. The L1 was
                    // populated synchronously inside put() (Caffeine
                    // call); we want to verify the async L2 write
                    // landed too.
                    ((com.github.benmanes.caffeine.cache.Cache<?, ?>) cache.getNativeCache())
                            .invalidateAll();
                    Cache.ValueWrapper wrapper = cache.get(key);
                    assertThat(wrapper)
                            .as("L2 must contain the value written from an event-loop thread")
                            .isNotNull();
                    assertThat(wrapper.get()).isEqualTo(value);
                });

        // Defense in depth: no L2 failure was metered. With the old
        // sync code each event-loop-invoked write incremented this
        // counter. With the fix the async write completes cleanly.
        double l2Failures = meterRegistry.find("cache.l2.failures")
                .tag("cache", cache.getName())
                .counter()
                .count();
        assertThat(l2Failures)
                .as("event-loop-invoked put() must not record any L2 failure — "
                        + "a positive count means a sync Redisson op threw "
                        + "IllegalStateException and was logged as a failure")
                .isZero();
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
