package io.github.nwwarm.hybridcache.core;

import io.github.nwwarm.hybridcache.invalidation.InvalidationMessage;
import io.github.nwwarm.hybridcache.testfixtures.RedisTestBase;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.Codec;
import org.redisson.codec.TypedJsonJacksonCodec;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression guard for the H5 fix: a failed L2 delete during evict must NOT
 * publish an invalidation. If it did, remote subscribers would drop their
 * still-coherent L1 entry, fall through to L2, and find the value still
 * present (the delete failed) — silently re-populating L1 with stale data.
 *
 * <p>The test uses two NearCache instances backed by the same Redis. Node A
 * has the value in L1 + L2 and node B has it cached in L1 (after a
 * cross-node read). Then Redis is paused, A.evict("k") is called, and the
 * test asserts that:
 *
 * <ol>
 *   <li>No invalidation message is observed on the topic (A correctly
 *       suppressed the publish because L2 delete failed).</li>
 *   <li>B's L1 still holds the value (no rogue invalidation arrived).</li>
 *   <li>A's L1 was still evicted — local state ends in a strict-or-equal
 *       state vs the (still-present) L2 entry.</li>
 * </ol>
 *
 * <p>The happy-path counterpart (evict succeeds → publish → B drops L1 →
 * B's @Cacheable read goes to L2/loader) is already covered by
 * {@code CrossNodeInvalidationIT.evictOnA_evictsL1OnB}.
 */
class EvictOrderingIT extends RedisTestBase {

    private static final Codec INVALIDATION_CODEC =
            new TypedJsonJacksonCodec(InvalidationMessage.class);

    private RedissonClient redissonA;
    private RedissonClient redissonB;
    private RedissonClient redissonObserver;
    private NearCache cacheA;
    private NearCache cacheB;
    private CircuitBreaker breakerA;
    private final String name = "evict-order-" + System.nanoTime();

    @BeforeEach
    void setUp() {
        // Fail-fast clients so the paused-Redis test fails over to the
        // breaker quickly rather than hanging on each probe.
        redissonA = newFailFastRedisson();
        redissonB = newRedisson();
        redissonObserver = newRedisson();
        breakerA = fastFailBreaker();
        cacheA = newNearCache(name, redissonA, breakerA, "node-A");
        cacheB = newNearCache(name, redissonB, defaultBreaker(), "node-B");
    }

    @AfterEach
    void tearDown() {
        try {
            cacheA.shutdown();
            cacheB.shutdown();
        } finally {
            try { redissonA.shutdown(); } catch (Exception ignored) {}
            try { redissonB.shutdown(); } catch (Exception ignored) {}
            try { redissonObserver.shutdown(); } catch (Exception ignored) {}
            // Always unpause in case a test failed mid-way.
            if (REDIS.isRunning()) {
                try { REDIS.getDockerClient().unpauseContainerCmd(REDIS.getContainerId()).exec(); }
                catch (Exception ignored) {}
            }
        }
    }

    @Test
    void l2EvictFailure_doesNotPublishInvalidation_andRemoteL1StaysCached() throws Exception {
        // 1. Seed both nodes' L1 via a normal write + cross-node read.
        cacheA.put("k", "v1");
        Awaitility.await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                assertThat(cacheB.get("k", String.class)).isEqualTo("v1"));
        assertThat(nativeOf(cacheB).getIfPresent("k")).isEqualTo("v1");

        // 2. Subscribe to the global invalidation topic from a third client
        //    so we can count any messages A publishes.
        AtomicInteger messages = new AtomicInteger();
        RTopic topic = redissonObserver.getTopic("cache:invalidate", INVALIDATION_CODEC);
        int listenerId = topic.addListener(InvalidationMessage.class,
                (channel, msg) -> {
                    if (name.equals(msg.cacheName())
                            && InvalidationMessage.OP_INVALIDATE.equals(msg.op())) {
                        messages.incrementAndGet();
                    }
                });

        try {
            // 3. Pause Redis. A's L2 delete will fail.
            REDIS.getDockerClient().pauseContainerCmd(REDIS.getContainerId()).exec();

            // 4. Drive a few cold reads through A so its breaker trips.
            //    (The breaker is what makes the delete attempt return fast
            //    rather than hanging on a TCP timeout.)
            for (int i = 0; i < 8; i++) {
                cacheA.get("warmup-" + i);
            }
            Awaitility.await().atMost(Duration.ofSeconds(8)).untilAsserted(() ->
                    assertThat(breakerA.getState())
                            .isIn(CircuitBreaker.State.OPEN, CircuitBreaker.State.FORCED_OPEN));

            int beforeEvict = messages.get();

            // 5. Evict on A. With Redis paused + breaker open, the L2 delete
            //    fails fast; per H5, no invalidation should be published.
            cacheA.evict("k");

            // 6. A's local L1 still ends up evicted (always-evict-local).
            assertThat(nativeOf(cacheA).getIfPresent("k")).isNull();

            // 7. Give pub/sub a generous window. B's L1 must still hold v1
            //    because no invalidation was published.
            Thread.sleep(800);
            assertThat(messages.get())
                    .as("L2 evict failed → publish must be suppressed")
                    .isEqualTo(beforeEvict);
            assertThat(nativeOf(cacheB).getIfPresent("k"))
                    .as("B's L1 must remain cached when A's evict didn't reach L2")
                    .isEqualTo("v1");
        } finally {
            topic.removeListener(listenerId);
        }
    }
}
