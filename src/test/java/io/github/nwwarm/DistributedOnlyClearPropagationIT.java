package io.github.nwwarm;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.Codec;
import org.redisson.codec.TypedJsonJacksonCodec;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link DistributedOnlyCache} subscribes to the same invalidation topic
 * {@link NearCache} uses, so a {@code clear()} on one node is visible on
 * peer nodes within a pub/sub round-trip — sub-second instead of waiting
 * up to {@code GENERATION_REFRESH_NANOS} (1s) for the next poll.
 *
 * <p>Coverage:
 * <ul>
 *   <li>Sub-100ms cross-node visibility of a clear via the topic.</li>
 *   <li>The 1s poll backstop still works when the topic message is missed.</li>
 *   <li>Per-key invalidation messages are accepted without error and take no
 *       action — distributed-only caches have no L1 to evict.</li>
 * </ul>
 */
class DistributedOnlyClearPropagationIT extends RedisTestBase {

    private static final String INVALIDATION_TOPIC = "cache:invalidate";
    private static final Codec TOPIC_CODEC = new TypedJsonJacksonCodec(InvalidationMessage.class);

    private RedissonClient redissonA;
    private RedissonClient redissonB;
    private DistributedOnlyCache cacheA;
    private DistributedOnlyCache cacheB;
    private final String name = "dist-clear-" + System.nanoTime();

    @BeforeEach
    void setUp() {
        redissonA = newRedisson();
        redissonB = newRedisson();
        cacheA = newDistributedCache(name, redissonA, defaultBreaker(), "node-A");
        cacheB = newDistributedCache(name, redissonB, defaultBreaker(), "node-B");
    }

    @AfterEach
    void tearDown() {
        cacheA.shutdown();
        cacheB.shutdown();
        redissonA.shutdown();
        redissonB.shutdown();
    }

    @Test
    void clearOnA_visibleOnBSubSecond() {
        // Seed and warm both nodes' generation caches at the current value.
        cacheA.put("k", "v1");
        assertThat(cacheB.get("k", String.class)).isEqualTo("v1");

        long bGenBefore = readGeneration(redissonB);

        long t0 = System.nanoTime();
        cacheA.clear();

        // B's locally cached generation should advance via the topic message,
        // not the 1s poll. Use a 100ms budget — well under the poll interval.
        Awaitility.await()
                .atMost(Duration.ofMillis(500))   // wide margin for CI variability
                .pollInterval(Duration.ofMillis(5))
                .untilAsserted(() ->
                        assertThat(cacheB.get("k", String.class))
                                .as("B should observe A's clear before the 1s poll")
                                .isNull());

        long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
        assertThat(elapsedMs)
                .as("propagation must be faster than the 1s poll backstop")
                .isLessThan(500);

        // Generation actually advanced (sanity: not just an L1 eviction trick).
        assertThat(readGeneration(redissonB)).isGreaterThan(bGenBefore);
    }

    @Test
    void pollBackstop_recoversWhenTopicMessageIsMissed() throws Exception {
        // Seed both nodes.
        cacheA.put("k", "v1");
        assertThat(cacheB.get("k", String.class)).isEqualTo("v1");

        // Bump the generation directly via Redis — bypassing A.clear() —
        // so no topic message is ever published. This simulates a dropped
        // pub/sub message: the generation has advanced, but B's listener
        // never heard about it. The 1s poll must catch up.
        redissonA.getAtomicLong(name + ":generation").incrementAndGet();

        // Force B past the cached refresh window. Without forceRefreshDue
        // we'd have to actually wait the 1s; with it, the next read polls.
        cacheB.forceRefreshDue();

        // B's next read polls Redis for the generation, sees the bump,
        // and reads from the new (empty) generation partition.
        Awaitility.await()
                .atMost(Duration.ofSeconds(2))
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() ->
                        assertThat(cacheB.get("k", String.class))
                                .as("poll backstop must surface the dropped clear")
                                .isNull());
    }

    @Test
    void perKeyInvalidationMessage_isAcceptedWithoutAction() throws InterruptedException {
        // Distributed-only caches have no L1, but they still subscribe so
        // they can receive OP_CLEAR. They must accept OP_INVALIDATE without
        // error — this exercises the trace-and-ignore branch.
        cacheA.put("k", "v1");
        assertThat(cacheB.get("k", String.class)).isEqualTo("v1");

        long bGenBefore = readGeneration(redissonB);

        // Hand-craft an OP_INVALIDATE message for cache `name`, key "k",
        // originating from a fictional node-X so neither A nor B self-skips.
        // Publish via a third client to keep the test independent of which
        // node "sent" it.
        RedissonClient publisher = newRedisson();
        try {
            RTopic topic = publisher.getTopic(INVALIDATION_TOPIC, TOPIC_CODEC);
            topic.publish(new InvalidationMessage(
                    "node-X", name, InvalidationMessage.OP_INVALIDATE, "k"));

            // Give the listener time to process.
            Thread.sleep(150);
        } finally {
            publisher.shutdown();
        }

        // Generation did not advance — OP_INVALIDATE is a no-op here.
        assertThat(readGeneration(redissonB)).isEqualTo(bGenBefore);

        // L2 was untouched by the message; the value still reads back.
        assertThat(cacheB.get("k", String.class)).isEqualTo("v1");
    }

    private long readGeneration(RedissonClient client) {
        return client.getAtomicLong(name + ":generation").get();
    }
}
