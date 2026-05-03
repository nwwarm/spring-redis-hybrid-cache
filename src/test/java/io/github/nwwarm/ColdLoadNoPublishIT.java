package io.github.nwwarm;

import io.micrometer.core.instrument.search.Search;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.Codec;
import org.redisson.codec.TypedJsonJacksonCodec;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cold-load completion does not publish an invalidation. Other nodes have no
 * stale L1 state to drop; the L2 write plus their lazy-load on next read
 * achieves coherence with no invalidation traffic.
 *
 * <p>Verifies, in order:
 * <ul>
 *   <li>A cold load on node A produces zero invalidation messages on the topic
 *       for that cache+key.</li>
 *   <li>Node B's first read of the same key after A's load goes through L2
 *       (not the loader) and populates B's own L1.</li>
 *   <li>{@code put} on A still publishes — existing semantics preserved.</li>
 *   <li>{@code evict} on A still publishes — existing semantics preserved.</li>
 *   <li>The {@code cache.invalidations.suppressed.cold_load} counter increments
 *       on each cold load and not on put/evict.</li>
 * </ul>
 *
 * <p>Topic message counting uses a parallel Redisson client subscribing to the
 * same topic the dispatcher publishes on. We count messages whose
 * {@code (cacheName, key)} match the test's target — agnostic of the
 * publishing node — so the absence of a publish on the cold-load path is a
 * direct, observable property rather than inferred from L1 state.
 */
class ColdLoadNoPublishIT extends RedisTestBase {

    private static final String INVALIDATION_TOPIC = "cache:invalidate";
    private static final Codec TOPIC_CODEC = new TypedJsonJacksonCodec(InvalidationMessage.class);

    private RedissonClient redissonA;
    private RedissonClient redissonB;
    private RedissonClient redissonObserver;
    private SimpleMeterRegistry meterRegistryA;
    private NearCache cacheA;
    private NearCache cacheB;
    private RTopic observerTopic;
    private int observerListenerId;
    private final List<InvalidationMessage> observed = new CopyOnWriteArrayList<>();
    private final String name = "cold-no-pub-" + System.nanoTime();

    @BeforeEach
    void setUp() {
        redissonA = newRedisson();
        redissonB = newRedisson();
        redissonObserver = newRedisson();

        meterRegistryA = new SimpleMeterRegistry();
        cacheA = newNearCache(name, redissonA, defaultBreaker(), "node-A",
                CacheProperties.Codec.JSON, meterRegistryA);
        cacheB = newNearCache(name, redissonB, defaultBreaker(), "node-B");

        observerTopic = redissonObserver.getTopic(INVALIDATION_TOPIC, TOPIC_CODEC);
        observerListenerId = observerTopic.addListener(InvalidationMessage.class,
                (channel, msg) -> observed.add(msg));
    }

    @AfterEach
    void tearDown() {
        try {
            observerTopic.removeListener(observerListenerId);
        } catch (Exception ignored) {
            // Best-effort cleanup; the observer client is being shut down anyway.
        }
        cacheA.shutdown();
        cacheB.shutdown();
        redissonA.shutdown();
        redissonB.shutdown();
        redissonObserver.shutdown();
    }

    @Test
    void coldLoadOnA_publishesNoInvalidation_andBLazyLoadsFromL2() {
        // Cold load on A.
        Object loaded = cacheA.get("k", () -> "v1");
        assertThat(loaded).isEqualTo("v1");

        // Suppression counter increments by exactly one for this cache+load.
        assertThat(suppressedColdLoadCount(meterRegistryA, name)).isEqualTo(1.0);

        // Give pub/sub plenty of time to deliver any message that *would* have
        // been published. We assert absence after waiting, not just immediately.
        sleepQuietly(Duration.ofMillis(500));
        assertThat(messagesFor(name, "k"))
                .as("no invalidation message should be published on cold load")
                .isEmpty();

        // B's L1 is still empty — nothing told it to populate or evict.
        assertThat(nativeOf(cacheB).getIfPresent("k")).isNull();

        // B's read goes to L2 and finds A's value, then populates B's L1.
        // The loader passed to B should NOT run, because L2 is warm.
        Object onB = cacheB.get("k", () -> {
            throw new AssertionError("B's loader must not run; value should come from L2");
        });
        assertThat(onB).isEqualTo("v1");
        assertThat(nativeOf(cacheB).getIfPresent("k")).isEqualTo("v1");
    }

    @Test
    void putOnA_stillPublishesInvalidation() {
        cacheA.put("k", "v1");

        Awaitility.await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                assertThat(messagesFor(name, "k"))
                        .as("put must publish exactly one invalidation")
                        .hasSize(1));

        // put does not go through the cold-load path; the suppression counter
        // stays at zero.
        assertThat(suppressedColdLoadCount(meterRegistryA, name)).isEqualTo(0.0);
    }

    @Test
    void evictOnA_stillPublishesInvalidation() {
        // Seed L2 so the evict has something to delete; that ensures the
        // L2-delete-then-publish path runs (a no-op delete still publishes
        // here because deleteFromL2 returns true on success regardless of
        // prior presence, but seeding makes the test intent explicit).
        cacheA.put("k", "v1");
        Awaitility.await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                assertThat(messagesFor(name, "k")).hasSize(1));

        observed.clear();

        cacheA.evict("k");

        Awaitility.await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                assertThat(messagesFor(name, "k"))
                        .as("evict must publish exactly one invalidation")
                        .hasSize(1));

        // evict does not go through the cold-load path either.
        assertThat(suppressedColdLoadCount(meterRegistryA, name)).isEqualTo(0.0);
    }

    // ---------- helpers ----------

    private List<InvalidationMessage> messagesFor(String cacheName, String key) {
        return observed.stream()
                .filter(m -> cacheName.equals(m.cacheName()))
                .filter(m -> key.equals(m.key()))
                .filter(m -> InvalidationMessage.OP_INVALIDATE.equals(m.op()))
                .toList();
    }

    private static double suppressedColdLoadCount(SimpleMeterRegistry registry, String cacheName) {
        return Search.in(registry)
                .name("cache.invalidations.suppressed.cold_load")
                .tag("cache", cacheName)
                .counter()
                .count();
    }

    private static void sleepQuietly(Duration d) {
        try {
            Thread.sleep(d.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
