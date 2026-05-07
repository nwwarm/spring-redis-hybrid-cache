package io.github.nwwarm.hybridcache.metrics;

import io.github.nwwarm.hybridcache.config.CacheProperties;
import io.github.nwwarm.hybridcache.core.DistributedOnlyCache;
import io.github.nwwarm.hybridcache.core.KeyLogFormatter;
import io.github.nwwarm.hybridcache.core.NearCache;
import io.github.nwwarm.hybridcache.invalidation.InvalidationDispatcher;
import io.github.nwwarm.hybridcache.invalidation.InvalidationMessage;
import io.github.nwwarm.hybridcache.testfixtures.RedisTestBase;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.search.Search;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.Codec;
import org.redisson.codec.TypedJsonJacksonCodec;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Counters around the invalidation pub/sub channel:
 * {@code cache.invalidations.published}, {@code cache.invalidations.received},
 * and {@code cache.invalidations.received.unknown}. Inspired by Hazelcast's
 * NearCacheStats.invalidations.
 *
 * <p>Coverage:
 * <ul>
 *   <li>Each publish path (NearCache.put/evict/clear, DistributedOnlyCache.clear)
 *       increments {@code published} with the right {@code op} tag exactly once
 *       per call.</li>
 *   <li>A clearImmediate publishes a single OP_CLEAR — no double counting on the
 *       eager-clear path.</li>
 *   <li>Two-context: when A publishes, B's dispatcher increments {@code received},
 *       A's dispatcher does not (self-skip).</li>
 *   <li>Forged message naming a cache the receiver does not host increments
 *       {@code received.unknown} but not {@code received} — useful for catching
 *       deployment drift.</li>
 * </ul>
 */
class InvalidationMetricsIT extends RedisTestBase {

    private static final String INVALIDATION_TOPIC = "cache:invalidate";
    private static final Codec TOPIC_CODEC = new TypedJsonJacksonCodec(InvalidationMessage.class);

    @Nested
    class PublishedCounter {

        private RedissonClient redisson;
        private SimpleMeterRegistry registry;
        private final String name = "pub-metrics-" + System.nanoTime();

        @BeforeEach
        void setUp() {
            redisson = newRedisson();
            registry = new SimpleMeterRegistry();
        }

        @AfterEach
        void tearDown() {
            redisson.shutdown();
        }

        @Test
        void nearCache_putIncrementsPublishedPut() {
            NearCache cache = newNearCache(name, redisson, defaultBreaker(), "node-A",
                    CacheProperties.Codec.JSON, registry);
            try {
                cache.put("k", "v");
                assertThat(publishedCount(registry, name, "put")).isEqualTo(1.0);
                assertThat(publishedCount(registry, name, "evict")).isZero();
                assertThat(publishedCount(registry, name, "clear")).isZero();
            } finally {
                cache.shutdown();
            }
        }

        @Test
        void nearCache_evictIncrementsPublishedEvict() {
            NearCache cache = newNearCache(name, redisson, defaultBreaker(), "node-A",
                    CacheProperties.Codec.JSON, registry);
            try {
                cache.put("k", "v");
                cache.evict("k");
                assertThat(publishedCount(registry, name, "put")).isEqualTo(1.0);
                assertThat(publishedCount(registry, name, "evict")).isEqualTo(1.0);
            } finally {
                cache.shutdown();
            }
        }

        @Test
        void nearCache_clearIncrementsPublishedClear() {
            NearCache cache = newNearCache(name, redisson, defaultBreaker(), "node-A",
                    CacheProperties.Codec.JSON, registry);
            try {
                cache.clear();
                assertThat(publishedCount(registry, name, "clear")).isEqualTo(1.0);
                assertThat(publishedCount(registry, name, "put")).isZero();
                assertThat(publishedCount(registry, name, "evict")).isZero();
            } finally {
                cache.shutdown();
            }
        }

        @Test
        void nearCache_clearImmediatePublishesExactlyOnceWithClearOp() {
            NearCache cache = newNearCache(name, redisson, defaultBreaker(), "node-A",
                    CacheProperties.Codec.JSON, registry);
            try {
                cache.put("k", "v");
                cache.clearImmediate();
                // clearImmediate publishes a single OP_CLEAR — same as clear().
                // Do not also count it as put/evict.
                assertThat(publishedCount(registry, name, "put")).isEqualTo(1.0);
                assertThat(publishedCount(registry, name, "clear")).isEqualTo(1.0);
                assertThat(publishedCount(registry, name, "evict")).isZero();
            } finally {
                cache.shutdown();
            }
        }

        @Test
        void distributedOnly_clearIncrementsPublishedClear() {
            // Construct a DistributedOnlyCache + dispatcher sharing one
            // registry, since RedisTestBase.newDistributedCache builds its
            // own SimpleMeterRegistry internally.
            CacheProperties.CacheSpec spec = new CacheProperties.CacheSpec(
                    CacheProperties.Tier.DISTRIBUTED_ONLY,
                    Duration.ofMinutes(10), 10_000,
                    Duration.ofSeconds(2), Duration.ofSeconds(10),
                    CacheProperties.Codec.JSON, null, null, null, 0.0, null, null, null, null);
            InvalidationDispatcher dispatcher = new InvalidationDispatcher(
                    redisson, "node-A", registry);
            DistributedOnlyCache cache = new DistributedOnlyCache(
                    name, spec, null, redisson, defaultBreaker(), dispatcher,
                    registry, new KeyLogFormatter(false, "test"));
            try {
                cache.clear();
                assertThat(publishedCount(registry, name, "clear")).isEqualTo(1.0);
            } finally {
                cache.shutdown();
            }
        }
    }

    @Nested
    class ReceivedCounter {

        private RedissonClient redissonA;
        private RedissonClient redissonB;
        private SimpleMeterRegistry registryA;
        private SimpleMeterRegistry registryB;
        private NearCache cacheA;
        private NearCache cacheB;
        private final String name = "recv-metrics-" + System.nanoTime();

        @BeforeEach
        void setUp() {
            redissonA = newRedisson();
            redissonB = newRedisson();
            registryA = new SimpleMeterRegistry();
            registryB = new SimpleMeterRegistry();
            cacheA = newNearCache(name, redissonA, defaultBreaker(), "node-A",
                    CacheProperties.Codec.JSON, registryA);
            cacheB = newNearCache(name, redissonB, defaultBreaker(), "node-B",
                    CacheProperties.Codec.JSON, registryB);
        }

        @AfterEach
        void tearDown() {
            cacheA.shutdown();
            cacheB.shutdown();
            redissonA.shutdown();
            redissonB.shutdown();
        }

        @Test
        void aPublishesPut_bIncrementsReceivedInvalidate_aDoesNot() {
            cacheA.put("k", "v1");

            // B's dispatcher should observe the message after pub/sub round-trip.
            Awaitility.await()
                    .atMost(Duration.ofSeconds(2))
                    .pollInterval(Duration.ofMillis(20))
                    .untilAsserted(() ->
                            assertThat(receivedCount(registryB, name, "invalidate"))
                                    .as("B should observe A's invalidate")
                                    .isEqualTo(1.0));

            // A self-skipped: its own dispatcher did not increment received.
            assertThat(receivedCount(registryA, name, "invalidate"))
                    .as("A must self-skip its own message")
                    .isZero();

            // Sanity: published incremented on A, not on B.
            assertThat(publishedCount(registryA, name, "put")).isEqualTo(1.0);
            assertThat(publishedCount(registryB, name, "put")).isZero();
        }

        @Test
        void aPublishesClear_bIncrementsReceivedClear_aDoesNot() {
            cacheA.clear();

            Awaitility.await()
                    .atMost(Duration.ofSeconds(2))
                    .pollInterval(Duration.ofMillis(20))
                    .untilAsserted(() ->
                            assertThat(receivedCount(registryB, name, "clear")).isEqualTo(1.0));

            assertThat(receivedCount(registryA, name, "clear")).isZero();
            assertThat(publishedCount(registryA, name, "clear")).isEqualTo(1.0);
        }
    }

    @Nested
    class UnknownCacheCounter {

        private RedissonClient redissonHost;
        private RedissonClient publisher;
        private SimpleMeterRegistry registry;
        private NearCache hostCache;
        private final String hostedName = "hosted-" + System.nanoTime();
        private final String ghostName = "ghost-" + System.nanoTime();

        @BeforeEach
        void setUp() {
            redissonHost = newRedisson();
            publisher = newRedisson();
            registry = new SimpleMeterRegistry();
            // The host node only knows about `hostedName`. The forged message
            // will name `ghostName`, which must hit the dispatcher's
            // unknown-cache path.
            hostCache = newNearCache(hostedName, redissonHost, defaultBreaker(),
                    "node-host", CacheProperties.Codec.JSON, registry);
        }

        @AfterEach
        void tearDown() {
            hostCache.shutdown();
            redissonHost.shutdown();
            publisher.shutdown();
        }

        @Test
        void forgedMessage_forUnknownCache_incrementsUnknownNotReceived() {
            // Publish from a different node id so the host doesn't self-skip.
            RTopic topic = publisher.getTopic(INVALIDATION_TOPIC, TOPIC_CODEC);
            topic.publish(new InvalidationMessage(
                    "node-forged", ghostName, InvalidationMessage.OP_INVALIDATE, "k"));

            Awaitility.await()
                    .atMost(Duration.ofSeconds(2))
                    .pollInterval(Duration.ofMillis(20))
                    .untilAsserted(() ->
                            assertThat(unknownReceivedCount(registry, ghostName, "invalidate"))
                                    .as("forged message must increment received.unknown")
                                    .isEqualTo(1.0));

            // Crucially: the regular received counter for the hosted cache
            // must not have moved — that would mean we'd misrouted unknown
            // messages onto the wrong cache's metric.
            assertThat(receivedCount(registry, hostedName, "invalidate"))
                    .as("regular received counter for the hosted cache must not move")
                    .isZero();
            assertThat(receivedCount(registry, ghostName, "invalidate"))
                    .as("regular received counter must not exist for the ghost cache")
                    .isZero();
        }
    }

    // ---------- helpers ----------

    private static double publishedCount(MeterRegistry registry, String cacheName, String op) {
        return counterValue(registry, "cache.invalidations.published", cacheName, op);
    }

    private static double receivedCount(MeterRegistry registry, String cacheName, String op) {
        return counterValue(registry, "cache.invalidations.received", cacheName, op);
    }

    private static double unknownReceivedCount(MeterRegistry registry, String cacheName, String op) {
        return counterValue(registry, "cache.invalidations.received.unknown", cacheName, op);
    }

    private static double counterValue(MeterRegistry registry, String name,
                                       String cacheName, String op) {
        Counter c = Search.in(registry)
                .name(name)
                .tag("cache", cacheName)
                .tag("op", op)
                .counter();
        return c == null ? 0.0 : c.count();
    }
}
