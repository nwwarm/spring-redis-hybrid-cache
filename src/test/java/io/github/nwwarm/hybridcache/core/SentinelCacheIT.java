package io.github.nwwarm.hybridcache.core;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.nwwarm.hybridcache.config.CacheConfig;
import io.github.nwwarm.hybridcache.config.CacheProperties;
import io.github.nwwarm.hybridcache.invalidation.InvalidationDispatcher;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.codec.JsonJacksonCodec;
import org.redisson.config.Config;
import org.springframework.cache.caffeine.CaffeineCache;
import org.testcontainers.containers.ComposeContainer;

import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Topology test for {@link CacheProperties.Mode#SENTINEL}. Boots one master +
 * one replica + three sentinels via Docker Compose, then exercises the
 * production {@link CacheConfig#redissonClient} path against the sentinel set.
 *
 * <p>Sentinel-driven failover is intentionally not exercised here — see
 * TESTING.md for the manual procedure. Booting failover reliably under CI
 * timing is fragile; the readiness, cacheable, evict, and cross-node tests
 * are the contract the library commits to.
 */
class SentinelCacheIT {

    private static final List<String> SENTINEL_ADDRESSES = List.of(
            "redis://127.0.0.1:26379",
            "redis://127.0.0.1:26380",
            "redis://127.0.0.1:26381");

    private static final String MASTER_NAME = "mymaster";

    private static ComposeContainer COMPOSE;

    @BeforeAll
    static void startSentinelTopology() throws InterruptedException {
        COMPOSE = new ComposeContainer(
                new File("src/test/resources/sentinel/docker-compose.yml"));
        COMPOSE.start();
        // Wait for the master to be reachable AND for at least one sentinel
        // to be reporting the master as a known monitor target. Without this
        // the first Redisson SET races sentinel-driven master discovery.
        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(500))
                .until(SentinelCacheIT::topologyReady);
        // The probe used inside `topologyReady` shuts down its Redisson
        // client when the check passes, which transiently disturbs the
        // sentinel connection counts that Redisson's checkSentinelsList
        // safety check observes. A short settle window before tests start
        // reliably eliminates the race; without it the first test's
        // subscribe-during-NearCache-construction fails.
        Thread.sleep(2000);
    }

    @AfterAll
    static void stopSentinelTopology() {
        if (COMPOSE != null) COMPOSE.stop();
    }

    private static boolean topologyReady() {
        // The most reliable readiness signal is "can a production-wired
        // Redisson sentinel client successfully connect AND subscribe to a
        // topic" — both paths run SENTINEL SENTINELS internally, and the
        // subscribe path (used by InvalidationDispatcher) fails first when
        // gossip hasn't converged.
        RedissonClient probe = null;
        try {
            probe = buildProductionClient();
            probe.getBucket("sentinel-it:probe").set("ready");
            if (!"ready".equals(probe.<String>getBucket("sentinel-it:probe").get())) {
                return false;
            }
            // Exercise the pub/sub path that InvalidationDispatcher uses.
            int listenerId = probe.getTopic("sentinel-it:probe-topic")
                    .addListener(String.class, (channel, msg) -> {});
            probe.getTopic("sentinel-it:probe-topic").removeListener(listenerId);
            return true;
        } catch (Throwable e) {
            return false;
        } finally {
            if (probe != null) {
                try { probe.shutdown(); } catch (Throwable ignored) {}
            }
        }
    }

    /**
     * Like {@link #buildProductionClient()} but disables Redisson's
     * checkSentinelsList safety check. Used only by the cross-node test,
     * where two clients in the same JVM race the sentinel peer count
     * inside Redisson — a test-environment artifact, not a production
     * concern.
     */
    private static RedissonClient buildSentinelClientWithoutPeerCheck() {
        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .activateDefaultTyping(
                        BasicPolymorphicTypeValidator.builder()
                                .allowIfBaseType("io.github.nwwarm.")
                                .allowIfBaseType("java.util.")
                                .allowIfBaseType("java.time.")
                                .allowIfBaseType("java.lang.")
                                .build(),
                        ObjectMapper.DefaultTyping.NON_FINAL,
                        JsonTypeInfo.As.PROPERTY);
        Config cfg = new Config();
        cfg.setCodec(new JsonJacksonCodec(mapper));
        var sentinel = cfg.useSentinelServers()
                .setMasterName(MASTER_NAME)
                .setCheckSentinelsList(false);
        SENTINEL_ADDRESSES.forEach(sentinel::addSentinelAddress);
        return Redisson.create(cfg);
    }

    private static RedissonClient buildProductionClient() {
        CacheProperties.Server server = new CacheProperties.Server(
                CacheProperties.Mode.SENTINEL,
                null,
                SENTINEL_ADDRESSES,
                MASTER_NAME,
                null,
                null);
        CacheProperties props = new CacheProperties(
                server, null, null, "sentinel-it",
                List.of("io.github.nwwarm."), null, null, null, false, null, null, null, null, null);
        return new CacheConfig().redissonClient(props);
    }

    private static NearCache newNearCache(String name, RedissonClient client, String nodeId) {
        CacheProperties.CacheSpec spec = new CacheProperties.CacheSpec(
                CacheProperties.Tier.NEAR_CACHE,
                Duration.ofMinutes(10), 10_000,
                Duration.ofSeconds(2), Duration.ofSeconds(10),
                CacheProperties.Codec.JSON, null, null, null, 0.0, null, null, null, null);
        var caffeineNative = Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofMinutes(10))
                .maximumSize(spec.maximumSize())
                .recordStats()
                .build();
        CaffeineCache springCache = new CaffeineCache(name, caffeineNative, true);
        InvalidationDispatcher dispatcher = new InvalidationDispatcher(client, nodeId, new SimpleMeterRegistry());
        CircuitBreaker breaker = CircuitBreakerRegistry.ofDefaults()
                .circuitBreaker("sentinel-test-" + System.nanoTime());
        return new NearCache(springCache, spec, null, client, breaker, dispatcher,
                new SimpleMeterRegistry(), new KeyLogFormatter(false, "test"));
    }

    @Test
    void productionWiredClient_startsAgainstSentinel() {
        RedissonClient client = buildProductionClient();
        try {
            client.getBucket("sentinel-it:ping").set("pong");
            assertThat(client.<String>getBucket("sentinel-it:ping").get())
                    .isEqualTo("pong");
        } finally {
            client.shutdown();
        }
    }

    @Test
    void cacheableSemantics_putAndGet() {
        RedissonClient client = buildProductionClient();
        String name = "sentinel-cacheable-" + UUID.randomUUID().toString().substring(0, 8);
        NearCache cache = newNearCache(name, client, "node-A");
        try {
            cache.put("k", "v1");
            assertThat(nativeOf(cache).getIfPresent("k")).isEqualTo("v1");
            assertThat(cache.get("k", String.class)).isEqualTo("v1");
        } finally {
            cache.shutdown();
            client.shutdown();
        }
    }

    @Test
    void evict_invalidatesBothLayers() {
        RedissonClient client = buildProductionClient();
        String name = "sentinel-evict-" + UUID.randomUUID().toString().substring(0, 8);
        NearCache cache = newNearCache(name, client, "node-A");
        try {
            cache.put("k", "v");
            cache.evict("k");
            assertThat(nativeOf(cache).getIfPresent("k")).isNull();
            assertThat(cache.get("k")).isNull();
        } finally {
            cache.shutdown();
            client.shutdown();
        }
    }

    @Test
    void crossNodeInvalidation_overSentinel() {
        // Two RedissonClients in the same JVM trip Redisson's
        // SentinelConnectionManager.doConnect safety check when both
        // construct InvalidationDispatcher (RTopic.addListener) within the
        // same window: that path runs SENTINEL SENTINELS, and a brief
        // overlap can show <2 reachable peers. Production never hits this
        // (one client per JVM), so test 1's production-wiring assertion
        // remains; here client B disables the safety check so the
        // cross-node behavior itself can be verified.
        RedissonClient clientA = buildProductionClient();
        RedissonClient clientB = buildSentinelClientWithoutPeerCheck();
        String name = "sentinel-xnode-" + UUID.randomUUID().toString().substring(0, 8);
        NearCache a = newNearCache(name, clientA, "node-A");
        NearCache b = newNearCache(name, clientB, "node-B");
        try {
            a.put("k", "v1");
            Awaitility.await().atMost(Duration.ofSeconds(3)).untilAsserted(() ->
                    assertThat(b.get("k", String.class)).isEqualTo("v1"));

            a.put("k", "v2");
            Awaitility.await().atMost(Duration.ofSeconds(3)).untilAsserted(() ->
                    assertThat(nativeOf(b).getIfPresent("k")).isNull());
            assertThat(b.get("k", String.class)).isEqualTo("v2");
        } finally {
            a.shutdown();
            b.shutdown();
            clientA.shutdown();
            clientB.shutdown();
        }
    }

    @SuppressWarnings("unchecked")
    private static com.github.benmanes.caffeine.cache.Cache<Object, Object> nativeOf(NearCache cache) {
        return (com.github.benmanes.caffeine.cache.Cache<Object, Object>) cache.getNativeCache();
    }
}
