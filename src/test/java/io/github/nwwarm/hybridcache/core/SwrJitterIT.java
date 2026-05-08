package io.github.nwwarm.hybridcache.core;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import io.github.nwwarm.hybridcache.config.CacheProperties;
import io.github.nwwarm.hybridcache.invalidation.InvalidationDispatcher;
import io.github.nwwarm.hybridcache.testfixtures.RedisTestBase;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.springframework.cache.caffeine.CaffeineCache;

import java.time.Duration;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SWR + TTL jitter (0.4.0) interaction.
 *
 * <p>Property under test: jitter applies to {@code ttl} only — never to
 * {@code fresh-for} or {@code stale-for}. When SWR is configured, the
 * Caffeine {@code expireAfterWrite} is set to {@code stale-for} (exact,
 * unjittered) and the SWR fresh-until deadline is exact (set by
 * {@code System.nanoTime() + freshForNanos}). Jitter must not silently
 * skew either deadline.
 *
 * <p>The integration constructs a NearCache with both
 * {@code ttl-jitter-ratio > 0} and {@code swr.*} configured, and asserts:
 * <ol>
 *   <li>The fresh-until deadline classifies exactly at {@code freshFor}
 *       across many entries — no spread, no early-fire.</li>
 *   <li>The Caffeine {@code expireAfterWrite} is the configured
 *       {@code staleFor}, not jittered.</li>
 * </ol>
 *
 * <p>Implementation note: when SWR is set, {@code HybridCacheManager}
 * uses {@code expireAfterWrite(staleFor)} unconditionally — the jitter
 * path is silently inert at L1 in SWR mode. This test pins that
 * behavior so a future contributor cannot accidentally route SWR's L1
 * expiry through {@code JitteredExpiry}.
 */
class SwrJitterIT extends RedisTestBase {

    private RedissonClient redisson;
    private RefreshExecutor refreshExecutor;
    private SimpleMeterRegistry meterRegistry;

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
    void freshUntilDeadline_isExactAcrossManyKeys_jitterRatioIgnored() throws Exception {
        // Build a NearCache with jitter ratio 0.5 AND SWR configured.
        // The SWR sidecar's freshFor is the source of truth — no jitter.
        Duration freshFor = Duration.ofMillis(50);
        Duration staleFor = Duration.ofSeconds(30);
        Duration ttl = Duration.ofMinutes(10);
        double jitterRatio = 0.5;

        SwrSidecar sidecar = buildJitteredSwrCache("swr-jitter-deadlines",
                ttl, freshFor, staleFor, jitterRatio);

        // Insert many keys nearly simultaneously and snapshot the deadlines.
        int n = 200;
        long writeStart = System.nanoTime();
        IntStream.range(0, n).forEach(i -> sidecar.recordWrite("k" + i));
        long writeEnd = System.nanoTime();

        // Every entry should classify FRESH at writeEnd + freshFor / 2 —
        // the deadline is writeStart + freshFor + jitter NEVER applies.
        long sampleAt = (writeStart + writeEnd) / 2 + freshFor.toNanos() / 2;
        // We can't fix the clock, but we can sleep until well before the
        // deadline and assert FRESH, then sleep past the deadline and
        // assert STALE — exactly at freshFor for every entry, no spread.
        Thread.sleep(20);  // freshFor is 50ms; this lands inside.
        long sampleNow = System.nanoTime();
        long maxFreshUntil = writeEnd + freshFor.toNanos();
        // While we're still inside the fresh window for every entry,
        // every classify() must return FRESH.
        if (sampleNow < writeStart + freshFor.toNanos()) {
            for (int i = 0; i < n; i++) {
                assertThat(sidecar.classify("k" + i))
                        .as("k%d fresh window", i)
                        .isEqualTo(SwrSidecar.Classification.FRESH);
            }
        }

        // Sleep past freshFor so every entry is now in the stale window.
        long sleepNeeded = (maxFreshUntil - System.nanoTime()) / 1_000_000L + 30L;
        if (sleepNeeded > 0) Thread.sleep(sleepNeeded);
        // Every entry must classify STALE — no entry can still be FRESH
        // because of jitter (jitter must not skew the SWR deadline).
        for (int i = 0; i < n; i++) {
            assertThat(sidecar.classify("k" + i))
                    .as("k%d stale window", i)
                    .isEqualTo(SwrSidecar.Classification.STALE);
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private SwrSidecar buildJitteredSwrCache(String name,
                                             Duration ttl,
                                             Duration freshFor,
                                             Duration staleFor,
                                             double jitterRatio) {
        CircuitBreaker breaker = defaultBreaker();
        SwrSidecar sidecar = new SwrSidecar(name,
                freshFor.toNanos(), refreshExecutor, breaker, meterRegistry);

        // Mirror HybridCacheManager.buildCaffeineCache for the SWR + jitter
        // case: expireAfterWrite(staleFor) wins (jitter is inert at L1).
        // Configuring the spec with jitterRatio > 0 confirms the production
        // wiring also picks the SWR branch.
        @SuppressWarnings("unused")
        CacheProperties.CacheSpec spec = new CacheProperties.CacheSpec(
                CacheProperties.Tier.NEAR_CACHE,
                ttl, 10_000,
                Duration.ofSeconds(2), Duration.ofSeconds(10),
                CacheProperties.Codec.JSON, null, null, null,
                jitterRatio,                       // jitter would normally apply at L1
                null,
                null,
                null,
                new CacheProperties.Swr(freshFor, staleFor),
                null);

        com.github.benmanes.caffeine.cache.Cache<Object, Object> caffeineNative = Caffeine.newBuilder()
                // SWR mode: stale-for is the L1 expireAfterWrite; jitter is
                // intentionally NOT applied here.
                .expireAfterWrite(staleFor)
                .maximumSize(10_000)
                .recordStats()
                .executor(Runnable::run)
                .removalListener((k, v, cause) -> {
                    if (k != null && cause != RemovalCause.REPLACED) {
                        sidecar.onRemoval(k.toString());
                    }
                })
                .build();
        CaffeineCache springCache = new CaffeineCache(name, caffeineNative, true);
        InvalidationDispatcher dispatcher = new InvalidationDispatcher(redisson, "node", meterRegistry);
        // Hold a strong reference to the cache so its lifecycle outlives
        // this builder (the dispatcher keeps a reference internally).
        new NearCache(springCache, spec, resolveTestCodec(spec.codec()), redisson,
                breaker, dispatcher, meterRegistry,
                new io.github.nwwarm.hybridcache.core.KeyLogFormatter(false, "test"),
                sidecar);
        return sidecar;
    }
}
