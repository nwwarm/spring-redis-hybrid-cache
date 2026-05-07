package io.github.nwwarm.hybridcache.core;

import io.github.nwwarm.hybridcache.config.CacheProperties;
import io.github.nwwarm.hybridcache.invalidation.InvalidationMessage;
import io.github.nwwarm.hybridcache.testfixtures.RedisTestBase;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SWR + reconciliation interaction.
 *
 * <p>Property under test: when reconciliation declares a miss (or any
 * invalidation reaches the cache), the SWR sidecar entry is cleaned up via
 * the Caffeine RemovalListener path — same path
 * {@code JitteredMaxIdleExpiry} uses. The next read after invalidation
 * does NOT see "stale value + dispatch refresh" — it sees "L1 miss → L2 →
 * loader" (a fresh load), because the L1 entry is gone and the sidecar
 * deadline has been removed.
 *
 * <p>This pins the integration: SWR's sidecar is not a parallel cache
 * that survives reconciliation invalidations; reconciliation's
 * {@code caffeineCache.clear()} drains everything through the same
 * removal-listener path.
 */
class SwrReconciliationIT extends RedisTestBase {

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
    void invalidation_clearsSidecarSoNextReadIsFreshLoad_notStaleAndRefresh() throws Exception {
        CircuitBreaker breaker = defaultBreaker();
        NearCache cache = newSwrNearCache("swr-recon", redisson, breaker, "node",
                meterRegistry, refreshExecutor,
                Duration.ofMinutes(10),  // ttl
                Duration.ofMillis(30),   // freshFor — short so we land in stale window
                Duration.ofMinutes(1),   // staleFor — wide so test doesn't race eviction
                new CacheProperties.Reconciliation(true,
                        Duration.ofSeconds(60), 5));

        cache.put("k", "v0");
        Thread.sleep(80);  // past freshFor; sidecar would classify STALE

        // Now invalidate: simulate an OP_INVALIDATE arriving from a peer.
        // The wire key is the already-stringified key ("k" — see how
        // publishInvalidation wires it in NearCache.put), not the namespaced
        // form. This is the same path reconciliation eventually drives
        // through caffeineCache.clear() — both go through the
        // RemovalListener.
        cache.handleInvalidation(InvalidationMessage.OP_INVALIDATE, "k", 0L);

        // After invalidation, the L1 entry is gone AND the sidecar deadline
        // has been cleared by the RemovalListener (skipping REPLACED is
        // honoured because EXPLICIT is the cause here, not REPLACED).
        assertThat(cache.swrSidecar().sidecarSize()).isZero();

        // Next read: L1 miss, L2 still has v0 (we didn't evict L2 here),
        // so this read populates L1 fresh from L2 and the sidecar deadline
        // is reset to "now + freshFor". No async refresh dispatch should
        // fire (the entry is fresh from this node's perspective).
        AtomicInteger loaderCalls = new AtomicInteger();
        Object v = cache.get("k", () -> {
            loaderCalls.incrementAndGet();
            return "from-loader";
        });
        assertThat(v).isEqualTo("v0");
        // No SWR dispatch — the entry is fresh again post-recovery.
        assertThat(refreshesStarted()).isZero();
    }

    @Test
    void clear_drainsSidecarForAllEntries() {
        CircuitBreaker breaker = defaultBreaker();
        NearCache cache = newSwrNearCache("swr-clear", redisson, breaker, "node",
                meterRegistry, refreshExecutor,
                Duration.ofMinutes(10),
                Duration.ofMinutes(5),
                Duration.ofMinutes(10),
                null);

        // Populate ten entries — each writes a sidecar deadline.
        for (int i = 0; i < 10; i++) cache.put("k" + i, "v" + i);
        assertThat(cache.swrSidecar().sidecarSize()).isEqualTo(10);

        // Reconciliation invalidation simulated as cache.clear() — same
        // recovery action a reconciliation miss triggers.
        cache.clear();

        // Clear's RemovalListener fires for every entry with cause=EXPLICIT
        // (Caffeine's invalidateAll), all of which are non-REPLACED, so the
        // sidecar drains entirely.
        assertThat(cache.swrSidecar().sidecarSize()).isZero();
    }

    private double refreshesStarted() {
        return meterRegistry.find("cache.swr.refreshes")
                .tag("status", "started").counter().count();
    }
}
