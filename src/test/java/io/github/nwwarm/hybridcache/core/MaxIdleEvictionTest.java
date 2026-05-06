package io.github.nwwarm.hybridcache.core;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Ticker;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the L1 max-idle behaviour. Two paths are exercised:
 * the native Caffeine pairing of {@code expireAfterWrite} +
 * {@code expireAfterAccess} (used by {@code HybridCacheManager} when
 * jitter is off) and the combined {@link JitteredMaxIdleExpiry} (used
 * when both jitter and max-idle are configured). Both paths share the
 * same observable contract: idle eviction at {@code maxIdle} since the
 * last access; TTL ceiling enforced even under continuous reads.
 *
 * <p>Time is controlled by a {@link FakeTicker} installed on the
 * Caffeine builder, so the tests are deterministic.
 */
class MaxIdleEvictionTest {

    /** Manually-advanced ticker; Caffeine reads time from this. */
    private static final class FakeTicker implements Ticker {
        private final AtomicLong nanos = new AtomicLong();

        @Override
        public long read() { return nanos.get(); }

        void advance(Duration d) { nanos.addAndGet(d.toNanos()); }
    }

    // -----------------------------------------------------------------------
    // Native path: expireAfterWrite(ttl) + expireAfterAccess(maxIdle).
    // -----------------------------------------------------------------------

    @Test
    void native_noAccessForMaxIdle_evictsEntry() {
        FakeTicker ticker = new FakeTicker();
        Cache<String, String> cache = Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofSeconds(10))
                .expireAfterAccess(Duration.ofSeconds(1))
                .ticker(ticker)
                .build();

        cache.put("k", "v");
        ticker.advance(Duration.ofMillis(1500));
        cache.cleanUp();

        assertThat(cache.getIfPresent("k")).isNull();
    }

    @Test
    void native_accessResetsIdleTimer() {
        FakeTicker ticker = new FakeTicker();
        Cache<String, String> cache = Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofSeconds(10))
                .expireAfterAccess(Duration.ofSeconds(1))
                .ticker(ticker)
                .build();

        cache.put("k", "v");

        // 0.8s after put — read, then idle timer resets to 0.
        ticker.advance(Duration.ofMillis(800));
        assertThat(cache.getIfPresent("k")).isEqualTo("v");

        // Another 0.8s — total 1.6s since put, but only 0.8s since last access.
        ticker.advance(Duration.ofMillis(800));
        assertThat(cache.getIfPresent("k"))
                .as("idle timer should have reset on previous read")
                .isEqualTo("v");

        // Stop accessing. After 1.5s of idleness the entry is evicted.
        ticker.advance(Duration.ofMillis(1500));
        cache.cleanUp();
        assertThat(cache.getIfPresent("k")).isNull();
    }

    @Test
    void native_ttlCeilingEnforced_underContinuousAccess() {
        FakeTicker ticker = new FakeTicker();
        Cache<String, String> cache = Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofSeconds(2))
                .expireAfterAccess(Duration.ofSeconds(1))
                .ticker(ticker)
                .build();

        cache.put("k", "v");

        // Read every 0.5s — under max-idle, so the idle timer never fires.
        for (int i = 0; i < 3; i++) {
            ticker.advance(Duration.ofMillis(500));
            assertThat(cache.getIfPresent("k"))
                    .as("alive at i=%d (elapsed=%dms)", i, (i + 1) * 500)
                    .isEqualTo("v");
        }
        // Now at t=1.5s. Advance well past TTL.
        ticker.advance(Duration.ofMillis(1000));
        cache.cleanUp();
        assertThat(cache.getIfPresent("k"))
                .as("evicted by TTL ceiling at t=2.5s")
                .isNull();
    }

    // -----------------------------------------------------------------------
    // Combined path: JitteredMaxIdleExpiry. Same observable contract.
    // We construct the Expiry with ratio=0.0 so the jittered TTL equals
    // the nominal TTL — that isolates the max-idle behaviour from the
    // statistical spread and lets us assert exact eviction times. The
    // ratio>0 path is exercised statistically by JitteredExpiryTest;
    // here we only need to verify the wiring of max-idle through the
    // combined Expiry.
    // -----------------------------------------------------------------------

    private static Cache<String, String> combinedCache(Duration ttl, Duration maxIdle, Ticker ticker) {
        JitteredMaxIdleExpiry expiry = new JitteredMaxIdleExpiry(
                ttl.toNanos(), 0.0, maxIdle.toNanos());
        return Caffeine.newBuilder()
                .expireAfter(expiry)
                .removalListener((k, v, cause) -> {
                    if (k != null && cause != RemovalCause.REPLACED) {
                        expiry.onRemoval(k);
                    }
                })
                .ticker(ticker)
                .<String, String>build();
    }

    @Test
    void combined_noAccessForMaxIdle_evictsEntry() {
        FakeTicker ticker = new FakeTicker();
        Cache<String, String> cache = combinedCache(
                Duration.ofSeconds(10), Duration.ofSeconds(1), ticker);

        cache.put("k", "v");
        ticker.advance(Duration.ofMillis(1500));
        cache.cleanUp();

        assertThat(cache.getIfPresent("k")).isNull();
    }

    @Test
    void combined_accessResetsIdleTimer() {
        FakeTicker ticker = new FakeTicker();
        Cache<String, String> cache = combinedCache(
                Duration.ofSeconds(10), Duration.ofSeconds(1), ticker);

        cache.put("k", "v");

        ticker.advance(Duration.ofMillis(800));
        assertThat(cache.getIfPresent("k")).isEqualTo("v");

        ticker.advance(Duration.ofMillis(800));
        assertThat(cache.getIfPresent("k"))
                .as("idle timer should have reset on previous read")
                .isEqualTo("v");

        ticker.advance(Duration.ofMillis(1500));
        cache.cleanUp();
        assertThat(cache.getIfPresent("k")).isNull();
    }

    @Test
    void combined_ttlCeilingEnforced_underContinuousAccess() {
        FakeTicker ticker = new FakeTicker();
        Cache<String, String> cache = combinedCache(
                Duration.ofSeconds(2), Duration.ofSeconds(1), ticker);

        cache.put("k", "v");

        for (int i = 0; i < 3; i++) {
            ticker.advance(Duration.ofMillis(500));
            assertThat(cache.getIfPresent("k"))
                    .as("alive at i=%d (elapsed=%dms)", i, (i + 1) * 500)
                    .isEqualTo("v");
        }
        ticker.advance(Duration.ofMillis(1000));
        cache.cleanUp();
        assertThat(cache.getIfPresent("k"))
                .as("evicted by TTL ceiling at t=2.5s")
                .isNull();
    }
}
