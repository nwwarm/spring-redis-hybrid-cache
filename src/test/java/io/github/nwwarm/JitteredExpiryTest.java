package io.github.nwwarm;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link JitteredExpiry}. The expiry implementation is a pure
 * function of its constructor arguments and {@link java.util.concurrent.ThreadLocalRandom};
 * the {@code currentTime}/{@code currentDuration} arguments to the {@link
 * com.github.benmanes.caffeine.cache.Expiry} methods do not feed into the
 * jittered-create / jittered-update return values, so a deterministic time
 * source is not needed — time is trivially fixed by passing the same constant.
 *
 * <p>Randomness comes from {@link java.util.concurrent.ThreadLocalRandom}.
 * The spec calls for that explicitly (not an injected seam), so the tests
 * use statistical assertions over a large sample (N=1000) chosen so the
 * false-failure probability is far below CI flake territory.
 */
class JitteredExpiryTest {

    private static final Duration TTL = Duration.ofSeconds(1);
    private static final long TTL_NANOS = TTL.toNanos();
    private static final int N = 1000;

    // -----------------------------------------------------------------------
    // ratio == 0 — exact TTL, no randomness. The library's manager doesn't
    // use JitteredExpiry at ratio=0 (it falls back to expireAfterWrite for
    // byte-identity with pre-0.4.0), but the class itself must still behave
    // correctly at the boundary so anyone constructing it directly gets a
    // sensible answer.
    // -----------------------------------------------------------------------

    @Test
    void ratioZero_returnsExactTtlOnCreate() {
        JitteredExpiry expiry = new JitteredExpiry(TTL_NANOS, 0.0);
        for (int i = 0; i < 10; i++) {
            assertThat(expiry.expireAfterCreate("k", "v", 0L)).isEqualTo(TTL_NANOS);
        }
    }

    @Test
    void ratioZero_returnsExactTtlOnUpdate() {
        JitteredExpiry expiry = new JitteredExpiry(TTL_NANOS, 0.0);
        for (int i = 0; i < 10; i++) {
            assertThat(expiry.expireAfterUpdate("k", "v", 0L, 12345L)).isEqualTo(TTL_NANOS);
        }
    }

    @Test
    void readPreservesCurrentDuration_atAnyRatio() {
        JitteredExpiry expiry = new JitteredExpiry(TTL_NANOS, 0.3);
        long current = TimeUnit.MILLISECONDS.toNanos(123);
        assertThat(expiry.expireAfterRead("k", "v", 0L, current)).isEqualTo(current);
    }

    // -----------------------------------------------------------------------
    // ratio > 0 — spread, bounds, mean. Statistical assertions over N=1000.
    // -----------------------------------------------------------------------

    @Test
    void create_spreadAcrossExpectedWindow() {
        double ratio = 0.2;
        JitteredExpiry expiry = new JitteredExpiry(TTL_NANOS, ratio);

        long[] durations = LongStream.range(0, N)
                .map(i -> expiry.expireAfterCreate("k" + i, "v", 0L))
                .toArray();

        assertWindowAndSpread(durations, ratio);
    }

    @Test
    void update_spreadAcrossExpectedWindow() {
        // Update applies fresh jitter on every call (see JitteredExpiry javadoc),
        // so the distribution must match the create distribution.
        double ratio = 0.3;
        JitteredExpiry expiry = new JitteredExpiry(TTL_NANOS, ratio);

        long[] durations = LongStream.range(0, N)
                .map(i -> expiry.expireAfterUpdate("k" + i, "v", 0L, TTL_NANOS))
                .toArray();

        assertWindowAndSpread(durations, ratio);
    }

    /**
     * Asserts the four spec invariants for the spread:
     * <ol>
     *   <li>Hard lower bound: every sample &ge; ttl·(1-ratio).</li>
     *   <li>Hard upper bound: every sample &le; ttl·(1+ratio).</li>
     *   <li>Spread covers &ge;50% of the 2·ttl·ratio window.</li>
     *   <li>Mean within 5% of ttl. With N=1000 and a uniform distribution,
     *       the standard error is bound·ttl/sqrt(3·N) — about 0.4% of ttl
     *       at ratio=0.2 — so a 5% tolerance is &gt;10σ and not a flake risk.</li>
     * </ol>
     */
    private static void assertWindowAndSpread(long[] durations, double ratio) {
        long bound = Math.round(TTL_NANOS * ratio);
        long min = LongStream.of(durations).min().orElseThrow();
        long max = LongStream.of(durations).max().orElseThrow();
        double mean = LongStream.of(durations).average().orElseThrow();

        // Hard bounds — the impl clamps the random delta to [-bound, +bound],
        // so any sample outside this window is a correctness bug, not flake.
        assertThat(min).as("min within lower bound")
                .isGreaterThanOrEqualTo(TTL_NANOS - bound);
        assertThat(max).as("max within upper bound")
                .isLessThanOrEqualTo(TTL_NANOS + bound);

        // Spread covers at least half the window.
        assertThat(max - min)
                .as("observed spread covers >=50%% of expected window 2*bound=%d", 2 * bound)
                .isGreaterThanOrEqualTo(bound);

        // Mean within 5% of TTL.
        double tolerance = TTL_NANOS * 0.05;
        assertThat(Math.abs(mean - TTL_NANOS))
                .as("mean within 5%% of TTL (mean=%.1f, ttl=%d)", mean, TTL_NANOS)
                .isLessThanOrEqualTo(tolerance);
    }

    // -----------------------------------------------------------------------
    // Constructor guards.
    // -----------------------------------------------------------------------

    @Test
    void nonPositiveTtl_rejected() {
        assertThatThrownBy(() -> new JitteredExpiry(0L, 0.2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ttlNanos must be positive");
        assertThatThrownBy(() -> new JitteredExpiry(-1L, 0.2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ttlNanos must be positive");
    }

    @Test
    void ratioOutOfRange_rejected() {
        assertThatThrownBy(() -> new JitteredExpiry(TTL_NANOS, -0.1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("[0.0, 0.5]");
        assertThatThrownBy(() -> new JitteredExpiry(TTL_NANOS, 0.51))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("[0.0, 0.5]");
    }
}
