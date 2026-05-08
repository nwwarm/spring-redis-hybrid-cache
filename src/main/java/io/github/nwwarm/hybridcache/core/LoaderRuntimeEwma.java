package io.github.nwwarm.hybridcache.core;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-cache exponentially-weighted moving average of loader durations
 * (0.5.0).
 *
 * <p>RA's XFetch predicate ({@link XFetchPredicate}) needs an estimate of
 * how long the loader takes so the speculative-refresh window
 * ({@code β · loaderRuntimeEstimate · (−ln U)}) has a meaningful
 * magnitude. The estimate is updated on <b>every</b> loader completion
 * (cold load, SWR refresh, RA refresh — sync or async) so RA's predicate
 * tracks loader behavior without per-key bookkeeping.
 *
 * <p>Per-cache, not per-key. § 10 0.5.0 entry on RA hotness signal pins
 * this: a per-key estimate would duplicate Caffeine's frequency sketch
 * and is the same anti-pattern the 0.4.0 frecency-tracker rejection
 * argued against — parallel source of truth, drift potential, no real
 * benefit. The cache-level average is what XFetch's published analysis
 * assumes.
 *
 * <h2>Concurrency</h2>
 *
 * <p>{@link AtomicLong#accumulateAndGet} folds concurrent samples into the
 * running EWMA without locking. The first non-zero sample <em>seeds</em>
 * the EWMA (rather than blending with zero) so a cache that has just run
 * its first loader gets a meaningful estimate immediately, not
 * {@code alpha · firstSample}. Subsequent samples blend per the standard
 * EWMA formula.
 *
 * <h2>Smoothing factor</h2>
 *
 * <p>{@code alpha} controls how quickly the EWMA tracks recent samples.
 * Default {@code 0.2} biases toward recent measurements (tracks load-time
 * shifts in roughly 5 samples) while still smoothing single-sample noise.
 * Not currently exposed as a knob — operators tune RA aggressiveness via
 * {@code beta}; the EWMA smoothing is a library-internal constant and
 * exposing it would only multiply the surface area of "RA fires too
 * often / not often enough."
 */
public final class LoaderRuntimeEwma {

    private final double alpha;
    private final AtomicLong nanos = new AtomicLong(0L);

    public LoaderRuntimeEwma() {
        this(0.2);
    }

    public LoaderRuntimeEwma(double alpha) {
        if (alpha <= 0.0 || alpha > 1.0) {
            throw new IllegalArgumentException(
                    "alpha must be in (0, 1] (got " + alpha + ")");
        }
        this.alpha = alpha;
    }

    /**
     * Records a loader-duration sample. Negative or zero samples are
     * silently dropped — a clock that walked backwards or a sub-nanosecond
     * "loader" is not useful signal.
     */
    public void record(long elapsedNanos) {
        if (elapsedNanos <= 0L) return;
        long sample = elapsedNanos;
        nanos.accumulateAndGet(sample, (cur, s) -> {
            if (cur <= 0L) {
                // Seed: first measurement bypasses the blend so the EWMA
                // is initialized to the actual sample, not alpha · sample.
                return s;
            }
            return Math.round(alpha * (double) s + (1.0 - alpha) * (double) cur);
        });
    }

    /** Returns the current EWMA in nanos, or {@code 0} if no sample has been recorded. */
    public long getNanos() {
        return nanos.get();
    }

    /** Test seam: alpha factor at construction. */
    public double alpha() {
        return alpha;
    }
}
