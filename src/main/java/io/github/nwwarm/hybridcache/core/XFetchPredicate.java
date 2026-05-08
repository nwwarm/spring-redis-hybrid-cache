package io.github.nwwarm.hybridcache.core;

import java.util.random.RandomGenerator;

/**
 * XFetch probability function for refresh-ahead (0.5.0).
 *
 * <p>Vattani, Cacheri, Vitale (<i>Optimal Probabilistic Cache Stampede
 * Prevention</i>, VLDB 2015): refresh fires probabilistically as a key
 * approaches its fresh-window boundary. The predicate
 *
 * <pre>{@code
 * now() − lastWrite + β · loaderRuntimeEstimate · (−ln(rand())) ≥ ttl
 * }</pre>
 *
 * <p>is evaluated on every read inside {@code [lastWrite, fresh-until)}.
 * The library uses {@code freshFor} (the SWR fresh-window width) as
 * {@code ttl} — that is the boundary at which the read-path classification
 * transitions from FRESH to STALE, and RA's whole point is to refresh
 * before that transition happens.
 *
 * <p>The exponential XFetch factor {@code −ln(U)} where {@code U ~ Uniform(0,1)}
 * is the canonical formulation; it yields an exponentially-distributed
 * "speculative refresh window" whose mean is {@code β · loaderRuntimeEstimate}.
 * Hotter keys (those read more frequently) roll the dice more often inside
 * the fresh window and therefore refresh-ahead more often — that is the
 * design's <b>access-driven hotness</b> (no per-key counter, no Caffeine
 * frequency-sketch reach-across, no parallel source of truth — see
 * § 10 0.5.0 decisions and {@code # implementation guardrails / ## Refresh-ahead}).
 *
 * <p>Pure function, no state. The {@code RandomGenerator} parameter is
 * threaded in by the caller so tests can fix a seed for stochastic
 * assertions.
 *
 * <h2>Properties</h2>
 *
 * <ul>
 *   <li><b>Monotonicity</b> in {@code timeSinceWriteNanos}: for a fixed
 *       random sample, the larger the time since write, the more likely
 *       the predicate fires. Asserted by
 *       {@code RefreshAheadMonotonicityPropertyTest}.</li>
 *   <li><b>Closed under early termination</b>: when
 *       {@code loaderRuntimeEstimate ≤ 0} (no measurement yet) the
 *       predicate returns {@code false} — there is no refresh-ahead
 *       behavior until the cache has run the loader at least once and
 *       recorded a duration. The first read after deploy never speculatively
 *       refreshes.</li>
 *   <li><b>Beta-zero rejection</b>: the predicate returns {@code false} for
 *       {@code β ≤ 0}. The validator catches β=0 at boot, but the
 *       defense-in-depth check here means a misconfigured β cannot fire a
 *       degenerate predicate at runtime.</li>
 * </ul>
 *
 * <p>See {@code DESIGN.md} § 2 RA architecture / § 10 0.5.0 decisions for
 * the rationale behind XFetch over alternatives.
 */
public final class XFetchPredicate {

    private XFetchPredicate() {}

    /**
     * Evaluates the XFetch refresh predicate for a single read.
     *
     * @param timeSinceWriteNanos {@code now() − lastWrite}, non-negative.
     *                            Negative values (clock skew between sample
     *                            and write timestamp) are clamped to zero.
     * @param freshForNanos       width of the fresh window — the boundary
     *                            beyond which SWR's stale-window logic
     *                            takes over. Must be positive (validator
     *                            enforces this for SWR-configured caches;
     *                            RA requires SWR).
     * @param loaderEwmaNanos     per-cache EWMA of measured loader
     *                            durations. Returns {@code false} when
     *                            zero or negative (no measurement yet).
     * @param beta                XFetch aggressiveness multiplier. Returns
     *                            {@code false} when {@code ≤ 0}.
     * @param rng                 random generator. Tests pass a seeded
     *                            {@code SplittableRandom}; production
     *                            callers pass {@link
     *                            java.util.concurrent.ThreadLocalRandom#current()}.
     * @return {@code true} if a refresh should fire on this read.
     */
    public static boolean shouldRefresh(long timeSinceWriteNanos,
                                         long freshForNanos,
                                         long loaderEwmaNanos,
                                         double beta,
                                         RandomGenerator rng) {
        if (loaderEwmaNanos <= 0L) return false;
        if (beta <= 0.0) return false;
        if (freshForNanos <= 0L) return false;
        long timeSinceWrite = Math.max(0L, timeSinceWriteNanos);

        // Sample U ~ Uniform(0, 1). Guard against U == 0 producing
        // -ln(0) = +∞: if rng.nextDouble() ever returns exactly 0 (the
        // contract is [0, 1) so that's possible), the predicate fires
        // unconditionally — which matches the math (the -ln(0) limit is
        // +∞, dominating any freshFor) but we want a deterministic,
        // bounded computation. Substituting Double.MIN_NORMAL keeps the
        // probability semantics intact while bounding the term.
        double u = rng.nextDouble();
        if (u <= 0.0) u = Double.MIN_NORMAL;

        // β · loaderRuntimeEstimate · (−ln(U)) is the speculative-refresh
        // window. The predicate fires when the entry is "close enough" to
        // freshFor that a refresh kicked off now would land before the
        // entry actually becomes stale.
        double xfetchTerm = beta * (double) loaderEwmaNanos * -Math.log(u);
        return (double) timeSinceWrite + xfetchTerm >= (double) freshForNanos;
    }
}
