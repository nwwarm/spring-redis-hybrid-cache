package io.github.nwwarm.hybridcache.core;

/**
 * The three outcomes of a single per-cache reconciliation comparison.
 * Extracted from {@link NearCache#reconcile()} and
 * {@link DistributedOnlyCache#reconcile()} so the classification rule can
 * be unit-tested without standing up Redis or constructing a cache.
 *
 * <p>The classification is intentionally a tiny pure function: given
 * {@code (redisSeq, observedSeq, missTolerance)} return one of
 * {@link #NO_MISS}, {@link #MISS}, {@link #REGRESSION}. The cache's
 * {@code reconcile()} method makes the decision via {@link #classify} and
 * dispatches the recovery action (clear local L1 / force generation refresh
 * / reset watermark) accordingly. Branch coverage on the rule lives on this
 * class, not on the cache.
 */
public enum ReconciliationDecision {

    /** {@code redisSeq − observedSeq <= missTolerance}. No action. */
    NO_MISS,

    /**
     * {@code redisSeq − observedSeq > missTolerance}. The local subscriber
     * missed at least {@code (missTolerance + 1)} publishes since the last
     * cycle. Recovery: drop local L1 ({@code NEAR_CACHE}) or force a
     * generation pointer refresh ({@code DISTRIBUTED_ONLY}); jump
     * {@code observedSeq} to {@code redisSeq} so the next cycle does not
     * re-fire on the same gap.
     */
    MISS,

    /**
     * {@code redisSeq < observedSeq}. The canonical counter went backward —
     * almost always operator-driven (manual {@code DEL}, {@code FLUSHALL}).
     * <b>Not a miss</b>: amplifying operator action into a cache-clearing
     * cascade across every node is the wrong response. Recovery: reset
     * {@code observedSeq} to {@code redisSeq}, increment a regression
     * metric, log WARN once per event.
     */
    REGRESSION;

    /**
     * Classify a single comparison. Pure function, no side effects, no
     * dependence on system clock or external state — every input
     * deterministically maps to one outcome.
     *
     * @param redisSeq      the canonical {@code <cache>:seq} value just
     *                      read from Redis
     * @param observedSeq   the locally observed maximum from received
     *                      {@code InvalidationMessage}s
     * @param missTolerance per-cache configured tolerance; {@code >= 0}
     */
    public static ReconciliationDecision classify(long redisSeq, long observedSeq, int missTolerance) {
        if (redisSeq < observedSeq) return REGRESSION;
        long delta = redisSeq - observedSeq;
        if (delta > missTolerance) return MISS;
        return NO_MISS;
    }
}
