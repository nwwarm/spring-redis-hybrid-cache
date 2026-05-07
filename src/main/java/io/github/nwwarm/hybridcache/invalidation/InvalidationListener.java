package io.github.nwwarm.hybridcache.invalidation;

import io.github.nwwarm.hybridcache.core.DistributedOnlyCache;

/**
 * Caches that subscribe to the dispatcher's invalidation topic implement this.
 *
 * <p>The dispatcher routes by {@link #getName() cache name} and self-skips by
 * {@code nodeId} before invoking {@link #handleInvalidation(String, String, long)},
 * so implementations only see messages for their own cache from other nodes.
 *
 * <p>Op codes are the constants on {@link InvalidationMessage}
 * ({@link InvalidationMessage#OP_INVALIDATE OP_INVALIDATE},
 * {@link InvalidationMessage#OP_CLEAR OP_CLEAR}). An implementation that has
 * no L1 to evict (e.g. {@link DistributedOnlyCache}) may legitimately ignore
 * {@code OP_INVALIDATE} — the value still lives in L2, generation-scoped, and
 * remote evicts already removed it from L2 directly.
 *
 * <p>The {@code seq} parameter (added 0.5.0) is the publisher's
 * {@code <cache>:seq} value at the moment of publish. Listeners that
 * participate in reconciliation track the maximum observed seq via
 * {@code accumulateAndGet(seq, Math::max)}; listeners that don't simply
 * ignore it. {@code 0} is a sentinel for "publisher does not carry a seq"
 * (reconciliation disabled, or 0.4.0 publisher) and is a no-op under
 * {@code Math.max}.
 */
public interface InvalidationListener {

    String getName();

    void handleInvalidation(String op, String key, long seq);
}
