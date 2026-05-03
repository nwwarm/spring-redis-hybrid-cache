package io.github.nwwarm;

/**
 * Caches that subscribe to the dispatcher's invalidation topic implement this.
 *
 * <p>The dispatcher routes by {@link #getName() cache name} and self-skips by
 * {@code nodeId} before invoking {@link #handleInvalidation(String, String)},
 * so implementations only see messages for their own cache from other nodes.
 *
 * <p>Op codes are the constants on {@link InvalidationMessage}
 * ({@link InvalidationMessage#OP_INVALIDATE OP_INVALIDATE},
 * {@link InvalidationMessage#OP_CLEAR OP_CLEAR}). An implementation that has
 * no L1 to evict (e.g. {@link DistributedOnlyCache}) may legitimately ignore
 * {@code OP_INVALIDATE} — the value still lives in L2, generation-scoped, and
 * remote evicts already removed it from L2 directly.
 */
interface InvalidationListener {

    String getName();

    void handleInvalidation(String op, String key);
}
