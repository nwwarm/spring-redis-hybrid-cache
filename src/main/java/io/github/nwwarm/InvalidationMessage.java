package io.github.nwwarm;

import java.io.Serializable;

/**
 * Wire format for invalidation events published over Redis pub/sub.
 *
 * <p>Self-skip via {@link #nodeId()} prevents the originating node from
 * acting on its own invalidation, which would cause an unnecessary local
 * miss and re-fetch from L2.
 */
public record InvalidationMessage(
        String nodeId,
        String cacheName,
        String op,
        Object key) implements Serializable {

    /** Single-key invalidation. {@link #key()} carries the key to evict. */
    public static final String OP_INVALIDATE = "I";

    /** Cache-wide clear. {@link #key()} is null. */
    public static final String OP_CLEAR = "C";
}
