package io.github.nwwarm;

import java.io.Serializable;

/**
 * Wire format for invalidation events published over Redis pub/sub.
 *
 * <p>Self-skip via {@link #nodeId()} prevents the originating node from
 * acting on its own invalidation, which would cause an unnecessary local
 * miss and re-fetch from L2.
 *
 * <p>{@link #key()} is a {@code String} — keys are stringified at the cache
 * boundary (see {@link CacheKeys#stringify}) so the pub/sub wire format
 * cannot suffer cross-type mismatches between publisher and subscriber. A
 * {@code Long} key would otherwise serialize as a JSON number and deserialize
 * on the remote node as {@code Integer}; the subscriber's
 * {@code Caffeine.evict(Integer)} would not match the original
 * {@code Long}-keyed entry.
 */
public record InvalidationMessage(
        String nodeId,
        String cacheName,
        String op,
        String key) implements Serializable {

    /** Single-key invalidation. {@link #key()} carries the key to evict. */
    public static final String OP_INVALIDATE = "I";

    /** Cache-wide clear. {@link #key()} is null. */
    public static final String OP_CLEAR = "C";
}
