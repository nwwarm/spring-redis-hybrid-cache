package io.github.nwwarm.hybridcache.invalidation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.nwwarm.hybridcache.core.CacheKeys;

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
 *
 * <p>{@link #seq()} (added 0.5.0) is the per-cache publish sequence number,
 * read after the publisher's {@code INCR} on {@code <cache>:seq} and before
 * the publish itself. Receivers update {@code lastObservedSeq} via
 * {@code accumulateAndGet(seq, Math::max)} on every received message, and
 * the reconciliation cycle compares {@code lastObservedSeq} against the
 * canonical {@code <cache>:seq} value to detect missed messages. When
 * reconciliation is disabled on the publishing side, {@code seq} is left at
 * {@code 0} and the receiver's {@code Math.max} sees no advance — the wire
 * format is forward-compatible with 0.4.0 nodes that don't carry the field.
 *
 * <p>Wire compatibility: the {@code @JsonCreator} accepts a missing
 * {@code seq} field (a 0.4.0-published message arriving at a 0.5.0
 * subscriber) and defaults it to {@code 0}; the receiver treats that as
 * "no advance" via the {@code accumulateAndGet} above.
 */
public record InvalidationMessage(
        String nodeId,
        String cacheName,
        String op,
        String key,
        long seq) implements Serializable {

    /** Single-key invalidation. {@link #key()} carries the key to evict. */
    public static final String OP_INVALIDATE = "I";

    /** Cache-wide clear. {@link #key()} is null. */
    public static final String OP_CLEAR = "C";

    /**
     * Pre-0.5.0 constructor preserved for callers that don't carry a seq
     * (e.g., test fixtures and any code path where reconciliation isn't
     * involved). Equivalent to {@code new InvalidationMessage(..., 0L)} —
     * the 0 seq sentinel is what receivers see when reconciliation is
     * disabled on the publisher; {@code accumulateAndGet(0, Math::max)}
     * is a no-op against any non-negative observed seq.
     */
    public InvalidationMessage(String nodeId, String cacheName, String op, String key) {
        this(nodeId, cacheName, op, key, 0L);
    }

    /**
     * Jackson entry point. Tolerates a missing {@code seq} field on the
     * wire — required for forward-compat with 0.4.0 publishers and for
     * deserializer settings (e.g. {@code FAIL_ON_UNKNOWN_PROPERTIES}) that
     * would otherwise reject the omitted field. The {@code @JsonProperty}s
     * on the other fields are explicit so Jackson's record introspection
     * does not switch modes silently when the missing-field default kicks
     * in.
     */
    @JsonCreator
    public static InvalidationMessage of(
            @JsonProperty("nodeId") String nodeId,
            @JsonProperty("cacheName") String cacheName,
            @JsonProperty("op") String op,
            @JsonProperty("key") String key,
            @JsonProperty("seq") Long seq) {
        return new InvalidationMessage(nodeId, cacheName, op, key,
                seq == null ? 0L : seq);
    }
}
