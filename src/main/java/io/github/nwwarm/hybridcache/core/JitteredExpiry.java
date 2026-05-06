package io.github.nwwarm.hybridcache.core;

import com.github.benmanes.caffeine.cache.Expiry;
import jakarta.annotation.Nonnull;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Caffeine {@link Expiry} that returns a per-entry jittered TTL.
 *
 * <p>Effective expiry on insert (and on update — see below) is
 * {@code ttl + uniform(-ttl·ratio, +ttl·ratio)} in nanoseconds, sampled
 * once at the call. The aim is to avoid coordinated reload spikes when a
 * batch of entries cached at the same instant would otherwise expire
 * together: jitter spreads expiration over a window of width
 * {@code 2·ttl·ratio} centred on the configured TTL.
 *
 * <p>This expiry is L1-only. The library never installs it on the L2
 * (Redis) side: jittering Redis TTLs would muddy the read-your-writes
 * coherence story across nodes without buying anything extra over the
 * already-jittered L1 expiry that drives reload behaviour.
 *
 * <h2>Update semantics — fresh jitter</h2>
 *
 * <p>{@link #expireAfterUpdate} returns a freshly jittered duration rather
 * than preserving the previous one. Three reasons:
 * <ul>
 *   <li>The zero-ratio code path uses {@code Caffeine.expireAfterWrite(ttl)},
 *       which resets the TTL window on every write. Picking a different
 *       update rule for the jittered builder would silently change
 *       semantics at ratio=0 vs ratio&gt;0.</li>
 *   <li>Preserving the previous remaining duration would let an entry
 *       rewritten just before its TTL fires expire immediately, which is
 *       the opposite of useful.</li>
 *   <li>Re-jittering on update gives the new value the same expiration
 *       distribution as a freshly inserted one — the property the feature
 *       exists to provide.</li>
 * </ul>
 *
 * <h2>Read semantics — preserve current</h2>
 *
 * <p>{@link #expireAfterRead} returns {@code currentDuration} so reads do
 * not extend an entry's lifetime. The library's TTL contract is "absolute
 * from the most recent write," not "sliding from last access."
 *
 * <h2>Random source</h2>
 *
 * <p>Uses {@link ThreadLocalRandom} — no injected seam. Tests cover the
 * spread statistically over a large sample.
 */
final class JitteredExpiry implements Expiry<Object, Object> {

    private final long ttlNanos;
    private final long jitterBoundNanos;

    JitteredExpiry(long ttlNanos, double ratio) {
        if (ttlNanos <= 0) {
            throw new IllegalArgumentException(
                    "ttlNanos must be positive (got " + ttlNanos + ")");
        }
        if (ratio < 0.0 || ratio > 0.5) {
            throw new IllegalArgumentException(
                    "ttlJitterRatio must be in [0.0, 0.5] (got " + ratio + ")");
        }
        this.ttlNanos = ttlNanos;
        // Round rather than truncate so a tiny positive ratio against a
        // small ttl doesn't silently degenerate to zero jitter.
        this.jitterBoundNanos = Math.round(ttlNanos * ratio);
    }

    private long jitteredNanos() {
        if (jitterBoundNanos == 0L) return ttlNanos;
        long delta = ThreadLocalRandom.current()
                .nextLong(-jitterBoundNanos, jitterBoundNanos + 1);
        return ttlNanos + delta;
    }

    @Override
    public long expireAfterCreate(@Nonnull Object key, @Nonnull Object value, long currentTime) {
        return jitteredNanos();
    }

    @Override
    public long expireAfterUpdate(@Nonnull Object key, @Nonnull Object value,
                                  long currentTime, long currentDuration) {
        return jitteredNanos();
    }

    @Override
    public long expireAfterRead(@Nonnull Object key, @Nonnull Object value,
                                long currentTime, long currentDuration) {
        return currentDuration;
    }
}
