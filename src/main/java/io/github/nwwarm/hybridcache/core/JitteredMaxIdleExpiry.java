package io.github.nwwarm.hybridcache.core;

import com.github.benmanes.caffeine.cache.Expiry;
import jakarta.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Caffeine {@link Expiry} that combines per-entry TTL jitter with idle-based
 * eviction. Installed when both {@code ttl-jitter-ratio &gt; 0} and
 * {@code max-idle} are configured.
 *
 * <p>Caffeine forbids combining {@code expireAfterAccess(Duration)} with a
 * custom {@link Expiry}, so when both knobs are set, the manager installs
 * this single Expiry and lets it handle both effects.
 *
 * <h2>Semantics</h2>
 *
 * <p>Matches Caffeine's native {@code expireAfterWrite + expireAfterAccess}
 * pairing: an entry expires at the EARLIER of {@code lastWrite + ttl} or
 * {@code lastAccess + maxIdle}. With TTL jitter applied, "ttl" is the
 * per-entry jittered value sampled at the most recent write, not the
 * configured nominal value.
 *
 * <p>Implementation: the Expiry tracks an absolute "TTL deadline" per key
 * in a sidecar map. {@link #expireAfterCreate} and {@link #expireAfterUpdate}
 * sample a fresh jittered duration, store {@code currentTime + jittered}
 * as the deadline, and return {@code min(jittered, maxIdle)} so the entry
 * also respects the idle window from the moment of write.
 * {@link #expireAfterRead} looks up the deadline and returns
 * {@code min(maxIdle, deadline - currentTime)}, which clamps the per-read
 * extension to {@code maxIdle} while never letting the entry outlive its
 * TTL ceiling.
 *
 * <h2>Sidecar cleanup</h2>
 *
 * <p>The owner ({@code HybridCacheManager}) wires a Caffeine
 * {@code RemovalListener} that calls {@link #onRemoval(Object)} for every
 * removal cause except {@code REPLACED}. {@code REPLACED} fires alongside
 * {@code expireAfterUpdate}; cleaning up there would race the new
 * deadline that update has just written. Every other cause (EXPIRED,
 * EXPLICIT, SIZE, COLLECTED) leaves no further write to the sidecar, so
 * cleanup there is unconditionally safe.
 *
 * <h2>Concurrency</h2>
 *
 * <p>The sidecar is a {@link ConcurrentHashMap}; per-key writes and
 * lookups are atomic. There is a benign race where {@code expireAfterRead}
 * runs concurrently with the removal listener for the same key — the
 * listener wins, removes the deadline, and the read sees a missing entry.
 * In that case the read returns {@code maxIdleNanos} unclamped, but
 * Caffeine has already evicted the entry, so the return value is moot.
 */
final class JitteredMaxIdleExpiry implements Expiry<Object, Object> {

    private final long ttlNanos;
    private final long jitterBoundNanos;
    private final long maxIdleNanos;
    private final Map<Object, Long> deadlines = new ConcurrentHashMap<>();

    JitteredMaxIdleExpiry(long ttlNanos, double ratio, long maxIdleNanos) {
        if (ttlNanos <= 0) {
            throw new IllegalArgumentException(
                    "ttlNanos must be positive (got " + ttlNanos + ")");
        }
        if (ratio < 0.0 || ratio > 0.5) {
            throw new IllegalArgumentException(
                    "ttlJitterRatio must be in [0.0, 0.5] (got " + ratio + ")");
        }
        if (maxIdleNanos <= 0) {
            throw new IllegalArgumentException(
                    "maxIdleNanos must be positive (got " + maxIdleNanos + ")");
        }
        this.ttlNanos = ttlNanos;
        this.jitterBoundNanos = Math.round(ttlNanos * ratio);
        this.maxIdleNanos = maxIdleNanos;
    }

    private long jitteredTtl() {
        if (jitterBoundNanos == 0L) return ttlNanos;
        long delta = ThreadLocalRandom.current()
                .nextLong(-jitterBoundNanos, jitterBoundNanos + 1);
        return ttlNanos + delta;
    }

    @Override
    public long expireAfterCreate(@Nonnull Object key, @Nonnull Object value, long currentTime) {
        long jittered = jitteredTtl();
        deadlines.put(key, currentTime + jittered);
        return Math.min(jittered, maxIdleNanos);
    }

    @Override
    public long expireAfterUpdate(@Nonnull Object key, @Nonnull Object value,
                                  long currentTime, long currentDuration) {
        long jittered = jitteredTtl();
        deadlines.put(key, currentTime + jittered);
        return Math.min(jittered, maxIdleNanos);
    }

    @Override
    public long expireAfterRead(@Nonnull Object key, @Nonnull Object value,
                                long currentTime, long currentDuration) {
        Long deadline = deadlines.get(key);
        if (deadline == null) {
            return maxIdleNanos;
        }
        long ttlRemaining = deadline - currentTime;
        if (ttlRemaining <= 0) return 0L;
        return Math.min(maxIdleNanos, ttlRemaining);
    }

    /**
     * Removes the per-key deadline. Called from the Caffeine removal
     * listener wired by {@code HybridCacheManager} for every cause except
     * {@code REPLACED} — see class javadoc.
     */
    void onRemoval(Object key) {
        deadlines.remove(key);
    }
}
