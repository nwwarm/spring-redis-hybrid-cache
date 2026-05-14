package io.github.nwwarm.hybridcache.core;

import org.springframework.cache.Cache;

/**
 * Extension of Spring's {@link Cache} contract that exposes
 * {@link #clearImmediate()} alongside the standard {@link #clear()}.
 *
 * <p>{@code clear()} is the default and returns in O(1): it bumps a per-cache
 * generation counter so subsequent reads land on a fresh prefix and old
 * entries become logically invisible. The orphan keys decay with TTL.
 *
 * <p>{@code clearImmediate()} is the stronger variant: <b>when it returns,
 * no orphan keys remain in Redis under this cache's prefix on any
 * reachable shard.</b> The implementation bumps the generation first (so
 * concurrent reads see the new prefix immediately), then performs an eager
 * SCAN + UNLINK across every shard for the old generation. Use it when a
 * cache is cleared frequently relative to its TTL — repeated O(1) clears
 * leave proportionally more orphans behind, and on a large working set the
 * unreclaimed memory adds up.
 *
 * <p>Cost: bounded by the number of keys at the old generation. On a Redis
 * Cluster, the work is parallelised per shard; UNLINK is non-blocking on
 * the Redis side. Failure mid-iteration is propagated to the caller, but
 * since the generation has already been bumped, correctness is preserved —
 * surviving keys orphan and expire via TTL exactly as with {@link #clear()}.
 *
 * <p>This is a programmatic-only entry point; no annotation drives it.
 * Operators wire it into Actuator endpoints or application code that knows
 * it needs the stronger guarantee.
 */
public interface HybridCache extends Cache {

    /**
     * Eager clear: bumps the generation, then SCAN + UNLINKs every old-generation
     * value key on every reachable shard, then publishes a clear invalidation so
     * peers drop their L1 immediately.
     *
     * <p>For tiers without a distributed layer (e.g. {@code LOCAL_ONLY}) this is
     * indistinguishable from {@link #clear()} — there is no distributed state to
     * reconcile.
     *
     * <p>Failure handling (1.0.1): the underlying chain is async, so failures
     * surface via the per-tier failure counter and {@code log.warn} from
     * completion callbacks rather than throwing. The "no orphan keys when this
     * returns" contract still holds for callers on regular worker threads —
     * the chain is awaited there. Callers on a Redisson Netty event-loop thread
     * (Spring reactive {@code @CacheEvict(allEntries=true)}) get fire-and-forget
     * dispatch to avoid deadlocking the I/O thread.
     */
    void clearImmediate();
}
