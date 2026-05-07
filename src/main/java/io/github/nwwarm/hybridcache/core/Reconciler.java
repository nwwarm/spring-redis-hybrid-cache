package io.github.nwwarm.hybridcache.core;

/**
 * Per-cache reconciliation hook (0.5.0). Caches that participate in
 * sequence-based reconciliation ({@link NearCache}, {@link DistributedOnlyCache})
 * implement this interface; {@link ReconciliationCoordinator} schedules
 * {@link #reconcile()} on every interval per registration.
 *
 * <h2>Contract</h2>
 *
 * <p>{@link #reconcile()} <b>must not throw</b>. The coordinator wraps every
 * call in {@code try { ... } catch (Throwable t) { ... }} as defense-in-depth,
 * but implementations are expected to catch their own exceptions and emit
 * skip metrics rather than propagating into the scheduler. An uncaught
 * exception inside a {@code ScheduledExecutorService} task disables the
 * scheduling slot — the periodic cycle would silently stop running. See
 * {@code # implementation guardrails / ## Reconciliation} item 1 in
 * {@code DESIGN.md}.
 *
 * <p>{@link #onMessageObserved(long)} is called by
 * {@link io.github.nwwarm.hybridcache.invalidation.InvalidationDispatcher}
 * for every received message that carries a non-zero seq. Implementations
 * use {@code accumulateAndGet(seq, Math::max)} on their internal
 * {@code lastObservedSeq} so out-of-order delivery (possible under cluster
 * reroute) does not walk the watermark backwards.
 */
public interface Reconciler {

    /** Cache name; matches {@code Cache.getName()}. */
    String getName();

    /**
     * Run a single reconciliation cycle: read {@code <cache>:seq} from
     * Redis (breaker-wrapped), compare against the locally observed
     * maximum, declare a miss / regression / skip, and run the recovery
     * action if needed. Must not throw.
     */
    void reconcile();

    /**
     * Called by {@code InvalidationDispatcher} on every received message
     * after self-skip and cache-name routing. Updates {@code lastObservedSeq}
     * via {@code accumulateAndGet(seq, Math::max)}. The dispatcher passes
     * {@code 0} when the wire message carries no seq (e.g., from a 0.4.0
     * publisher); zero is a no-op against any non-negative observed seq,
     * so this method is safe to call unconditionally.
     */
    void onMessageObserved(long seq);
}
