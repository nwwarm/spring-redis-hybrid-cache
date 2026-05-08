package io.github.nwwarm.hybridcache.core;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Non-blocking variant of {@link LoaderGate} for the async/reactive path
 * (0.5.0).
 *
 * <p>The synchronous {@code LoaderGate} blocks on
 * {@code semaphore.tryAcquire(timeout)} when permits are exhausted. That is
 * the wrong primitive for the async path: blocking a Redisson Netty thread
 * waiting on a permit stalls every other Redis operation in the JVM.
 *
 * <p>This gate uses a queue-of-waiters instead. Acquire path:
 *
 * <ol>
 *   <li>Try {@code semaphore.tryAcquire(0)} immediately. On success, run
 *       the loader and release on completion (success or failure).</li>
 *   <li>On failure, register a {@link CompletableFuture}{@code <Void>}
 *       waiter at the tail of a FIFO queue and chain the loader off the
 *       waiter's completion. Each waiter has an
 *       {@code orTimeout(loaderAcquireTimeout)} deadline; on timeout the
 *       waiter completes exceptionally with
 *       {@link LoaderRejectedException}.</li>
 *   <li>Permit release polls the queue head and {@code complete(null)}s
 *       the next waiter — strict FIFO so the oldest waiter wins the
 *       permit, not the most recent. Without FIFO, sustained contention
 *       could starve old waiters until their deadline fires (guardrail
 *       item 4 of {@code # implementation guardrails / ## Async/reactive}).</li>
 * </ol>
 *
 * <p>Polling-based wait via {@code CompletableFuture.delayedExecutor + tryAcquire(0)}
 * was rejected at design time: it is the wrong primitive for non-blocking
 * flows and complicates failure modes (when does the poll loop terminate?
 * what if the permit is released between polls?). See
 * {@code DESIGN.md} § 10 0.5.0 entry on {@code AsyncLoaderGate}.
 *
 * <h2>Semaphore non-fairness vs queue FIFO</h2>
 *
 * <p>The underlying {@link Semaphore} is non-fair (matches {@link LoaderGate});
 * the FIFO concerns the <i>waiter queue</i>, not the semaphore's wait list.
 * Non-fair semaphores have higher throughput; the queue-of-waiters is what
 * delivers ordering, on the slow path.
 *
 * <h2>Sync/async permit-pool sharing</h2>
 *
 * <p>For a given cache, this async gate is constructed alongside the sync
 * {@link LoaderGate}; they own <i>separate</i> {@link Semaphore} instances
 * because the async path's "register a waiter and return" semantic does
 * not compose with a shared pool whose permits the sync path may already
 * have parked threads waiting on. A future enhancement could collapse the
 * two onto one semaphore via the wait queue, but the current sizing
 * {@code maxConcurrentLoaders} applies independently to sync and async
 * paths. Operators concerned about absolute aggregate concurrency should
 * rely on Redis-side rate-limiting; the cache's loader-gate is per-cache,
 * per-flavor protection.
 */
final class AsyncLoaderGate {

    private final String cacheName;
    private final Semaphore semaphore;
    private final Duration acquireTimeout;
    /** FIFO waiter queue. Each waiter is a CompletableFuture<Void> we complete to grant a permit. */
    private final Queue<CompletableFuture<Void>> waiters;
    private final Counter rejections;

    AsyncLoaderGate(String cacheName,
                    Integer maxConcurrentLoaders,
                    Duration loaderAcquireTimeout,
                    MeterRegistry meterRegistry) {
        this.cacheName = cacheName;
        if (maxConcurrentLoaders == null) {
            this.semaphore = null;
            this.acquireTimeout = null;
            this.waiters = null;
            this.rejections = null;
            return;
        }
        this.semaphore = new Semaphore(maxConcurrentLoaders, false);
        this.acquireTimeout = loaderAcquireTimeout;
        this.waiters = new ConcurrentLinkedQueue<>();
        // Distinct metric names from the sync LoaderGate, NOT a `flavor`
        // tag overlay. Two reasons:
        //   1. Same-name-different-tag counters confuse
        //      Search.counter() callers — a downstream `.find(name).counter()`
        //      may match either depending on registration order, which
        //      makes existing sync-path metric assertions flaky.
        //   2. Operators reading dashboards see "async loaders" as a
        //      separate row from "sync loaders," which is what they
        //      actually want — the two gates have separate permit pools
        //      and separate behaviors.
        Gauge.builder("cache.loaders.async.permits.available", semaphore, Semaphore::availablePermits)
                .tag("cache", cacheName).register(meterRegistry);
        Gauge.builder("cache.loaders.async.queue.depth", waiters, Queue::size)
                .tag("cache", cacheName).register(meterRegistry);
        this.rejections = Counter.builder("cache.loaders.async.rejections")
                .tag("cache", cacheName).register(meterRegistry);
    }

    /**
     * Runs the loader under the async gate. The returned future completes
     * with the loader's result on success, with {@link LoaderRejectedException}
     * on permit-acquisition timeout, or with the loader's own exception
     * (unchanged) on loader failure.
     *
     * <p>When the gate is unconfigured (no max-concurrent-loaders), the
     * loader is invoked directly and its future returned. Same control
     * flow as the pre-feature pass-through; the only overhead is the null
     * check on {@code semaphore}.
     *
     * @param key   key for the {@link LoaderRejectedException} message,
     *              not used for routing.
     * @param loader supplier producing the loader's future. Invoked at most
     *              once per call. Even when the gate is unconfigured,
     *              callers should not assume a particular thread runs the
     *              supplier; the cache layer's
     *              {@code thenComposeAsync(refreshExecutor)} hop is what
     *              keeps the loader off Redisson's Netty event loop.
     */
    <T> CompletableFuture<T> run(Object key, Supplier<CompletableFuture<T>> loader) {
        if (semaphore == null) {
            // No gate configured. Invoke directly; preserve any exception
            // the supplier itself throws as a failed future so async
            // callers get uniform error semantics.
            try {
                return loader.get();
            } catch (Throwable t) {
                CompletableFuture<T> failed = new CompletableFuture<>();
                failed.completeExceptionally(t);
                return failed;
            }
        }

        if (semaphore.tryAcquire()) {
            // Fast path: permit available immediately. Run loader; release
            // on completion (whenComplete fires for both success and
            // failure).
            return runWithRelease(loader);
        }

        // Slow path: enqueue a waiter, attach the loader to its
        // completion, and arm the deadline. Order matters — enqueue
        // BEFORE arming the timeout so a permit-release racing with the
        // tryAcquire above can't fire on an empty queue.
        CompletableFuture<Void> waiter = new CompletableFuture<>();
        waiters.add(waiter);

        // Race window: a permit may have been released between the
        // tryAcquire above and the queue.add. Re-check by trying to take
        // the head and acquire a permit ourselves — if successful, the
        // waiter we just added (or another that was head) wins now.
        drainOnePermitIfAvailable();

        // Each waiter has its own deadline. orTimeout completes the
        // waiter exceptionally with TimeoutException; we map that to
        // LoaderRejectedException and remove from the queue so the
        // waiter doesn't get a permit late.
        CompletableFuture<T> result = waiter
                .orTimeout(acquireTimeout.toNanos(), TimeUnit.NANOSECONDS)
                .handle((v, ex) -> {
                    if (ex != null) {
                        // Timeout (or cancellation): we did NOT acquire a
                        // permit. Remove from queue (best-effort — if
                        // permit-release already pulled us off, nothing
                        // happens). Increment rejections, surface as
                        // LoaderRejectedException.
                        waiters.remove(waiter);
                        rejections.increment();
                        throw new LoaderRejectedExceptionWrapper(
                                new LoaderRejectedException(cacheName, key));
                    }
                    return null;
                })
                .thenCompose(v -> runWithRelease(loader));

        // Unwrap LoaderRejectedExceptionWrapper so callers see the bare
        // exception (matches sync-path: LoaderRejectedException flows
        // through unchanged, NOT wrapped in CompletionException).
        return result.exceptionallyCompose(throwable -> {
            Throwable cause = unwrapCompletion(throwable);
            if (cause instanceof LoaderRejectedExceptionWrapper w) {
                CompletableFuture<T> failed = new CompletableFuture<>();
                failed.completeExceptionally(w.getCause());
                return failed;
            }
            CompletableFuture<T> failed = new CompletableFuture<>();
            failed.completeExceptionally(cause);
            return failed;
        });
    }

    private <T> CompletableFuture<T> runWithRelease(Supplier<CompletableFuture<T>> loader) {
        CompletableFuture<T> loaderFuture;
        try {
            loaderFuture = loader.get();
        } catch (Throwable t) {
            // Synchronous throw from the supplier: release immediately,
            // surface as failed future.
            release();
            CompletableFuture<T> failed = new CompletableFuture<>();
            failed.completeExceptionally(t);
            return failed;
        }
        // whenComplete fires for both success and failure; release in
        // either case.
        return loaderFuture.whenComplete((v, ex) -> release());
    }

    private void release() {
        // Order matters: release the permit FIRST, then try to grant it
        // to the next waiter. drainOnePermitIfAvailable acquires a
        // permit if available — without the release-first order, a
        // racing fast-path tryAcquire could not see the freed permit.
        semaphore.release();
        drainOnePermitIfAvailable();
    }

    /**
     * If a permit is available AND there is a waiter at the head of the
     * queue, hand the permit to the waiter by completing its future.
     * The waiter's downstream {@link CompletableFuture#thenCompose} chain
     * proceeds with {@link #runWithRelease}, which will release the
     * permit again on completion.
     *
     * <p>Concurrency: between {@code semaphore.tryAcquire()} and
     * {@code waiter.complete(null)}, another release may run. That's OK —
     * we hold the permit (the tryAcquire took it from the pool); the
     * subsequent release simply hands the permit to the next waiter or
     * back to the pool.
     */
    private void drainOnePermitIfAvailable() {
        if (waiters.isEmpty()) return;
        if (!semaphore.tryAcquire()) return;
        CompletableFuture<Void> next = waiters.poll();
        if (next == null) {
            // Race: head was taken by another release. Return the permit.
            semaphore.release();
            return;
        }
        // Hand the permit to the waiter by completing it. The downstream
        // chain attached to the waiter runs the loader; runWithRelease
        // will release on completion.
        if (!next.complete(null)) {
            // Waiter already completed (e.g. timed out); return the
            // permit and try the next one.
            semaphore.release();
            drainOnePermitIfAvailable();
        }
    }

    private static Throwable unwrapCompletion(Throwable t) {
        Throwable cur = t;
        while (cur instanceof java.util.concurrent.CompletionException
                || cur instanceof java.util.concurrent.ExecutionException) {
            Throwable cause = cur.getCause();
            if (cause == null || cause == cur) break;
            cur = cause;
        }
        return cur;
    }

    /** Internal sentinel: marks a {@link LoaderRejectedException} we generated so we can unwrap on the way out. */
    private static final class LoaderRejectedExceptionWrapper extends RuntimeException {
        LoaderRejectedExceptionWrapper(LoaderRejectedException cause) {
            super(cause);
        }
    }

    /** Test seam: number of currently-queued waiters. */
    int queueSize() {
        return waiters == null ? 0 : waiters.size();
    }

    /** Test seam: available permits. */
    int availablePermits() {
        return semaphore == null ? Integer.MAX_VALUE : semaphore.availablePermits();
    }

    /** Test seam: true iff the gate is configured (max-concurrent-loaders set). */
    boolean isConfigured() {
        return semaphore != null;
    }
}
