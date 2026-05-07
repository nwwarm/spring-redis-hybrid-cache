package io.github.nwwarm.hybridcache.core;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Per-cache state for stale-while-revalidate (0.5.0).
 *
 * <p>Two pieces of state per key:
 *
 * <ol>
 *   <li>A <b>fresh-until deadline</b> in {@link #deadlines}, populated on
 *       every {@code put}/loader-completion write. Reads classify against
 *       this against {@code System.nanoTime()}: inside the deadline →
 *       fresh, past it → stale (caller dispatches refresh).</li>
 *   <li>An <b>in-flight map</b> in {@link #inflight} of refreshes that
 *       have been submitted to the shared {@link RefreshExecutor} but
 *       have not completed yet. Per-key single-flight: the first stale
 *       reader inserts a future and submits the refresh; subsequent stale
 *       readers in the same window see the in-flight entry and skip the
 *       dispatch (incrementing the {@code in_flight} suppression counter
 *       so dashboards can confirm the collapse is firing).</li>
 * </ol>
 *
 * <p>Sidecar pattern matches {@code JitteredMaxIdleExpiry}'s deadline
 * tracker (0.4.0) — the owning cache wires a Caffeine
 * {@code RemovalListener} that calls {@link #onRemoval(String)} for every
 * cause except {@code REPLACED} (REPLACED races the sidecar write the
 * caller has just performed).
 *
 * <p>The chosen single-flight primitive is {@code ConcurrentMap<String,
 * CompletableFuture<Object>>} — same shape that {@code DistributedOnlyCache}
 * uses for cold-load deduplication. {@code LoaderSemaphore} was rejected
 * (caps total loaders, doesn't dedupe per-key); {@code RLock} was rejected
 * (round-trips Redis on every refresh, overkill for best-effort
 * background work). See §10 0.5.0 decision log and
 * {@code # implementation guardrails / ## Stale-while-revalidate}.
 */
public final class SwrSidecar {

    private static final Logger log = LoggerFactory.getLogger(SwrSidecar.class);

    private final String cacheName;
    private final long freshForNanos;
    private final RefreshExecutor refreshExecutor;
    private final CircuitBreaker breaker;

    private final Map<String, Long> deadlines = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, CompletableFuture<Object>> inflight =
            new ConcurrentHashMap<>();

    private final Counter staleReturns;
    private final Counter refreshesStarted;
    private final Counter refreshesCompleted;
    private final Counter refreshesFailed;
    private final Counter refreshesSuppressedBreaker;
    private final Counter refreshesSkippedInFlight;

    /**
     * @param cacheName       used as the {@code cache} tag on every metric.
     * @param freshForNanos   width of the fresh window. Reads inside
     *                        {@code [write, write + freshFor)} return
     *                        synchronously without dispatching a refresh.
     * @param refreshExecutor shared executor for dispatch. May be null
     *                        only in tests that exercise the deadline /
     *                        single-flight logic without dispatching.
     * @param breaker         per-cache circuit breaker. Stale reads still
     *                        return stale when the breaker is open, but
     *                        the refresh dispatch is suppressed and
     *                        {@code suppressed_breaker_open} increments —
     *                        firing the loader against an unreachable
     *                        downstream during a Redis incident is exactly
     *                        the stampede SWR is meant to prevent.
     */
    public SwrSidecar(String cacheName,
                      long freshForNanos,
                      RefreshExecutor refreshExecutor,
                      CircuitBreaker breaker,
                      MeterRegistry meterRegistry) {
        if (freshForNanos <= 0) {
            throw new IllegalArgumentException(
                    "freshForNanos must be positive (got " + freshForNanos + ")");
        }
        this.cacheName = cacheName;
        this.freshForNanos = freshForNanos;
        this.refreshExecutor = refreshExecutor;
        this.breaker = breaker;

        this.staleReturns = Counter.builder("cache.swr.stale_returns")
                .tag("cache", cacheName).register(meterRegistry);
        this.refreshesStarted = Counter.builder("cache.swr.refreshes")
                .tag("cache", cacheName).tag("status", "started")
                .register(meterRegistry);
        this.refreshesCompleted = Counter.builder("cache.swr.refreshes")
                .tag("cache", cacheName).tag("status", "completed")
                .register(meterRegistry);
        this.refreshesFailed = Counter.builder("cache.swr.refreshes")
                .tag("cache", cacheName).tag("status", "failed")
                .register(meterRegistry);
        this.refreshesSuppressedBreaker = Counter.builder("cache.swr.refreshes")
                .tag("cache", cacheName).tag("status", "suppressed_breaker_open")
                .register(meterRegistry);
        this.refreshesSkippedInFlight = Counter.builder("cache.swr.refreshes.skipped")
                .tag("cache", cacheName).tag("reason", "in_flight")
                .register(meterRegistry);
    }

    /**
     * Records the fresh-until deadline for a write.
     *
     * <p>The owning cache must call this <b>before</b> the cached value
     * becomes visible to readers (item 2 of the SWR guardrails): a reader
     * landing between {@code caffeineCache.put} and a subsequent sidecar
     * write would see the new value with no fresh deadline, classify as
     * stale, and dispatch a refresh against a value that was just written
     * and is fresh by definition.
     */
    public void recordWrite(String key) {
        deadlines.put(key, System.nanoTime() + freshForNanos);
    }

    /**
     * Returns the classification for a read against the dual deadlines.
     * The owning cache calls this only when Caffeine returned a non-null
     * wrapper; reads past {@code stale-until} miss L1 entirely (Caffeine's
     * physical eviction) and never reach this code.
     */
    public Classification classify(String key) {
        Long deadline = deadlines.get(key);
        if (deadline == null) {
            // No SWR deadline tracked. Two cases:
            // (a) entry was inserted before SWR was wired — treat as fresh,
            //     no work to do (transitional, only relevant during a
            //     hot config swap).
            // (b) sidecar entry was cleaned up by the removal listener but
            //     Caffeine still returned a wrapper — race window. Treat
            //     as fresh; the next read will see eviction or a fresh
            //     entry.
            return Classification.FRESH;
        }
        return System.nanoTime() < deadline
                ? Classification.FRESH
                : Classification.STALE;
    }

    /**
     * Single-flight + dispatch. Called from the read path when
     * {@link #classify(String)} returned {@code STALE}. Increments
     * {@code cache.swr.stale_returns} unconditionally (the read is about
     * to return the stale value), then either submits a refresh task or
     * skips with {@code reason=in_flight}.
     *
     * <p>Order is critical: the in-flight check happens on the calling
     * thread <b>before</b> {@link RefreshExecutor#submit} so N concurrent
     * stale reads on the same key collapse to exactly one executor
     * submission. See guardrail item 3.
     *
     * <p>If the per-cache breaker is open at dispatch time, the refresh
     * is suppressed and {@code status=suppressed_breaker_open} increments.
     * Stale value still returned to the caller (refresh is best-effort).
     *
     * @param refreshTask the loader-aware refresh body. Must update the
     *                    sidecar deadline + the cached value + (for
     *                    NearCache) L2 + invalidation. Wrapped in
     *                    try/catch(Throwable) so failures never surface
     *                    out the executor — guardrail item 4.
     */
    public void maybeDispatchRefresh(String key, Callable<Object> refreshTask) {
        staleReturns.increment();

        if (refreshExecutor == null) {
            // Test-only construction without an executor; dispatch is a no-op
            // but the metric incremented above lets the test verify the
            // classification fired.
            return;
        }

        // Per-key single-flight: another stale read on this key may have
        // already submitted. The check is on the calling thread before
        // executor submission so we don't waste an executor slot on
        // duplicates (guardrail item 3).
        CompletableFuture<Object> mine = new CompletableFuture<>();
        CompletableFuture<Object> theirs = inflight.putIfAbsent(key, mine);
        if (theirs != null) {
            refreshesSkippedInFlight.increment();
            return;
        }

        // Breaker check is also on the calling thread. Submitting and
        // catching CallNotPermittedException inside the task body would
        // burn an executor slot for nothing; better to suppress at the
        // dispatch site.
        if (breaker != null && breaker.getState() == CircuitBreaker.State.OPEN) {
            refreshesSuppressedBreaker.increment();
            inflight.remove(key, mine);
            return;
        }

        refreshesStarted.increment();
        refreshExecutor.submit(() -> {
            try {
                Object value = refreshTask.call();
                refreshesCompleted.increment();
                mine.complete(value);
            } catch (CallNotPermittedException e) {
                // Breaker opened between dispatch and execution. Same
                // semantics as the up-front check; surface as
                // suppressed_breaker_open rather than failed so dashboards
                // distinguishing "Redis incident" from "loader bug" stay
                // honest.
                refreshesSuppressedBreaker.increment();
                mine.completeExceptionally(e);
            } catch (Throwable t) {
                // Catch Throwable per guardrail item 4: SWR refresh failures
                // NEVER surface to the calling reader. Stale value continues
                // to be served until physical eviction at stale-until. Log
                // WARN, increment failure counter, do not rethrow.
                refreshesFailed.increment();
                log.warn("SWR refresh failed for cache '{}' key='{}'",
                        cacheName, key, t);
                mine.completeExceptionally(t);
            } finally {
                inflight.remove(key, mine);
            }
        });
    }

    /**
     * Removes the per-key fresh-until deadline. Wired by the owning cache
     * from a Caffeine {@code RemovalListener} for every cause except
     * {@code REPLACED}. {@code REPLACED} fires alongside {@code put}; the
     * sidecar entry has already been overwritten by {@link #recordWrite}
     * on the new write, so cleaning up here would null out a fresh
     * deadline. See SWR guardrail item 1.
     */
    public void onRemoval(String key) {
        deadlines.remove(key);
    }

    /** Test seam: number of tracked fresh-until deadlines. */
    public int sidecarSize() {
        return deadlines.size();
    }

    /** Test seam: number of in-flight refreshes (post-submit, pre-completion). */
    public int inflightSize() {
        return inflight.size();
    }

    /** Test seam: width of the fresh window. */
    public long freshForNanos() {
        return freshForNanos;
    }

    /**
     * Read-path classification. {@code FRESH} means return the wrapper
     * directly; {@code STALE} means return the wrapper <em>and</em> call
     * {@link #maybeDispatchRefresh(String, Callable)}.
     */
    public enum Classification {
        FRESH,
        STALE
    }
}
