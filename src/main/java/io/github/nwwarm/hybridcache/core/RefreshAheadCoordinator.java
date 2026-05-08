package io.github.nwwarm.hybridcache.core;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.random.RandomGenerator;

/**
 * Per-cache refresh-ahead orchestrator (0.5.0).
 *
 * <p>RA is implemented as a probabilistic firing mode of SWR (see § 10
 * 0.5.0 decisions, {@code # implementation guardrails / ## Refresh-ahead}).
 * This coordinator:
 *
 * <ol>
 *   <li>Evaluates the {@link XFetchPredicate} on every read inside the
 *       SWR fresh window — the predicate is access-driven, so hotter keys
 *       roll the dice more often (no explicit hotness counter).</li>
 *   <li>Dispatches refresh tasks through the same {@link RefreshExecutor}
 *       that SWR uses, and through the same in-flight map that lives on
 *       {@link SwrSidecar}. A single key never has both an RA-triggered
 *       and an SWR-triggered refresh in flight simultaneously — the
 *       in-flight map collision returns {@code deduped} for whichever
 *       firing arrived second.</li>
 *   <li>Records its own metric family ({@code cache.refresh_ahead.refreshes}
 *       with {@code status} tag) so dashboards distinguish "RA fired
 *       speculatively" from "SWR fired because the entry was already
 *       stale." The two share the executor and the in-flight map; they
 *       do <em>not</em> share metric counters.</li>
 * </ol>
 *
 * <p>The SWR sidecar is the source of truth for fresh-window deadlines
 * and per-key in-flight tracking — this coordinator does not duplicate
 * either. § 10's "RA-as-a-mode-of-SWR" decision is enforced at this
 * level: every RA-specific code path either delegates to the sidecar or
 * touches only RA's own metrics + EWMA.
 */
public final class RefreshAheadCoordinator {

    private static final Logger log = LoggerFactory.getLogger(RefreshAheadCoordinator.class);

    private final String cacheName;
    private final long freshForNanos;
    private final double beta;
    private final SwrSidecar sidecar;
    private final RefreshExecutor refreshExecutor;
    private final CircuitBreaker breaker;
    private final LoaderRuntimeEwma loaderEwma;
    private final RandomGenerator rng;

    private final Counter refreshesStarted;
    private final Counter refreshesCompleted;
    private final Counter refreshesFailed;
    private final Counter refreshesDeduped;
    private final Counter refreshesSuppressedBreaker;

    /**
     * Production-path constructor. Uses {@link ThreadLocalRandom#current()}
     * for the XFetch random sample.
     */
    public RefreshAheadCoordinator(String cacheName,
                                    long freshForNanos,
                                    double beta,
                                    SwrSidecar sidecar,
                                    RefreshExecutor refreshExecutor,
                                    CircuitBreaker breaker,
                                    LoaderRuntimeEwma loaderEwma,
                                    MeterRegistry meterRegistry) {
        this(cacheName, freshForNanos, beta, sidecar, refreshExecutor,
                breaker, loaderEwma, meterRegistry, null);
    }

    /**
     * Constructor with explicit RNG — tests pass a seeded {@code SplittableRandom}
     * so stochastic assertions (the XFetch curve at TTL/2) are deterministic.
     * Production callers use the no-RNG overload.
     */
    public RefreshAheadCoordinator(String cacheName,
                                    long freshForNanos,
                                    double beta,
                                    SwrSidecar sidecar,
                                    RefreshExecutor refreshExecutor,
                                    CircuitBreaker breaker,
                                    LoaderRuntimeEwma loaderEwma,
                                    MeterRegistry meterRegistry,
                                    RandomGenerator rng) {
        if (freshForNanos <= 0L) {
            throw new IllegalArgumentException(
                    "freshForNanos must be positive (got " + freshForNanos + ")");
        }
        if (beta <= 0.0) {
            // Defense-in-depth: validator already rejects beta=0, but this
            // belt-and-suspenders catch keeps the invariant on the runtime
            // path so a programmatic-construction-site bypass cannot fire
            // a degenerate predicate.
            throw new IllegalArgumentException(
                    "beta must be > 0 (got " + beta + ")");
        }
        this.cacheName = cacheName;
        this.freshForNanos = freshForNanos;
        this.beta = beta;
        this.sidecar = sidecar;
        this.refreshExecutor = refreshExecutor;
        this.breaker = breaker;
        this.loaderEwma = loaderEwma;
        this.rng = rng;

        this.refreshesStarted = Counter.builder("cache.refresh_ahead.refreshes")
                .tag("cache", cacheName).tag("status", "started")
                .register(meterRegistry);
        this.refreshesCompleted = Counter.builder("cache.refresh_ahead.refreshes")
                .tag("cache", cacheName).tag("status", "completed")
                .register(meterRegistry);
        this.refreshesFailed = Counter.builder("cache.refresh_ahead.refreshes")
                .tag("cache", cacheName).tag("status", "failed")
                .register(meterRegistry);
        this.refreshesDeduped = Counter.builder("cache.refresh_ahead.refreshes")
                .tag("cache", cacheName).tag("status", "deduped")
                .register(meterRegistry);
        this.refreshesSuppressedBreaker = Counter.builder("cache.refresh_ahead.refreshes")
                .tag("cache", cacheName).tag("status", "suppressed_breaker_open")
                .register(meterRegistry);
    }

    /**
     * Read-path entry point. Called only on a FRESH classification — past
     * the fresh-until deadline, SWR's stale-window logic owns the read.
     *
     * <p>Evaluates the XFetch predicate; if it fires, delegates dispatch
     * to {@link #dispatch(String, Callable)} which uses the SwrSidecar's
     * in-flight map for de-duplication.
     *
     * @return {@code true} if a refresh was dispatched (or de-duplicated
     *         against a SWR/RA refresh already in flight); {@code false}
     *         if the predicate did not fire.
     */
    public boolean evaluateAndMaybeDispatch(String key, Callable<Object> refreshTask) {
        long deadline = sidecar.deadlineNanos(key);
        if (deadline == 0L) {
            // No tracked deadline (config-swap or RemovalListener race).
            // Skip — SWR's classify() already returned FRESH for the same
            // reason; matching that behavior keeps the read path coherent.
            return false;
        }
        long now = System.nanoTime();
        if (now >= deadline) {
            // Read landed exactly at or past the boundary. SWR's stale path
            // owns this — RA evaluates strictly inside the fresh window.
            return false;
        }
        long timeSinceWrite = freshForNanos - (deadline - now);
        long ewmaNanos = loaderEwma.getNanos();

        RandomGenerator generator = rng != null ? rng : ThreadLocalRandom.current();
        boolean fires = XFetchPredicate.shouldRefresh(
                timeSinceWrite, freshForNanos, ewmaNanos, beta, generator);
        if (!fires) return false;
        return dispatch(key, refreshTask);
    }

    /**
     * Dispatch a refresh task through the same pipeline SWR uses.
     *
     * <p>De-duplicates against SWR-triggered refreshes via the shared
     * in-flight map on {@link SwrSidecar}. The first caller (whether SWR
     * or RA) inserts the future and submits to the executor; subsequent
     * callers — including the other trigger — see the in-flight slot and
     * increment their respective {@code skipped}/{@code deduped} counter
     * without burning an executor slot.
     *
     * @return {@code true} if dispatch (or de-dup) ran; {@code false} if
     *         dispatch was suppressed because the executor is null
     *         (test-only).
     */
    public boolean dispatch(String key, Callable<Object> refreshTask) {
        if (refreshExecutor == null) {
            // Test-only construction without an executor. Increment the
            // started counter so the test can verify the predicate fired
            // and the call site reached dispatch — but do not submit.
            refreshesStarted.increment();
            return true;
        }

        ConcurrentMap<String, CompletableFuture<Object>> inflight = sidecar.inflightMap();
        CompletableFuture<Object> mine = new CompletableFuture<>();
        CompletableFuture<Object> theirs = inflight.putIfAbsent(key, mine);
        if (theirs != null) {
            // Either a SWR stale-refresh or a previous RA fire is already
            // running. Per § 10 / guardrail item 4: shared in-flight tracking
            // means a single key never has two refreshes in flight at once.
            refreshesDeduped.increment();
            return true;
        }

        if (breaker != null && breaker.getState() == CircuitBreaker.State.OPEN) {
            // Same up-front breaker check SwrSidecar's stale path uses —
            // suppress at the dispatch site rather than burning an
            // executor slot to throw CallNotPermittedException inside the
            // task body.
            refreshesSuppressedBreaker.increment();
            inflight.remove(key, mine);
            return true;
        }

        refreshesStarted.increment();
        refreshExecutor.submit(() -> {
            try {
                Object value = refreshTask.call();
                refreshesCompleted.increment();
                mine.complete(value);
            } catch (CallNotPermittedException e) {
                // Breaker tripped between dispatch and execution — same
                // semantics as the up-front check; surface as
                // suppressed_breaker_open so dashboards distinguishing
                // "Redis incident" from "loader bug" stay honest. Mirrors
                // SwrSidecar's behavior exactly.
                refreshesSuppressedBreaker.increment();
                mine.completeExceptionally(e);
            } catch (Throwable t) {
                // RA failures, like SWR failures, NEVER surface to the
                // calling reader. Catch Throwable, increment failure
                // metric, log WARN, do not rethrow. § 10 0.5.0 entry on
                // SWR refresh failures explicitly applies "same rule to
                // RA failures."
                refreshesFailed.increment();
                log.warn("RA refresh failed for cache '{}' key='{}'",
                        cacheName, key, t);
                mine.completeExceptionally(t);
            } finally {
                inflight.remove(key, mine);
            }
        });
        return true;
    }

    /** Test seam: width of the fresh window that drives the predicate. */
    public long freshForNanos() {
        return freshForNanos;
    }

    /** Test seam: β at construction. */
    public double beta() {
        return beta;
    }

    /** Test seam: returns the EWMA estimator. */
    public LoaderRuntimeEwma loaderEwma() {
        return loaderEwma;
    }
}
