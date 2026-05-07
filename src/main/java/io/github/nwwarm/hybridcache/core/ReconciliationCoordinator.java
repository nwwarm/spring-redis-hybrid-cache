package io.github.nwwarm.hybridcache.core;

import io.github.nwwarm.hybridcache.config.CacheProperties;
import io.github.nwwarm.hybridcache.invalidation.InvalidationDispatcher;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Owns the {@link ThreadPoolTaskScheduler} that drives per-cache reconciliation
 * cycles, and is the registration entry point for {@link Reconciler}s.
 *
 * <h2>Lifecycle</h2>
 *
 * <p>Implements {@link SmartLifecycle} with the same phase as
 * {@link InvalidationDispatcher} ({@link #LIFECYCLE_PHASE} —
 * {@code Integer.MAX_VALUE - 1024}) so Spring stops this bean before the
 * bean-destruction phase that disposes the {@code RedissonClient}. A
 * pending cycle that fires after Redisson teardown would surface as
 * {@code RedissonShutdownException} or breaker-open; the cache's
 * {@link Reconciler#reconcile()} catches both, but the coordinator must
 * actually stop the scheduler so the JVM exits cleanly. Item 5 of the
 * {@code # implementation guardrails / ## Reconciliation} block in
 * {@code DESIGN.md}.
 *
 * <p>Registration is idempotent — calling {@link #register} twice for the
 * same cache name replaces the previous schedule (the test fixtures lean
 * on this when the same cache is rebuilt across {@code @BeforeEach}).
 *
 * <h2>Thread sizing</h2>
 *
 * <p>Pool size 2 by default. Reconciliation work is read-only and
 * latency-tolerant: each cycle is one {@code RAtomicLong.get()}, scheduled
 * at the cache's configured interval (default 60s). Two threads let the
 * cycles run independently without serializing — a slow cycle on cache A
 * does not delay cache B's cycle. Sizing higher would idle most threads;
 * sizing to 1 would couple all caches' cycle latencies.
 *
 * <h2>Failure isolation</h2>
 *
 * <p>The scheduled task wraps {@link Reconciler#reconcile()} in
 * try/catch(Throwable). The {@code Reconciler} contract already says
 * implementations must not throw, but uncaught exceptions inside a
 * {@code ScheduledExecutorService} task disable the scheduling slot —
 * the cycle would silently stop running. The wrapper here is
 * defense-in-depth.
 */
public class ReconciliationCoordinator implements SmartLifecycle {

    /**
     * Same phase as {@link InvalidationDispatcher#LIFECYCLE_PHASE}. Spring
     * stops phases in descending order; sharing the phase puts the cache
     * layer's two SmartLifecycle beans in the same shutdown stride, ahead
     * of the bean-destruction phase that disposes Redisson.
     */
    public static final int LIFECYCLE_PHASE = Integer.MAX_VALUE - 1024;

    private static final Logger log = LoggerFactory.getLogger(ReconciliationCoordinator.class);

    private final MeterRegistry meterRegistry;
    private final ThreadPoolTaskScheduler scheduler;
    private final ConcurrentMap<String, Registration> registrations = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);

    public ReconciliationCoordinator(MeterRegistry meterRegistry) {
        this(meterRegistry, 2);
    }

    public ReconciliationCoordinator(MeterRegistry meterRegistry, int poolSize) {
        this.meterRegistry = meterRegistry;
        ThreadPoolTaskScheduler ts = new ThreadPoolTaskScheduler();
        ts.setPoolSize(poolSize);
        ts.setThreadNamePrefix("hybrid-cache-reconcile-");
        ts.setDaemon(true);
        // Continue scheduling after a task throws. Spring's default for
        // ThreadPoolTaskScheduler does propagate exceptions (the underlying
        // ScheduledThreadPoolExecutor disables the slot on uncaught throw).
        // setRemoveOnCancelPolicy is a separate concern — leaving the default
        // is fine because we never cancel individual schedules; we shut the
        // whole scheduler down on stop().
        ts.setWaitForTasksToCompleteOnShutdown(false);
        ts.initialize();
        this.scheduler = ts;
        // Constructor leaves the bean ready to accept registrations but
        // marks isRunning=true immediately so direct construction in test
        // fixtures works without a separate start() call. Spring's later
        // start() during refresh CAS-no-ops; same pattern as
        // InvalidationDispatcher.
        this.running.set(true);
    }

    /**
     * Schedule periodic {@link Reconciler#reconcile()} for this cache.
     * No-op if {@code reconciliation.enabled=false} on the spec — a defensive
     * check on top of {@link CacheProperties.CacheSpec} validation that lets
     * callers register every cache unconditionally.
     */
    public void register(Reconciler reconciler, CacheProperties.Reconciliation cfg) {
        if (cfg == null || !cfg.enabled()) return;
        Duration interval = cfg.interval();
        Registration prev = registrations.remove(reconciler.getName());
        if (prev != null) {
            prev.cancel();
        }
        Counter cyclesScheduled = Counter.builder("cache.reconciliation.cycles.scheduled")
                .tag("cache", reconciler.getName())
                .register(meterRegistry);
        Runnable task = () -> {
            try {
                cyclesScheduled.increment();
                reconciler.reconcile();
            } catch (Throwable t) {
                // Reconciler.reconcile() must not throw. If it does anyway,
                // catch here — uncaught exceptions inside a ScheduledExecutor
                // disable the schedule slot. Item 1 of the reconciliation
                // guardrails. We don't increment a separate metric here because
                // the cache's reconcile() already does on its own catch-all
                // path; this is the very-last-line defense.
                log.warn("Reconciliation task for cache '{}' threw past the cache's"
                                + " own catch path; swallowed at coordinator boundary"
                                + " to keep the schedule slot alive (item 1 guardrail)",
                        reconciler.getName(), t);
            }
        };
        // scheduleAtFixedRate would let cycles overlap if a cycle takes longer
        // than the interval (rare — a cycle is one GET — but possible during a
        // Redis incident). scheduleWithFixedDelay measures from completion of
        // the previous cycle, which is the safer shape.
        ScheduledFuture<?> future = scheduler.scheduleWithFixedDelay(
                task, java.time.Instant.now().plus(interval), interval);
        registrations.put(reconciler.getName(), new Registration(future, reconciler));
        log.debug("Scheduled reconciliation for cache '{}' every {} (tolerance={})",
                reconciler.getName(), interval, cfg.missTolerance());
    }

    public void deregister(String cacheName) {
        Registration prev = registrations.remove(cacheName);
        if (prev != null) prev.cancel();
    }

    /**
     * Test seam: drives a single cycle synchronously for a registered cache.
     * Bypasses the scheduler so tests don't have to wait for the configured
     * interval. Idempotent and safe to call concurrently with the scheduled
     * cycle — both paths land in the cache's own concurrent reconcile().
     */
    public void triggerCycleNow(String cacheName) {
        Registration r = registrations.get(cacheName);
        if (r == null) return;
        r.reconciler.reconcile();
    }

    @Override
    public void start() {
        running.set(true);
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) return;
        for (Map.Entry<String, Registration> e : registrations.entrySet()) {
            e.getValue().cancel();
        }
        registrations.clear();
        try {
            scheduler.shutdown();
            if (!scheduler.getScheduledThreadPoolExecutor()
                    .awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("Reconciliation scheduler did not terminate within 5s; forcing");
                scheduler.getScheduledThreadPoolExecutor().shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            scheduler.getScheduledThreadPoolExecutor().shutdownNow();
        } catch (Exception e) {
            log.warn("Error shutting down reconciliation scheduler", e);
        }
    }

    @Override
    public void stop(Runnable callback) {
        try {
            stop();
        } finally {
            // Same pattern as InvalidationDispatcher — Spring's
            // LifecycleProcessor uses the callback as a sync signal; failing
            // to invoke it would wedge the processor.
            callback.run();
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public int getPhase() {
        return LIFECYCLE_PHASE;
    }

    /** Test seam: number of currently registered caches. */
    public int registrationCount() {
        return registrations.size();
    }

    private record Registration(ScheduledFuture<?> future, Reconciler reconciler) {
        void cancel() {
            // mayInterruptIfRunning=false: a cycle in flight should be allowed
            // to complete naturally — it is one GET, sub-millisecond, and
            // interrupting could leave half-updated metrics or in-flight
            // logging.
            future.cancel(false);
        }
    }
}
