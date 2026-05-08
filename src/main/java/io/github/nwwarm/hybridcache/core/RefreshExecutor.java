package io.github.nwwarm.hybridcache.core;

import io.github.nwwarm.hybridcache.invalidation.InvalidationDispatcher;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Shared executor for SWR stale dispatches, RA probabilistic dispatches, and
 * async-loader hops off the Netty event loop (0.5.0).
 *
 * <p>Sized via {@code cache.refresh.scheduler-pool-size} (default {@code 4}).
 * Single source of background-refresh truth means single tuning knob, single
 * metric, single shutdown path. See §10 0.5.0 decision log.
 *
 * <h2>Lifecycle</h2>
 *
 * <p>Implements {@link SmartLifecycle} at the same phase as
 * {@link InvalidationDispatcher} ({@link #LIFECYCLE_PHASE} —
 * {@code Integer.MAX_VALUE - 1024}) so refresh tasks drain ahead of the
 * bean-destruction phase that disposes {@code RedissonClient}. A task that
 * lands on a destroyed Redisson would surface as
 * {@code RedissonShutdownException}; SWR/RA refresh-task bodies catch all
 * Throwables, but the executor must actually stop on context shutdown to
 * keep the JVM exit clean.
 *
 * <h2>Metrics</h2>
 *
 * <ul>
 *   <li>{@code cache.refresh.queue.size} (gauge) — work waiting to run.
 *       Sustained non-zero means the executor is undersized.</li>
 *   <li>{@code cache.refresh.executor.active} (gauge) — currently-running
 *       tasks.</li>
 * </ul>
 */
public class RefreshExecutor implements SmartLifecycle {

    /** Same phase as {@link InvalidationDispatcher#LIFECYCLE_PHASE}. */
    public static final int LIFECYCLE_PHASE = Integer.MAX_VALUE - 1024;

    private static final Logger log = LoggerFactory.getLogger(RefreshExecutor.class);

    private final ThreadPoolExecutor pool;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public RefreshExecutor(int poolSize, MeterRegistry meterRegistry) {
        if (poolSize <= 0) poolSize = 4;
        // FixedThreadPool semantics: corePoolSize == maximumPoolSize, unbounded
        // queue. SWR/RA work is fire-and-forget; ThreadPoolExecutor's saturation
        // policies are not load-bearing here — we'd rather queue than reject
        // refresh work, and the queue gauge surfaces sustained pressure.
        AtomicLong threadId = new AtomicLong(0);
        ThreadFactory tf = r -> {
            Thread t = new Thread(r, "hybrid-cache-refresh-" + threadId.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
        this.pool = new ThreadPoolExecutor(
                poolSize, poolSize,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                tf);

        Gauge.builder("cache.refresh.queue.size", pool, p -> p.getQueue().size())
                .register(meterRegistry);
        Gauge.builder("cache.refresh.executor.active", pool, ThreadPoolExecutor::getActiveCount)
                .register(meterRegistry);
        running.set(true);
    }

    /**
     * Submit a refresh task. The task body is responsible for catching its
     * own {@link Throwable}s — uncaught exceptions in a {@code submit()}'d
     * task only surface via {@code Future.get()}, which the SWR/RA paths
     * never call. See {@code # implementation guardrails / ## SWR} item 4.
     */
    public void submit(Runnable task) {
        if (!running.get()) {
            // After SmartLifecycle.stop() the pool is shut down; new tasks
            // must not be submitted. Drop with a debug log — refreshes are
            // best-effort and an in-flight shutdown is not a correctness bug.
            log.debug("Refresh task dropped — executor is stopped");
            return;
        }
        try {
            pool.execute(task);
        } catch (java.util.concurrent.RejectedExecutionException e) {
            // Race with stop(). Same handling as the running.get() check above.
            log.debug("Refresh task rejected — executor was being shut down", e);
        }
    }

    /** Test seam: pool size at construction (after coercion). */
    public int getPoolSize() {
        return pool.getCorePoolSize();
    }

    /**
     * Returns this executor as a {@link java.util.concurrent.Executor} for
     * use with {@code CompletableFuture.thenComposeAsync(loader, executor)}
     * on the async/reactive read path (0.5.0). The returned executor
     * delegates to {@link #submit(Runnable)} so the executor's running
     * gate (drop-on-shutdown) and rejected-execution behavior apply
     * uniformly to every caller — sync SWR/RA dispatch and async-path
     * loader hops both flow through {@code submit}.
     */
    public java.util.concurrent.Executor asExecutor() {
        return this::submit;
    }

    /** Test seam: queue depth, mirroring the gauge. */
    public int queueSize() {
        return pool.getQueue().size();
    }

    @Override
    public void start() {
        running.set(true);
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) return;
        try {
            pool.shutdown();
            if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("Refresh executor did not terminate within 5s; forcing");
                pool.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            pool.shutdownNow();
        } catch (Exception e) {
            log.warn("Error shutting down refresh executor", e);
        }
    }

    @Override
    public void stop(Runnable callback) {
        try {
            stop();
        } finally {
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
}
