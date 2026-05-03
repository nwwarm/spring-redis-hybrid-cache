package io.github.nwwarm;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Per-cache concurrency cap for loader calls.
 *
 * <p>When a cache is configured with {@code max-concurrent-loaders}, this gate
 * sits in front of {@code valueLoader.call()} so a stampede on the source of
 * truth is bounded even when the distributed lock is unavailable (Redis down,
 * breaker open, or {@code LOCAL_ONLY} tier).
 *
 * <p>When {@code maxConcurrentLoaders} is null at construction, every gate
 * method short-circuits to a direct loader call — no semaphore is created,
 * no metrics are registered, and the JIT-inlined null check is the only
 * overhead vs. the pre-feature codepath.
 */
final class LoaderGate {

    private final String cacheName;
    private final Semaphore semaphore;
    private final Duration acquireTimeout;
    private final Counter rejections;

    LoaderGate(String cacheName,
               Integer maxConcurrentLoaders,
               Duration loaderAcquireTimeout,
               MeterRegistry meterRegistry) {
        this.cacheName = cacheName;
        if (maxConcurrentLoaders == null) {
            this.semaphore = null;
            this.acquireTimeout = null;
            this.rejections = null;
            return;
        }
        // fair=false: throughput trumps strict FIFO under load. The cap
        // exists to bound concurrency, not to guarantee ordering.
        this.semaphore = new Semaphore(maxConcurrentLoaders, false);
        this.acquireTimeout = loaderAcquireTimeout;
        Gauge.builder("cache.loaders.permits.available", semaphore, Semaphore::availablePermits)
                .tag("cache", cacheName).register(meterRegistry);
        this.rejections = Counter.builder("cache.loaders.rejections")
                .tag("cache", cacheName).register(meterRegistry);
    }

    /**
     * Runs the loader under the gate. When the gate is unconfigured this is
     * a direct {@code loader.call()} — same control flow as before the gate
     * existed.
     *
     * @throws LoaderRejectedException when permit acquisition times out
     * @throws Exception loader's own exception, unchanged
     */
    <T> T run(Object key, Callable<T> loader) throws Exception {
        if (semaphore == null) return loader.call();
        boolean acquired;
        try {
            acquired = semaphore.tryAcquire(acquireTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
        if (!acquired) {
            rejections.increment();
            throw new LoaderRejectedException(cacheName, key);
        }
        try {
            return loader.call();
        } finally {
            semaphore.release();
        }
    }
}
