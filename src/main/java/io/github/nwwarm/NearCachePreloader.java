package io.github.nwwarm;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache.ValueWrapper;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Per-cache preloader. Reads/writes the snapshot file, dispatches the
 * boot prefetch fan-out, and owns the per-cache metrics. Lifetime is
 * managed by {@link PreloaderCoordinator}; this class does not schedule
 * its own work.
 *
 * <p>See {@code docs/preloader-design.md} for the full design.
 */
final class NearCachePreloader {

    private static final Logger log = LoggerFactory.getLogger(NearCachePreloader.class);
    static final String SNAPSHOT_FILENAME = "keys";

    private final String cacheName;
    private final Path file;
    private final CacheProperties.Preloader config;
    private final NearCache nearCache;

    private final AtomicBoolean storeInFlight = new AtomicBoolean(false);
    private volatile int lastSnapshotSize = 0;

    private final Counter storeFailures;
    private final Timer storeDuration;
    private final Counter prefetchHits;
    private final Counter prefetchMisses;
    private final Counter prefetchFails;
    private final Counter prefetchSkipped;
    private final Timer prefetchDuration;

    NearCachePreloader(String cacheName,
                       Path directory,
                       CacheProperties.Preloader config,
                       NearCache nearCache,
                       MeterRegistry meterRegistry) {
        this.cacheName = cacheName;
        this.file = directory.resolve(SNAPSHOT_FILENAME);
        this.config = config;
        this.nearCache = nearCache;

        this.storeFailures = Counter.builder("cache.preloader.store.failures")
                .tag("cache", cacheName).register(meterRegistry);
        this.storeDuration = Timer.builder("cache.preloader.store.duration")
                .tag("cache", cacheName).register(meterRegistry);
        this.prefetchHits = Counter.builder("cache.preloader.prefetch")
                .tag("cache", cacheName).tag("result", "hit").register(meterRegistry);
        this.prefetchMisses = Counter.builder("cache.preloader.prefetch")
                .tag("cache", cacheName).tag("result", "miss").register(meterRegistry);
        this.prefetchFails = Counter.builder("cache.preloader.prefetch")
                .tag("cache", cacheName).tag("result", "fail").register(meterRegistry);
        this.prefetchSkipped = Counter.builder("cache.preloader.prefetch")
                .tag("cache", cacheName).tag("result", "skipped").register(meterRegistry);
        this.prefetchDuration = Timer.builder("cache.preloader.prefetch.duration")
                .tag("cache", cacheName).register(meterRegistry);
        // Gauge backed by a volatile field updated post-store. Reads the
        // field, never the disk — discrete step changes per store interval.
        Gauge.builder("cache.preloader.snapshot.size", this, NearCachePreloader::lastSnapshotSize)
                .tag("cache", cacheName).register(meterRegistry);
    }

    String cacheName() { return cacheName; }
    Path file() { return file; }
    int lastSnapshotSize() { return lastSnapshotSize; }
    CacheProperties.Preloader config() { return config; }

    /**
     * Reads the snapshot file synchronously. Returns the keys to prefetch,
     * or an empty list if the file is missing, corrupt, or stamped at a
     * stale generation. Never throws — every error path is logged and
     * resolved to "boot cold."
     */
    List<String> loadKeys() {
        PreloaderFile.LoadResult result = PreloaderFile.load(file);
        if (result instanceof PreloaderFile.Missing) {
            log.debug("Cache '{}' preloader: no snapshot file at {}; first boot", cacheName, file);
            return List.of();
        }
        if (result instanceof PreloaderFile.ParseError pe) {
            log.warn("Cache '{}' preloader: snapshot file {} corrupt - {}; starting cold",
                    cacheName, file, pe.reason());
            return List.of();
        }
        PreloaderFile.Loaded loaded = (PreloaderFile.Loaded) result;
        long currentGen = nearCache.localGeneration();
        if (loaded.generation() != currentGen) {
            log.info("Cache '{}' preloader: file generation {} != current {};"
                            + " snapshot is from before a clear, discarding",
                    cacheName, loaded.generation(), currentGen);
            return List.of();
        }
        if (loaded.skippedLines() > 0) {
            log.warn("Cache '{}' preloader: skipped {} malformed body lines in {}",
                    cacheName, loaded.skippedLines(), file);
        }
        return loaded.keys();
    }

    /**
     * Periodic store. Idempotent — if a previous run is still in flight,
     * this is a no-op with a debug log. Failures are logged + counted; the
     * next interval retries.
     */
    void store() {
        if (!storeInFlight.compareAndSet(false, true)) {
            log.debug("Cache '{}' preloader: store already in flight; skipping", cacheName);
            return;
        }
        Timer.Sample sample = Timer.start();
        try {
            long currentGen = nearCache.localGeneration();
            // Caffeine.asMap().keySet() — weakly consistent snapshot, see
            // design §5 Q3. Keys are already stringified at insert time
            // (NearCache routes everything through CacheKeys.stringify).
            @SuppressWarnings("unchecked")
            com.github.benmanes.caffeine.cache.Cache<Object, Object> native_ =
                    (com.github.benmanes.caffeine.cache.Cache<Object, Object>) nearCache.getNativeCache();
            Set<Object> keySet = native_.asMap().keySet();
            List<String> keys = new ArrayList<>(keySet.size());
            for (Object k : keySet) {
                if (k instanceof String s) keys.add(s);
                else keys.add(String.valueOf(k));
            }

            try {
                PreloaderFile.writeAtomic(file, cacheName, currentGen, keys, config.maxStoredKeys());
                int cap = config.maxStoredKeys() == null ? Integer.MAX_VALUE : config.maxStoredKeys();
                lastSnapshotSize = Math.min(keys.size(), cap);
            } catch (IOException e) {
                log.warn("Cache '{}' preloader: store failed for {}; will retry next interval",
                        cacheName, file, e);
                storeFailures.increment();
            }
        } finally {
            storeInFlight.set(false);
            sample.stop(storeDuration);
        }
    }

    /**
     * Async prefetch fan-out. Returns a {@link CompletableFuture} that
     * completes when all keys have been processed <em>or</em> the per-cache
     * {@code prefetchTimeout} elapses, whichever comes first. The
     * coordinator awaits {@code allOf(perCacheDeadlines)} to know when
     * the boot-burst executor can be shut down.
     *
     * <p>Each key submits one task. Inside the task: an absolute deadline
     * check (skips remaining work after timeout), a per-cache semaphore
     * acquire (gates concurrency without blocking the executor pool), then
     * a direct {@code nearCache.get(key)} — the same code path as a normal
     * read, hitting L2 and populating L1. <b>No loader invocation</b> by
     * design.
     */
    CompletableFuture<Void> prefetch(List<String> keys, ExecutorService executor) {
        if (keys.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        Duration timeout = config.prefetchTimeout();
        Semaphore gate = new Semaphore(config.prefetchConcurrency());
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        Timer.Sample sample = Timer.start();

        List<CompletableFuture<Void>> tasks = new ArrayList<>(keys.size());
        for (String key : keys) {
            tasks.add(CompletableFuture.runAsync(() -> runOne(key, gate, deadlineNanos), executor));
        }

        return CompletableFuture.allOf(tasks.toArray(CompletableFuture[]::new))
                .whenComplete((v, t) -> {
                    sample.stop(prefetchDuration);
                    if (t != null) {
                        log.debug("Cache '{}' preloader: prefetch aggregator completed exceptionally",
                                cacheName, t);
                    }
                });
    }

    private void runOne(String key, Semaphore gate, long deadlineNanos) {
        if (System.nanoTime() > deadlineNanos) {
            prefetchSkipped.increment();
            return;
        }
        boolean acquired = false;
        try {
            // Bounded acquire so a stuck task can't park forever. The
            // semaphore is per-cache; cap at a small ceiling — if we can't
            // acquire within this window, cap and skip rather than queue.
            acquired = gate.tryAcquire(250, TimeUnit.MILLISECONDS);
            if (!acquired) {
                prefetchSkipped.increment();
                return;
            }
            if (System.nanoTime() > deadlineNanos) {
                // Re-check after the wait — could have crossed the deadline.
                prefetchSkipped.increment();
                return;
            }
            ValueWrapper wrapper = nearCache.get(key);
            if (wrapper == null) prefetchMisses.increment();
            else prefetchHits.increment();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            prefetchFails.increment();
        } catch (Throwable t) {
            // Bounded log — debug level. Per-key WARN would dominate
            // production logs on a multi-thousand-key set; the counter is
            // the primary signal for chronic failures.
            log.debug("Cache '{}' preloader: prefetch failed for key '{}'", cacheName, key, t);
            prefetchFails.increment();
        } finally {
            if (acquired) gate.release();
        }
    }

    /**
     * Called once at shutdown. Same shape as {@link #store()} — the
     * coordinator wraps it in a per-cache timeout so a slow disk can't
     * extend context shutdown indefinitely.
     */
    void finalStore() {
        store();
    }

    /**
     * Map<Cache.NativeKey, MetricCount> hint for tests asserting on counter
     * shapes; production code reads via {@link MeterRegistry#find}.
     */
    Map<String, Counter> prefetchCountersForTest() {
        return Map.of(
                "hit", prefetchHits,
                "miss", prefetchMisses,
                "fail", prefetchFails,
                "skipped", prefetchSkipped);
    }
}
