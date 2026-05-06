package io.github.nwwarm;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * Singleton coordinator for all per-cache preloaders. Owns:
 *
 * <ul>
 *   <li>The shared {@link ThreadPoolTaskScheduler} that runs <em>periodic
 *       stores</em> (default size 4, sized for steady-state idle work).</li>
 *   <li>A separate boot-prefetch {@link ExecutorService}, lazily created on
 *       first registration, sized for the burst, shut down once every
 *       cache's {@code prefetchTimeout} has elapsed or its prefetch has
 *       completed.</li>
 *   <li>A {@code .preloader.lock} file per directory, parsed and
 *       overwritten according to the rules in
 *       {@code docs/preloader-design.md} §11.</li>
 *   <li>A registry of every {@link NearCachePreloader} in this JVM, keyed
 *       by cache name.</li>
 * </ul>
 *
 * <p>Implements {@link SmartLifecycle} at the same phase as
 * {@link InvalidationDispatcher} so its {@code stop()} runs <em>before</em>
 * the bean-destruction phase that disposes Redisson — see item 5's
 * lifecycle ordering work. The final per-cache store on shutdown therefore
 * captures the latest key set while the rest of the application context is
 * still live.
 */
public class PreloaderCoordinator implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(PreloaderCoordinator.class);
    private static final String LOCK_FILENAME = ".preloader.lock";
    private static final Set<PosixFilePermission> DIR_PERMS =
            PosixFilePermissions.fromString("rwx------");
    private static final Pattern LOCK_PATTERN = Pattern.compile(
            "^pid=(\\d+) host=(\\S+) started=(\\S+)$");

    /** Lock holder details parsed from a {@code .preloader.lock} file. */
    record LockInfo(long pid, String host, String startedAt) {
        @Override public String toString() {
            return "pid=" + pid + " host=" + host + " started=" + startedAt;
        }
    }

    /** Result of probing whether a lock holder is still alive. */
    enum LockHolderState {
        ALIVE_LOCAL,        // holder is alive on this host → fail boot
        STALE_DEAD_LOCAL,   // same host but PID is gone → take over with WARN
        FOREIGN_HOST,       // different host → take over with WARN (see §11)
        CORRUPT             // can't parse → take over with WARN
    }

    /**
     * Hook for tests to override platform-specific behaviour (process
     * liveness, hostname). Production code uses {@link #DEFAULT}.
     */
    interface PlatformProbe {
        boolean isProcessAlive(long pid);
        String currentHostname();
    }

    static final PlatformProbe DEFAULT = new PlatformProbe() {
        @Override public boolean isProcessAlive(long pid) {
            return ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false);
        }
        @Override public String currentHostname() {
            try {
                return InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                return "unknown";
            }
        }
    };

    private final CacheProperties properties;
    private final MeterRegistry meterRegistry;
    private final PlatformProbe platform;

    private final Map<String, NearCachePreloader> preloaders = new ConcurrentHashMap<>();
    private final Map<Path, Path> lockFiles = new ConcurrentHashMap<>();    // dir -> lockFile
    private final Map<String, java.util.concurrent.ScheduledFuture<?>> storeFutures = new ConcurrentHashMap<>();
    private final java.util.List<CompletableFuture<Void>> prefetchDeadlines =
            Collections.synchronizedList(new java.util.ArrayList<>());

    private volatile ThreadPoolTaskScheduler storeScheduler;
    private volatile ExecutorService prefetchExecutor;
    private volatile boolean running = false;

    public PreloaderCoordinator(CacheProperties properties, MeterRegistry meterRegistry) {
        this(properties, meterRegistry, DEFAULT);
    }

    PreloaderCoordinator(CacheProperties properties, MeterRegistry meterRegistry,
                         PlatformProbe platform) {
        this.properties = properties;
        this.meterRegistry = meterRegistry;
        this.platform = platform;
    }

    // ---------- SmartLifecycle ----------

    @Override
    public int getPhase() {
        // Match the dispatcher's phase. The lifecycle-vs-destruction split
        // is what enforces "stop runs before Redisson teardown"; the phase
        // value pins us near the top of any chain of SmartLifecycle stops.
        return InvalidationDispatcher.LIFECYCLE_PHASE;
    }

    @Override public boolean isAutoStartup() { return true; }
    @Override public boolean isRunning() { return running; }

    @Override
    public synchronized void start() {
        if (running) return;
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(properties.preloader().schedulerPoolSize());
        scheduler.setThreadNamePrefix("hybrid-cache-preloader-store-");
        scheduler.setDaemon(true);
        scheduler.initialize();
        this.storeScheduler = scheduler;
        // Prefetch executor is lazy-init'd on first register() — sized to
        // the cache's prefetch-concurrency, expanded as more caches join.
        this.running = true;
        log.debug("PreloaderCoordinator started (schedulerPoolSize={})",
                properties.preloader().schedulerPoolSize());
    }

    @Override
    public synchronized void stop() {
        if (!running) return;
        running = false;

        // 1. Cancel scheduled store tasks.
        for (var entry : storeFutures.entrySet()) {
            entry.getValue().cancel(false);
        }
        storeFutures.clear();

        // 2. Final store per cache, sequentially. Bound by min(storeInterval, 30s).
        for (NearCachePreloader p : preloaders.values()) {
            Duration bound = p.config().storeInterval().compareTo(Duration.ofSeconds(30)) < 0
                    ? p.config().storeInterval()
                    : Duration.ofSeconds(30);
            CompletableFuture<Void> finalStore = CompletableFuture.runAsync(
                    p::finalStore, storeScheduler.getScheduledExecutor());
            try {
                finalStore.get(bound.toMillis(), TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                log.warn("Cache '{}' preloader: final store on shutdown did not"
                                + " complete within {}", p.cacheName(), bound, e);
            }
        }

        // 3. Shutdown the boot-prefetch executor if it's still alive.
        ExecutorService prefetch = this.prefetchExecutor;
        if (prefetch != null && !prefetch.isShutdown()) {
            prefetch.shutdownNow();
        }

        // 4. Shutdown the periodic-store scheduler.
        if (storeScheduler != null) {
            storeScheduler.shutdown();
        }

        // 5. Release every directory lock.
        for (var entry : lockFiles.entrySet()) {
            try {
                Files.deleteIfExists(entry.getValue());
            } catch (IOException e) {
                log.warn("Failed to release preloader lock at {}; the next boot's"
                        + " stale-lock path will take over", entry.getValue(), e);
            }
        }
        lockFiles.clear();
        preloaders.clear();
    }

    // ---------- registration ----------

    /**
     * Called from {@link NearCache} when a preloader-enabled cache is
     * constructed. Synchronous — performs directory creation, lock
     * acquisition, snapshot load, and schedules the periodic store.
     * Prefetch is dispatched asynchronously.
     */
    void register(String cacheName, NearCache nearCache,
                  CacheProperties.Preloader config) {
        if (!running) {
            // The coordinator's start() runs at refresh; lazy NearCache
            // construction happens after that. If we're seeing register()
            // before running=true, something is very wrong with bean
            // ordering — surface it loudly.
            throw new IllegalStateException("PreloaderCoordinator.register('" + cacheName
                    + "') called before start(); check bean ordering");
        }
        Path directory = resolveDirectory(cacheName, config);
        prepareDirectory(directory);
        acquireLock(directory);

        NearCachePreloader preloader = new NearCachePreloader(
                cacheName, directory, config, nearCache, meterRegistry);
        NearCachePreloader prev = preloaders.put(cacheName, preloader);
        if (prev != null) {
            log.warn("Preloader already registered for '{}'; replacing", cacheName);
        }

        // Synchronous load — application startup waits on this.
        List<String> keys = preloader.loadKeys();

        // Schedule the periodic store. The trigger fires every
        // store-interval, with the first firing offset by store-initial-delay.
        Instant firstRun = Instant.now().plus(config.storeInitialDelay());
        var future = storeScheduler.scheduleWithFixedDelay(preloader::store, firstRun,
                config.storeInterval());
        storeFutures.put(cacheName, future);

        // Spawn the prefetch on the boot-burst executor. Async — startup
        // does not block on prefetch completion, only on file load.
        if (!keys.isEmpty()) {
            ExecutorService executor = getOrCreatePrefetchExecutor(config.prefetchConcurrency());
            CompletableFuture<Void> deadline = preloader.prefetch(keys, executor)
                    .completeOnTimeout(null, config.prefetchTimeout().toMillis(),
                            TimeUnit.MILLISECONDS);
            prefetchDeadlines.add(deadline);
            // When *every* cache's deadline has fired or completed, shutdown
            // the executor exactly once. The whenComplete handler registers
            // its own all-of every time a new cache joins, but since we
            // gate via the executor's idempotent shutdown, double-firing is
            // harmless and cheap.
            CompletableFuture.allOf(prefetchDeadlines.toArray(CompletableFuture[]::new))
                    .whenComplete((v, t) -> {
                        try {
                            if (t != null) {
                                log.debug("Prefetch aggregator completed exceptionally", t);
                            }
                        } finally {
                            shutdownPrefetchExecutorIfIdle();
                        }
                    });
        }

        log.debug("Cache '{}' preloader registered: directory={}, prefetched-keys={}",
                cacheName, directory, keys.size());
    }

    private synchronized ExecutorService getOrCreatePrefetchExecutor(int requestedConcurrency) {
        if (prefetchExecutor == null || prefetchExecutor.isShutdown()) {
            // Cached-pool style: 0 core, unbounded max bounded in practice
            // by the per-cache semaphores in NearCachePreloader. Idle
            // threads die after 60s. SynchronousQueue means submissions
            // either get a thread immediately or create one. The
            // per-cache concurrency cap is enforced upstream (in
            // NearCachePreloader.runOne via its Semaphore), not here.
            ThreadPoolExecutor executor = new ThreadPoolExecutor(
                    0, Integer.MAX_VALUE,
                    60L, TimeUnit.SECONDS,
                    new SynchronousQueue<>(),
                    namedThreadFactory("hybrid-cache-preloader-prefetch-"));
            executor.allowCoreThreadTimeOut(true);
            prefetchExecutor = executor;
        }
        return prefetchExecutor;
    }

    private synchronized void shutdownPrefetchExecutorIfIdle() {
        // Are all known per-cache deadlines settled?
        for (CompletableFuture<Void> f : prefetchDeadlines) {
            if (!f.isDone()) return;
        }
        ExecutorService e = prefetchExecutor;
        if (e != null && !e.isShutdown()) {
            // try/finally so the shutdown is unconditional once we've
            // decided to call it.
            try {
                e.shutdown();
            } finally {
                log.debug("Boot-prefetch executor shut down ({} cache deadlines settled)",
                        prefetchDeadlines.size());
            }
        }
    }

    // ---------- test seam ----------

    /**
     * Force a synchronous store for the named cache, ignoring the
     * scheduled interval. <b>Test-only.</b> Production code never calls
     * this
     */
    void triggerStoreNow(String cacheName) {
        NearCachePreloader p = preloaders.get(cacheName);
        if (p == null) {
            throw new IllegalStateException("No preloader registered for '" + cacheName + "'");
        }
        p.store();
    }

    NearCachePreloader getPreloaderForTest(String cacheName) {
        return preloaders.get(cacheName);
    }

    // ---------- directory + lock ----------

    private Path resolveDirectory(String cacheName, CacheProperties.Preloader config) {
        if (config.directory() != null && !config.directory().isBlank()) {
            return Path.of(config.directory());
        }
        // Default: ${java.io.tmpdir}/hybrid-cache/<application-name>/<cache-name>
        String tmpdir = System.getProperty("java.io.tmpdir");
        String app = System.getProperty("spring.application.name", "default");
        return Path.of(tmpdir, "hybrid-cache", app, cacheName);
    }

    /**
     * Creates the directory if missing (POSIX 0700) and warns if an existing
     * directory has more permissive permissions. Does <em>not</em>
     * auto-tighten — that would surprise operators. See design §12.
     */
    private void prepareDirectory(Path directory) {
        try {
            if (Files.notExists(directory)) {
                Files.createDirectories(directory);
                applyPosixDirPerms(directory);
            } else {
                checkExistingDirPerms(directory);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Preloader directory '" + directory
                    + "' is not creatable; refusing to boot. Either create it manually"
                    + " with the right permissions, or point cache.caches.<name>.preloader"
                    + ".directory at a writable path.", e);
        }
    }

    private void applyPosixDirPerms(Path directory) {
        try {
            Files.setPosixFilePermissions(directory, DIR_PERMS);
        } catch (UnsupportedOperationException ignored) {
            // Non-POSIX (Windows). ACL-inheritance applies.
        } catch (IOException e) {
            log.debug("Failed to set 0700 on {}; continuing", directory, e);
        }
    }

    private void checkExistingDirPerms(Path directory) {
        try {
            PosixFileAttributeView view = Files.getFileAttributeView(directory,
                    PosixFileAttributeView.class);
            if (view == null) return;  // non-POSIX
            Set<PosixFilePermission> actual = view.readAttributes().permissions();
            if (!DIR_PERMS.containsAll(actual)) {
                log.warn("Preloader directory {} has permissions {} which is more permissive"
                        + " than 0700; not auto-tightening (would surprise operators)."
                        + " Adjust the permissions manually if the directory holds sensitive"
                        + " key material.", directory, PosixFilePermissions.toString(actual));
            }
        } catch (IOException e) {
            log.debug("Could not read POSIX permissions for {}; continuing", directory, e);
        }
    }

    private void acquireLock(Path directory) {
        Path lockFile = directory.resolve(LOCK_FILENAME);
        if (lockFiles.containsValue(lockFile)) {
            // Same coordinator already owns this directory (multiple
            // caches sharing one). Subsequent caches don't re-lock.
            lockFiles.put(directory, lockFile);
            return;
        }
        if (Files.exists(lockFile)) {
            LockInfo existing = readLockInfo(lockFile);
            LockHolderState state = classifyHolder(existing);
            switch (state) {
                case ALIVE_LOCAL -> throw new IllegalStateException(
                        "Preloader directory '" + directory + "' is locked by another live"
                        + " process: " + existing + ". Either kill the holder or point"
                        + " cache.caches.<name>.preloader.directory at a different path."
                        + " See docs/preloader-design.md §10–§11.");
                case FOREIGN_HOST, STALE_DEAD_LOCAL, CORRUPT -> log.warn(
                        "Preloader directory {} has a stale or foreign lock ({});"
                        + " taking over. If this directory is genuinely shared between"
                        + " hosts on a network filesystem, that is undefined behavior —"
                        + " see docs/preloader-design.md §10.",
                        directory, existing);
            }
        }
        writeLockFile(lockFile);
        lockFiles.put(directory, lockFile);
    }

    private LockInfo readLockInfo(Path lockFile) {
        try {
            String content = Files.readString(lockFile, StandardCharsets.UTF_8).trim();
            Matcher m = LOCK_PATTERN.matcher(content);
            if (!m.matches()) return null;
            return new LockInfo(Long.parseLong(m.group(1)), m.group(2), m.group(3));
        } catch (NoSuchFileException e) {
            return null;
        } catch (IOException | NumberFormatException e) {
            return null;
        }
    }

    private LockHolderState classifyHolder(LockInfo info) {
        if (info == null) return LockHolderState.CORRUPT;
        if (!info.host().equals(platform.currentHostname())) return LockHolderState.FOREIGN_HOST;
        if (platform.isProcessAlive(info.pid())) return LockHolderState.ALIVE_LOCAL;
        return LockHolderState.STALE_DEAD_LOCAL;
    }

    private void writeLockFile(Path lockFile) {
        String content = "pid=" + ProcessHandle.current().pid()
                + " host=" + platform.currentHostname()
                + " started=" + Instant.now();
        Path tmp = lockFile.resolveSibling(LOCK_FILENAME + ".tmp");
        try {
            Files.deleteIfExists(tmp);
            Files.writeString(tmp, content, StandardCharsets.UTF_8, CREATE_NEW, WRITE);
            Files.move(tmp, lockFile,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to acquire preloader lock at " + lockFile, e);
        }
    }

    // ---------- helpers ----------

    private static ThreadFactory namedThreadFactory(String prefix) {
        AtomicLong seq = new AtomicLong();
        return r -> {
            Thread t = new Thread(r);
            t.setName(prefix + seq.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }
}
