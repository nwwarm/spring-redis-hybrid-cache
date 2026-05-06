package io.github.nwwarm;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.Nonnull;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RBucket;
import org.redisson.api.RKeys;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Redis-only cache with no L1.
 *
 * <p>Every read crosses the network. Use when working set is too large to
 * replicate per-node, when consistency matters more than latency, or when the
 * cached values are inherently shared across nodes (sessions, idempotency
 * keys, distributed coordination state).
 *
 * <h2>Single-flight</h2>
 *
 * Two-tier herd protection on the loader path ({@link #get(Object, Callable)}):
 *
 * <ol>
 *   <li><b>Local single-flight</b> via an in-flight {@link CompletableFuture} map.
 *       Only one thread per JVM executes the loader for a given key; other threads
 *       wait on the future. Works whether Redis is healthy, slow, or unreachable.</li>
 *   <li><b>Cross-node single-flight</b> via Redisson {@link RLock}. Acquired inside
 *       the loader. If Redis is down, lock acquisition fails and we proceed with
 *       local-only protection — the worst case is N database loads across N nodes,
 *       not N × threads-per-node.</li>
 * </ol>
 *
 * <p>Generation counter (same trick as {@link NearCache}) gives O(1) {@link #clear()}.
 *
 * <h2>Failure semantics</h2>
 *
 * <p>When the circuit breaker is open every read returns null and every write is
 * suppressed — callers fall through to the source. A cache must not cause outages;
 * it must fail open. Source systems behind a distributed-only cache must handle
 * full unfiltered load during a Redis incident.
 */
public class DistributedOnlyCache implements HybridCache, InvalidationListener {

    private static final Logger log = LoggerFactory.getLogger(DistributedOnlyCache.class);
    private static final long GENERATION_REFRESH_NANOS = 1_000_000_000L; // 1s

    private final String cacheName;
    private final CacheProperties.CacheSpec spec;
    private final RedissonClient redisson;
    private final CircuitBreaker breaker;
    private final Codec bucketCodec;
    private final KeyLogFormatter keyLogFormatter;
    private final InvalidationDispatcher dispatcher;

    private final RAtomicLong distributedGeneration;
    private final AtomicLong localGeneration = new AtomicLong(0);
    private final AtomicLong lastRefreshNanos = new AtomicLong(0);

    /** In-flight loads, used for local single-flight when there is no L1 to lean on. */
    private final ConcurrentMap<Object, CompletableFuture<Object>> inflight = new ConcurrentHashMap<>();

    private final Counter hits;
    private final Counter misses;
    private final Counter failures;
    private final Counter breakerOpen;
    private final Timer getLatency;
    private final LoaderGate loaderGate;

    public DistributedOnlyCache(String cacheName,
                                CacheProperties.CacheSpec spec,
                                Codec bucketCodec,
                                RedissonClient redisson,
                                CircuitBreaker breaker,
                                InvalidationDispatcher dispatcher,
                                MeterRegistry meterRegistry,
                                KeyLogFormatter keyLogFormatter) {
        this.cacheName = cacheName;
        this.spec = spec;
        this.redisson = redisson;
        this.breaker = breaker;
        this.bucketCodec = bucketCodec;
        this.keyLogFormatter = keyLogFormatter;
        this.dispatcher = dispatcher;
        this.distributedGeneration = redisson.getAtomicLong(CacheKeys.generationKey(cacheName));

        try {
            breaker.executeRunnable(() -> localGeneration.set(distributedGeneration.get()));
            lastRefreshNanos.set(System.nanoTime());
        } catch (Exception e) {
            log.warn("Failed to initialize generation for cache '{}'", cacheName, e);
        }

        dispatcher.register(this);

        this.hits = Counter.builder("cache.distributed.gets")
                .tag("cache", cacheName).tag("result", "hit").register(meterRegistry);
        this.misses = Counter.builder("cache.distributed.gets")
                .tag("cache", cacheName).tag("result", "miss").register(meterRegistry);
        this.failures = Counter.builder("cache.distributed.failures")
                .tag("cache", cacheName).register(meterRegistry);
        this.breakerOpen = Counter.builder("cache.distributed.breaker.open")
                .tag("cache", cacheName).register(meterRegistry);
        this.getLatency = Timer.builder("cache.distributed.get.latency")
                .tag("cache", cacheName).register(meterRegistry);
        this.loaderGate = new LoaderGate(cacheName,
                spec.maxConcurrentLoaders(), spec.loaderAcquireTimeout(), meterRegistry);
    }

    @Override
    @Nonnull
    public String getName() {
        return cacheName;
    }

    @Override
    @Nonnull
    public Object getNativeCache() {
        return redisson;
    }

    @Override
    public ValueWrapper get(@Nonnull Object key) {
        String stringKey = CacheKeys.stringify(cacheName, key);
        Object value = readFromRedis(stringKey);
        return value == null ? null : new SimpleValueWrapper(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(@Nonnull Object key, Class<T> type) {
        String stringKey = CacheKeys.stringify(cacheName, key);
        Object value = readFromRedis(stringKey);
        if (value != null && type != null && !type.isInstance(value)) {
            throw new IllegalStateException(
                    "Cached value [" + value + "] is not of required type [" + type.getName() + "]");
        }
        return (T) value;
    }

    /**
     * Loader-aware get with two-tier single-flight protection.
     *
     * <ol>
     *   <li><b>Local single-flight</b> via {@link #inflight} map. Concurrent requests
     *       on this JVM for the same key collapse onto a single {@link CompletableFuture}
     *       — only the first thread executes the loader path; others wait. Works whether
     *       Redis is up or down.</li>
     *   <li><b>Cross-node single-flight</b> via {@link RLock} inside the loader path.
     *       Falls through if Redis is unreachable; local single-flight remains.</li>
     * </ol>
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(@Nonnull Object key, @Nonnull Callable<T> valueLoader) {
        String stringKey = CacheKeys.stringify(cacheName, key);

        // Fast path: already in Redis.
        Object existing = readFromRedis(stringKey);
        if (existing != null) return (T) existing;

        CompletableFuture<Object> mine = new CompletableFuture<>();
        CompletableFuture<Object> theirs = inflight.putIfAbsent(stringKey, mine);

        if (theirs != null) {
            // Another thread on this JVM is already loading. Wait for its result.
            try {
                long timeoutMs = spec.lockWait().toMillis() + spec.lockLease().toMillis();
                Object value = theirs.get(timeoutMs, TimeUnit.MILLISECONDS);
                return (T) value;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ValueRetrievalException(key, valueLoader, e);
            } catch (ExecutionException | TimeoutException e) {
                Throwable cause = e.getCause();
                if (cause instanceof LoaderRejectedException lr) throw lr;
                throw new ValueRetrievalException(key, valueLoader,
                        cause != null ? cause : e);
            }
        }

        // We're the first; do the load.
        try {
            Object value = doLoadWithCrossNodeSingleFlight(stringKey, valueLoader);
            mine.complete(value);
            return (T) value;
        } catch (RuntimeException t) {
            mine.completeExceptionally(t);
            throw t;
        } catch (Throwable t) {
            mine.completeExceptionally(t);
            throw new ValueRetrievalException(key, valueLoader, t);
        } finally {
            inflight.remove(stringKey, mine);
        }
    }

    /**
     * Performs the load behind a Redis lock when possible. Falls through to
     * loader-only if Redis is unreachable — local inflight map already handles
     * the per-JVM herd.
     */
    private Object doLoadWithCrossNodeSingleFlight(String key, Callable<?> valueLoader) {
        RLock lock = null;
        boolean acquired = false;
        try {
            if (!spec.lockWait().isZero()) {
                lock = redisson.getLock(CacheKeys.lockKey(cacheName, key));
                acquired = lock.tryLock(
                        spec.lockWait().toMillis(),
                        spec.lockLease().toMillis(),
                        TimeUnit.MILLISECONDS);

                if (acquired) {
                    Object existing = readFromRedis(key);
                    if (existing != null) return existing;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ValueRetrievalException(key, valueLoader, e);
        } catch (Exception e) {
            log.debug("Distributed lock acquisition failed for key '{}'; proceeding with local single-flight only", keyLogFormatter.format(key), e);
        }

        try {
            Object value = loaderGate.run(key, valueLoader);
            // Bypass put()'s key validation: we already stringified above.
            writeToRedis(key, value);
            return value;
        } catch (LoaderRejectedException e) {
            // Backpressure: do not wrap as ValueRetrievalException — callers
            // distinguish rejection (no loader call made) from failure
            // (loader ran and threw).
            throw e;
        } catch (Throwable t) {
            throw new ValueRetrievalException(key, valueLoader, t);
        } finally {
            if (acquired && lock != null && lock.isHeldByCurrentThread()) {
                try {
                    lock.unlock();
                } catch (Exception e) {
                    log.warn("Failed to unlock cache='{}' key={}", cacheName, keyLogFormatter.format(key), e);
                }
            }
        }
    }

    @Override
    public void put(@Nonnull Object key, Object value) {
        String stringKey = CacheKeys.stringify(cacheName, key);
        writeToRedis(stringKey, value);
    }

    private void writeToRedis(String key, Object value) {
        try {
            breaker.executeRunnable(() -> bucket(key).set(value, spec.ttl()));
        } catch (CallNotPermittedException e) {
            breakerOpen.increment();
            // Distributed-only + Redis down = no caching this call. Caller hits source on next read.
        } catch (Exception e) {
            failures.increment();
            log.warn("Distributed put failed for key '{}'", keyLogFormatter.format(key), e);
        }
    }

    @Override
    public void evict(@Nonnull Object key) {
        String stringKey = CacheKeys.stringify(cacheName, key);
        try {
            breaker.executeRunnable(() -> bucket(stringKey).delete());
        } catch (CallNotPermittedException e) {
            breakerOpen.increment();
        } catch (Exception e) {
            failures.increment();
            log.warn("Distributed evict failed for key '{}'", keyLogFormatter.format(stringKey), e);
        }
    }

    @Override
    public void clear() {
        try {
            Long newGen = breaker.executeSupplier(distributedGeneration::incrementAndGet);
            localGeneration.set(newGen);
            lastRefreshNanos.set(System.nanoTime());
        } catch (Exception e) {
            failures.increment();
            log.warn("Distributed clear (generation bump) failed for cache '{}'", cacheName, e);
        }
        // Publish so peers refresh their cached generation immediately rather
        // than waiting up to GENERATION_REFRESH_NANOS for the next poll. The
        // 1s poll remains as a backstop if the message is dropped.
        publishClear();
    }

    /**
     * Eager clear: bump generation, then SCAN + UNLINK every old-generation
     * value bucket on every reachable shard. Stronger than {@link #clear()} —
     * when this returns, no orphan keys remain in Redis under this cache's
     * prefix. See {@link HybridCache#clearImmediate()} for the rationale.
     *
     * <p>Order matches {@code NearCache.clearImmediate}: bump generation
     * first (concurrent reads switch to the new prefix immediately), publish
     * OP_CLEAR (peers refresh their generation pointer ASAP), then UNLINK
     * old-generation keys via {@link RKeys#unlinkByPattern}, which iterates
     * every cluster master and pipelines UNLINK in batches.
     *
     * <p>Failure handling: if the bump fails the exception propagates and
     * no UNLINK is attempted. If UNLINK fails mid-iteration the exception
     * still propagates, but correctness is preserved — the generation is
     * already bumped, so reads land on a fresh prefix and surviving keys
     * orphan and decay via TTL.
     */
    @Override
    public void clearImmediate() {
        long newGen;
        try {
            newGen = breaker.executeSupplier(distributedGeneration::incrementAndGet);
        } catch (RuntimeException e) {
            failures.increment();
            log.warn("clearImmediate generation bump failed for cache '{}'", cacheName, e);
            throw e;
        }
        localGeneration.set(newGen);
        lastRefreshNanos.set(System.nanoTime());
        long oldGen = newGen - 1;

        publishClear();

        String pattern = CacheKeys.valueKeyPattern(cacheName, oldGen);
        try {
            redisson.getKeys().unlinkByPattern(pattern);
        } catch (RuntimeException e) {
            failures.increment();
            log.warn("clearImmediate SCAN/UNLINK failed for cache '{}' pattern '{}';"
                    + " surviving old-generation keys will expire via TTL",
                    cacheName, pattern, e);
            throw e;
        }
    }

    private void publishClear() {
        try {
            breaker.executeRunnable(() ->
                    dispatcher.publish(new InvalidationMessage(
                            dispatcher.getNodeId(), cacheName, InvalidationMessage.OP_CLEAR, null)));
        } catch (Exception e) {
            // Suppressed — peers will catch up via the 1s generation poll.
        }
    }

    /**
     * Invoked by {@link InvalidationDispatcher} after self-skip and name routing.
     * Only {@code OP_CLEAR} drives any action: a clear from a peer means the
     * generation counter has advanced, so we force a refresh on the next read
     * instead of waiting for the 1s poll. {@code OP_INVALIDATE} carries no
     * action here — there is no L1 to evict, and the peer's L2 delete already
     * removed the entry from L2.
     */
    @Override
    public void handleInvalidation(String op, String key) {
        switch (op) {
            case InvalidationMessage.OP_CLEAR -> {
                lastRefreshNanos.set(0);
                currentGeneration();
            }
            case InvalidationMessage.OP_INVALIDATE -> log.trace(
                    "Ignoring per-key invalidation on distributed-only cache '{}' key={}",
                    cacheName, keyLogFormatter.format(key));
            default -> log.warn("Unknown invalidation op '{}' on cache '{}'", op, cacheName);
        }
    }

    /** Called by the cache manager during context shutdown. Idempotent. */
    public void shutdown() {
        dispatcher.deregister(cacheName);
    }

    private Object readFromRedis(String key) {
        try {
            return breaker.executeSupplier(() -> {
                Timer.Sample sample = Timer.start();
                try {
                    Object value = bucket(key).get();
                    if (value == null) {
                        misses.increment();
                    } else {
                        hits.increment();
                    }
                    return value;
                } finally {
                    sample.stop(getLatency);
                }
            });
        } catch (CallNotPermittedException e) {
            breakerOpen.increment();
            return null;
        } catch (Exception e) {
            failures.increment();
            log.warn("Distributed read failed for key '{}'", keyLogFormatter.format(key), e);
            return null;
        }
    }

    private RBucket<Object> bucket(String key) {
        String fullKey = CacheKeys.valueKey(cacheName, key, currentGeneration());
        return bucketCodec == null
                ? redisson.getBucket(fullKey)
                : redisson.getBucket(fullKey, bucketCodec);
    }

    private long currentGeneration() {
        long observed = lastRefreshNanos.get();
        long now = System.nanoTime();
        if (now - observed < GENERATION_REFRESH_NANOS) return localGeneration.get();
        if (!lastRefreshNanos.compareAndSet(observed, now)) return localGeneration.get();
        try {
            breaker.executeRunnable(() -> localGeneration.set(distributedGeneration.get()));
        } catch (Exception e) {
            lastRefreshNanos.compareAndSet(now, observed);
        }
        return localGeneration.get();
    }

    void forceRefreshDue() {
        lastRefreshNanos.set(0);
    }

    CircuitBreaker getBreaker() {
        return breaker;
    }
}
