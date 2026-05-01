package io.github.nwwarm;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.Nonnull;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RBucket;
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
public class DistributedOnlyCache implements Cache {

    private static final Logger log = LoggerFactory.getLogger(DistributedOnlyCache.class);
    private static final long GENERATION_REFRESH_MS = 1000;

    private final String cacheName;
    private final CacheProperties.CacheSpec spec;
    private final RedissonClient redisson;
    private final CircuitBreaker breaker;
    private final Codec bucketCodec;

    private final RAtomicLong distributedGeneration;
    private final AtomicLong localGeneration = new AtomicLong(0);
    private volatile long lastGenerationRefresh = 0;

    /** In-flight loads, used for local single-flight when there is no L1 to lean on. */
    private final ConcurrentMap<Object, CompletableFuture<Object>> inflight = new ConcurrentHashMap<>();

    private final Counter hits;
    private final Counter misses;
    private final Counter failures;
    private final Counter breakerOpen;
    private final Timer getLatency;

    public DistributedOnlyCache(String cacheName,
                                CacheProperties.CacheSpec spec,
                                Codec bucketCodec,
                                RedissonClient redisson,
                                CircuitBreaker breaker,
                                MeterRegistry meterRegistry) {
        this.cacheName = cacheName;
        this.spec = spec;
        this.redisson = redisson;
        this.breaker = breaker;
        this.bucketCodec = bucketCodec;
        this.distributedGeneration = redisson.getAtomicLong(cacheName + ":generation");

        try {
            breaker.executeRunnable(() -> localGeneration.set(distributedGeneration.get()));
            lastGenerationRefresh = System.currentTimeMillis();
        } catch (Exception e) {
            log.warn("Failed to initialize generation for cache '{}'", cacheName, e);
        }

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
                throw new ValueRetrievalException(key, valueLoader,
                        e.getCause() != null ? e.getCause() : e);
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
                lock = redisson.getLock(cacheName + ":lock:" + key);
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
            log.debug("Distributed lock acquisition failed for key '{}'; proceeding with local single-flight only", key, e);
        }

        try {
            Object value = valueLoader.call();
            // Bypass put()'s key validation: we already stringified above.
            writeToRedis(key, value);
            return value;
        } catch (Throwable t) {
            throw new ValueRetrievalException(key, valueLoader, t);
        } finally {
            if (acquired && lock != null && lock.isHeldByCurrentThread()) {
                try {
                    lock.unlock();
                } catch (Exception e) {
                    log.warn("Failed to unlock {}", lock.getName(), e);
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
            log.warn("Distributed put failed for key '{}'", key, e);
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
            log.warn("Distributed evict failed for key '{}'", stringKey, e);
        }
    }

    @Override
    public void clear() {
        try {
            Long newGen = breaker.executeSupplier(distributedGeneration::incrementAndGet);
            localGeneration.set(newGen);
            lastGenerationRefresh = System.currentTimeMillis();
        } catch (Exception e) {
            failures.increment();
            log.warn("Distributed clear (generation bump) failed for cache '{}'", cacheName, e);
        }
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
            log.warn("Distributed read failed for key '{}'", key, e);
            return null;
        }
    }

    private RBucket<Object> bucket(String key) {
        String fullKey = cacheName + ":" + currentGeneration() + ":" + key;
        return bucketCodec == null
                ? redisson.getBucket(fullKey)
                : redisson.getBucket(fullKey, bucketCodec);
    }

    private long currentGeneration() {
        long now = System.currentTimeMillis();
        if (now - lastGenerationRefresh > GENERATION_REFRESH_MS) {
            try {
                breaker.executeRunnable(() -> localGeneration.set(distributedGeneration.get()));
                lastGenerationRefresh = now;
            } catch (Exception e) {
                // Use stale generation
            }
        }
        return localGeneration.get();
    }
}
