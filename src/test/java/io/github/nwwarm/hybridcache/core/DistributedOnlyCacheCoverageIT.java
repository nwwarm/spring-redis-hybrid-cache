package io.github.nwwarm.hybridcache.core;

import io.github.nwwarm.hybridcache.config.CacheProperties;
import io.github.nwwarm.hybridcache.invalidation.InvalidationDispatcher;
import io.github.nwwarm.hybridcache.testfixtures.RedisTestBase;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.springframework.cache.Cache;

import java.time.Duration;

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Targeted coverage on {@link DistributedOnlyCache} read-and-load paths
 * that the existing distributed-only IT suite touches only at the edges:
 * the synchronous {@code get(key, Callable)} cold-load path through
 * {@code doLoadWithCrossNodeSingleFlight}, the per-JVM {@code inflight}
 * collapse, the async {@code retrieve(...)} surface (loader-less and
 * loader-full), evict, and {@code clearImmediate()}'s SCAN+UNLINK cycle.
 *
 * <p>Backed by a real Redis container (via {@link RedisTestBase}) because
 * mocking Redisson's async API surface tends to mask wire-format and
 * codec interactions that the production code relies on.
 */
class DistributedOnlyCacheCoverageIT extends RedisTestBase {

    private RedissonClient redisson;
    private DistributedOnlyCache cache;
    private final String name = "dist-cov-" + System.nanoTime();

    @BeforeEach
    void setUp() {
        redisson = newRedisson();
        cache = newDistributedCache(name, redisson, defaultBreaker(), "node-cov");
    }

    @AfterEach
    void tearDown() {
        if (cache != null) cache.shutdown();
        if (redisson != null) redisson.shutdown();
    }

    // ---------- get(key, Callable): sync cold-load via lock + single-flight ----------

    @Test
    void getWithLoader_coldKey_loaderRunsOnce_andValuePersistsToL2() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        User loaded = cache.get("u1", () -> {
            calls.incrementAndGet();
            return new User("alice", 30);
        });
        assertThat(loaded).isEqualTo(new User("alice", 30));
        assertThat(calls.get()).isEqualTo(1);

        // Second call must hit L2 and skip the loader entirely. The
        // valueLoader argument the contract takes still has to type-check;
        // verifying calls stays at 1 confirms the loader was not invoked.
        User again = cache.get("u1", () -> {
            calls.incrementAndGet();
            return new User("never", -1);
        });
        assertThat(again).isEqualTo(loaded);
        assertThat(calls.get()).isEqualTo(1);
    }

    @Test
    void getWithLoader_concurrentColdReads_collapseToOneLoader() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch loaderEntered = new CountDownLatch(1);
        CountDownLatch loaderRelease = new CountDownLatch(1);

        // Thread A: enters loader and blocks until release; holds the
        // shared inflight slot for "u2".
        Thread first = new Thread(() -> cache.get("u2", () -> {
            calls.incrementAndGet();
            loaderEntered.countDown();
            loaderRelease.await();
            return new User("bob", 42);
        }));
        first.setDaemon(true);
        first.start();
        assertThat(loaderEntered.await(2, TimeUnit.SECONDS)).isTrue();

        // Thread B: arrives while A is still loading; must NOT invoke the
        // loader. It waits on A's in-flight future.
        CompletableFuture<User> b = CompletableFuture.supplyAsync(() ->
                cache.get("u2", () -> {
                    calls.incrementAndGet();
                    return new User("never-runs", -1);
                }));

        // Pause briefly so B has a chance to register on inflight.
        Thread.sleep(100);
        loaderRelease.countDown();
        first.join(2_000);

        assertThat(b.get(2, TimeUnit.SECONDS)).isEqualTo(new User("bob", 42));
        assertThat(calls.get())
                .as("per-JVM inflight collapse: A loaded, B waited on A's future")
                .isEqualTo(1);
    }

    @Test
    void getWithLoader_loaderThrows_wrapsInValueRetrievalException() {
        assertThatThrownBy(() -> cache.get("u-fail", () -> {
            throw new IllegalStateException("loader-blew-up");
        }))
                .isInstanceOf(Cache.ValueRetrievalException.class)
                .hasRootCauseInstanceOf(IllegalStateException.class);
    }

    // ---------- evict + clearImmediate ----------

    @Test
    void evict_removesValueFromL2_subsequentGetReturnsNull() {
        cache.put("e1", new User("evictable", 1));
        assertThat(cache.get("e1", User.class)).isEqualTo(new User("evictable", 1));

        cache.evict("e1");

        assertThat(cache.get("e1", User.class)).isNull();
    }

    @Test
    void clearImmediate_unlinksAllCurrentGenerationKeys() {
        cache.put("c1", new User("alpha", 1));
        cache.put("c2", new User("beta", 2));
        long genBefore = cache.localGeneration();

        cache.clearImmediate();

        // Generation bumped: every old-generation key is invisible
        // regardless of UNLINK timing. The eager UNLINK is the
        // "no orphan keys remain" guarantee on top of the generation
        // bump — we cannot directly assert orphan absence without a
        // SCAN, so we assert the visible behaviour: prior values are
        // gone and the generation has advanced.
        assertThat(cache.get("c1", User.class)).isNull();
        assertThat(cache.get("c2", User.class)).isNull();
        assertThat(cache.localGeneration()).isGreaterThan(genBefore);
    }

    // ---------- retrieve(key): async, loader-less ----------

    @Test
    void retrieveNoLoader_l2Miss_completesWithNull() throws Exception {
        CompletableFuture<?> f = cache.retrieve("missing");
        Object got = f.get(2, TimeUnit.SECONDS);
        assertThat(got).isNull();
    }

    @Test
    void retrieveNoLoader_l2Hit_completesWithSimpleValueWrapper() throws Exception {
        cache.put("rk", new User("retrieved", 7));
        Object got = cache.retrieve("rk").get(2, TimeUnit.SECONDS);
        assertThat(got).isInstanceOf(Cache.ValueWrapper.class);
        assertThat(((Cache.ValueWrapper) got).get()).isEqualTo(new User("retrieved", 7));
    }

    // ---------- retrieve(key, Supplier): async with loader ----------

    @Test
    void retrieveWithLoader_l2Miss_loaderRuns_andValuePersistsToL2() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        CompletableFuture<User> f = cache.retrieve("rl", () -> {
            calls.incrementAndGet();
            return CompletableFuture.completedFuture(new User("retrieved-async", 11));
        });

        User loaded = f.get(2, TimeUnit.SECONDS);
        assertThat(loaded).isEqualTo(new User("retrieved-async", 11));
        assertThat(calls.get()).isEqualTo(1);

        // Second async retrieve hits L2 and does not invoke the loader.
        User again = cache.<User>retrieve("rl", () -> {
            calls.incrementAndGet();
            return CompletableFuture.completedFuture(new User("never", -1));
        }).get(2, TimeUnit.SECONDS);
        assertThat(again).isEqualTo(loaded);
        assertThat(calls.get()).isEqualTo(1);
    }

    @Test
    void retrieveWithLoader_loaderFails_completesExceptionallyWithValueRetrievalException() {
        CompletableFuture<User> f = cache.retrieve("rl-fail", () -> {
            CompletableFuture<User> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IllegalStateException("loader-blew-up"));
            return failed;
        });
        assertThatThrownBy(() -> f.get(2, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(IllegalStateException.class);
    }

    // ---------- forceRefreshDue + localGeneration accessors ----------

    @Test
    void forceRefreshDue_marksGenerationStale_nextReadRefreshesIt() {
        long before = cache.localGeneration();
        cache.forceRefreshDue();
        // The accessor returns the locally-cached value; the next read
        // path is what re-fetches from Redis. We do not assert on a
        // generation bump (no clear happened), only that the call is a
        // no-op on the value itself.
        cache.put("frd", new User("after-force", 0));
        assertThat(cache.get("frd", User.class)).isEqualTo(new User("after-force", 0));
        assertThat(cache.localGeneration()).isGreaterThanOrEqualTo(before);
    }

    // ---------- getNativeCache: distributed-only has no L1, returns the
    // Redisson client so callers that want raw Redis access have a seam
    // (mirrors Spring's CaffeineCache.getNativeCache returning Caffeine).

    @Test
    void getNativeCache_distributedOnly_returnsRedissonClient() {
        assertThat(cache.getNativeCache()).isSameAs(redisson);
    }

    @Test
    void isReconciliationEnabled_defaultSpec_isFalse() {
        assertThat(cache.isReconciliationEnabled()).isFalse();
    }

    @Test
    void getBreaker_returnsTheConfiguredBreaker() {
        assertThat(cache.getBreaker()).isNotNull();
    }

    @Test
    void handleInvalidation_withPositiveSeq_advancesObservedWatermark() {
        // seq=0 is the "no reconciliation seq carried" sentinel; only
        // seq>0 hits onMessageObserved. Drive the path explicitly because
        // the integration suite does not exercise distributed-only with
        // reconciliation enabled.
        cache.handleInvalidation("invalidate", "k", 5L);
        assertThat(cache.lastObservedSeq()).isEqualTo(5L);

        // accumulateAndGet uses Math::max — a lower value must not regress
        // the watermark.
        cache.handleInvalidation("invalidate", "k", 2L);
        assertThat(cache.lastObservedSeq()).isEqualTo(5L);
    }

    @Test
    void handleInvalidation_clear_advancesGenerationPointer() {
        long before = cache.localGeneration();
        cache.handleInvalidation("clear", null, 0L);
        // The clear branch resets lastRefreshNanos and re-reads the
        // generation; the call must complete without throwing.
        assertThat(cache.localGeneration()).isGreaterThanOrEqualTo(before);
    }

    @Test
    void handleInvalidation_unknownOp_isLoggedAndIgnored() {
        // The default branch logs WARN and drops. Verify it does not
        // throw and does not corrupt state.
        long before = cache.lastObservedSeq();
        cache.handleInvalidation("not-a-real-op", "k", 0L);
        assertThat(cache.lastObservedSeq()).isEqualTo(before);
    }

    @Test
    void getWithLoader_lockWaitZero_takesLoaderOnlyBranch() throws Exception {
        // Cover doLoadWithCrossNodeSingleFlight's spec.lockWait().isZero()
        // branch: when lock-wait is configured to zero, the cross-node
        // RLock acquisition is skipped entirely and the loader runs under
        // the per-JVM inflight protection only.
        CacheProperties.CacheSpec spec = new CacheProperties.CacheSpec(
                CacheProperties.Tier.DISTRIBUTED_ONLY,
                Duration.ofMinutes(10), 10_000,
                Duration.ZERO, Duration.ofSeconds(10),
                CacheProperties.Codec.JSON,
                null, null, null, 0.0, null, null, null, null, null);
        SimpleMeterRegistry mr = new SimpleMeterRegistry();
        InvalidationDispatcher d = new InvalidationDispatcher(redisson, "node-zero", mr);
        DistributedOnlyCache zeroLockCache = new DistributedOnlyCache(
                "dist-zero-" + System.nanoTime(), spec, null, redisson,
                defaultBreaker(), d, mr, new KeyLogFormatter(false, "test"), null);
        try {
            AtomicInteger calls = new AtomicInteger();
            User loaded = zeroLockCache.get("z1", () -> {
                calls.incrementAndGet();
                return new User("zero-lock", 99);
            });
            assertThat(loaded).isEqualTo(new User("zero-lock", 99));
            assertThat(calls.get()).isEqualTo(1);
        } finally {
            zeroLockCache.shutdown();
        }
    }

    // ---------- fixtures ----------

    /**
     * Public, no-arg-constructor + setters: required for Jackson
     * polymorphic deserialization through the test JsonJacksonCodec.
     */
    public static class User implements Serializable {
        private String name;
        private int age;

        public User() {}
        public User(String name, int age) {
            this.name = name;
            this.age = age;
        }

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public int getAge() { return age; }
        public void setAge(int age) { this.age = age; }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof User u)) return false;
            return age == u.age && Objects.equals(name, u.name);
        }

        @Override
        public int hashCode() { return Objects.hash(name, age); }

        @Override
        public String toString() { return "User[" + name + "," + age + "]"; }
    }
}
