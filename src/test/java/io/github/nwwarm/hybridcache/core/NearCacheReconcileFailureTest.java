package io.github.nwwarm.hybridcache.core;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.nwwarm.hybridcache.config.CacheProperties;
import io.github.nwwarm.hybridcache.invalidation.InvalidationDispatcher;
import io.github.nwwarm.hybridcache.testutil.ListAppender;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RedissonClient;
import org.redisson.client.RedisTimeoutException;
import org.springframework.cache.caffeine.CaffeineCache;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Failure-mode coverage for {@link NearCache#reconcile()}: the breaker-open
 * and Redis-error catches on the first read of {@code distributedSeq} and
 * on the 1.0.2 suspected-REGRESSION recheck. Pure Mockito (no Testcontainers),
 * same pattern as {@link NearCacheClearImmediateFailureTest}.
 *
 * <p>NearCache's first-read {@code CallNotPermittedException} catch is
 * already exercised by
 * {@code ReconciliationFailureModesIT.breakerOpen_skipsCycle_andEmitsSkippedCounter};
 * not duplicated here. The other three catches were uncovered in the 1.0.2
 * coverage report.
 */
class NearCacheReconcileFailureTest {

    private static final String CACHE_NAME = "near-recon-fail";

    @Test
    void reconcile_redisErrorOnFirstRead_skipsCycle_withReasonException() {
        Fixture f = Fixture.create();
        when(f.distributedSeq.get())
                .thenThrow(new RedisTimeoutException("injected on first read"));

        try (ListAppender logs = ListAppender.attach(NearCache.class)) {
            NearCache cache = f.build();
            long observedBefore = cache.lastObservedSeq();

            cache.reconcile();

            assertThat(f.skipped("exception"))
                    .as("first-read Redis error must increment skipped with reason=exception")
                    .isEqualTo(1.0);
            assertThat(f.skipped("breaker-open")).isZero();
            assertThat(f.regressions()).isZero();
            assertThat(f.misses()).isZero();
            assertThat(cache.lastObservedSeq())
                    .as("first-read failure must not advance or reset the watermark")
                    .isEqualTo(observedBefore);
            assertThat(logs.messages())
                    .anyMatch(m -> m.contains("Reconciliation cycle exception")
                            && m.contains(CACHE_NAME));
            assertThat(logs.messages())
                    .as("skipped cycle must not log the counter-deleted warning")
                    .noneMatch(m -> m.contains("Counter likely deleted by operator"));

            cache.shutdown();
        }
    }

    @Test
    void reconcile_breakerOpenOnRecheck_skipsCycle_withReasonBreakerOpen() {
        // First read succeeds and returns 0; watermark is 50 → classifier
        // returns REGRESSION → recheck path. The first-read supplier
        // transitions the breaker to OPEN as a side effect, so when
        // executeSupplier dispatches the recheck the breaker rejects
        // synchronously.
        Fixture f = Fixture.create();
        when(f.distributedSeq.get()).thenAnswer(invocation -> {
            f.breaker.transitionToOpenState();
            return 0L;
        });

        try (ListAppender logs = ListAppender.attach(NearCache.class)) {
            NearCache cache = f.build();
            cache.onMessageObserved(50L);
            long observedBefore = cache.lastObservedSeq();

            cache.reconcile();

            assertThat(f.skipped("breaker-open"))
                    .as("recheck breaker-open must increment skipped with reason=breaker-open")
                    .isEqualTo(1.0);
            assertThat(f.skipped("exception")).isZero();
            assertThat(f.regressions())
                    .as("skipped recheck must not fall through to the regression handler")
                    .isZero();
            assertThat(f.regressionRecheckResolved()).isZero();
            assertThat(cache.lastObservedSeq())
                    .as("recheck failure must not reset the watermark")
                    .isEqualTo(observedBefore);
            assertThat(logs.messages())
                    .as("skipped cycle must not log the counter-deleted warning")
                    .noneMatch(m -> m.contains("Counter likely deleted by operator"));

            cache.shutdown();
        }
    }

    @Test
    void reconcile_redisErrorOnRecheck_skipsCycle_withReasonException() {
        // First read returns 0; watermark is 50 → REGRESSION → recheck.
        // Recheck throws RedisTimeoutException.
        Fixture f = Fixture.create();
        when(f.distributedSeq.get())
                .thenReturn(0L)
                .thenThrow(new RedisTimeoutException("injected on recheck"));

        try (ListAppender logs = ListAppender.attach(NearCache.class)) {
            NearCache cache = f.build();
            cache.onMessageObserved(50L);
            long observedBefore = cache.lastObservedSeq();

            cache.reconcile();

            assertThat(f.skipped("exception"))
                    .as("recheck Redis error must increment skipped with reason=exception")
                    .isEqualTo(1.0);
            assertThat(f.skipped("breaker-open")).isZero();
            assertThat(f.regressions()).isZero();
            assertThat(f.regressionRecheckResolved()).isZero();
            assertThat(cache.lastObservedSeq()).isEqualTo(observedBefore);
            assertThat(logs.messages())
                    .anyMatch(m -> m.contains("Reconciliation recheck exception")
                            && m.contains(CACHE_NAME));
            assertThat(logs.messages())
                    .as("skipped cycle must not log the counter-deleted warning")
                    .noneMatch(m -> m.contains("Counter likely deleted by operator"));

            cache.shutdown();
        }
    }

    // ----- Fixture (mock wiring shared across tests) ----------------------

    private static final class Fixture {
        final RedissonClient redisson = mock(RedissonClient.class);
        final RAtomicLong distributedGeneration = mock(RAtomicLong.class);
        final RAtomicLong distributedSeq = mock(RAtomicLong.class);
        final InvalidationDispatcher dispatcher = mock(InvalidationDispatcher.class);
        final CircuitBreaker breaker = CircuitBreakerRegistry.ofDefaults()
                .circuitBreaker("near-recon-fail-" + System.nanoTime());
        final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

        static Fixture create() {
            Fixture f = new Fixture();
            // Route the two distinct RAtomicLong handles by the key suffix.
            // Reconciliation reads only touch :seq; the :generation handle is
            // touched by the ctor's initializeGeneration() and must succeed.
            when(f.redisson.getAtomicLong(ArgumentMatchers.endsWith(":generation")))
                    .thenReturn(f.distributedGeneration);
            when(f.redisson.getAtomicLong(ArgumentMatchers.endsWith(":seq")))
                    .thenReturn(f.distributedSeq);
            when(f.distributedGeneration.get()).thenReturn(0L);
            when(f.dispatcher.getNodeId()).thenReturn("test-node");
            return f;
        }

        NearCache build() {
            return new NearCache(
                    caffeine(CACHE_NAME), spec(), null,
                    redisson, breaker, dispatcher, meterRegistry,
                    new KeyLogFormatter(false, "test"));
        }

        double skipped(String reason) {
            Counter c = meterRegistry.find("cache.reconciliation.skipped")
                    .tag("cache", CACHE_NAME).tag("reason", reason).counter();
            return c == null ? 0.0 : c.count();
        }

        double regressions() {
            Counter c = meterRegistry.find("cache.reconciliation.seq.regressions")
                    .tag("cache", CACHE_NAME).counter();
            return c == null ? 0.0 : c.count();
        }

        double regressionRecheckResolved() {
            Counter c = meterRegistry.find("cache.reconciliation.seq.regression_recheck_resolved")
                    .tag("cache", CACHE_NAME).counter();
            return c == null ? 0.0 : c.count();
        }

        double misses() {
            Counter c = meterRegistry.find("cache.reconciliation.misses.detected")
                    .tag("cache", CACHE_NAME).counter();
            return c == null ? 0.0 : c.count();
        }
    }

    private static CaffeineCache caffeine(String name) {
        com.github.benmanes.caffeine.cache.Cache<Object, Object> native_ =
                Caffeine.newBuilder().maximumSize(100).recordStats().build();
        return new CaffeineCache(name, native_, true);
    }

    private static CacheProperties.CacheSpec spec() {
        return new CacheProperties.CacheSpec(
                CacheProperties.Tier.NEAR_CACHE,
                Duration.ofMinutes(10), 100,
                Duration.ofSeconds(2), Duration.ofSeconds(10),
                CacheProperties.Codec.JSON, null, null, null, 0.0,
                null, null,
                new CacheProperties.Reconciliation(true, Duration.ofMinutes(10), 5),
                null, null);
    }
}
