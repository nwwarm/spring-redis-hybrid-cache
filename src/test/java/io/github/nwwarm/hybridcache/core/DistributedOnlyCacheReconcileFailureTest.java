package io.github.nwwarm.hybridcache.core;

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

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Failure-mode coverage for {@link DistributedOnlyCache#reconcile()}: the
 * breaker-open and Redis-error catches on the first read of
 * {@code distributedSeq} and on the 1.0.2 suspected-REGRESSION recheck.
 * Pure Mockito (no Testcontainers), same pattern as
 * {@link NearCacheReconcileFailureTest}.
 *
 * <p>All four catches were uncovered in the 1.0.2 coverage report.
 * NearCache's first-read breaker-open was already covered by
 * {@code ReconciliationFailureModesIT.breakerOpen_skipsCycle_andEmitsSkippedCounter};
 * its DistributedOnly equivalent did not exist before this file.
 */
class DistributedOnlyCacheReconcileFailureTest {

    private static final String CACHE_NAME = "dist-recon-fail";

    @Test
    void reconcile_breakerOpenOnFirstRead_skipsCycle_withReasonBreakerOpen() {
        Fixture f = Fixture.create();
        // Cache constructed with breaker closed, so initializeGeneration()
        // succeeds. Trip the breaker before driving the cycle.
        DistributedOnlyCache cache = f.build();
        f.breaker.transitionToOpenState();

        try (ListAppender logs = ListAppender.attach(DistributedOnlyCache.class)) {
            long observedBefore = cache.lastObservedSeq();

            cache.reconcile();

            assertThat(f.skipped("breaker-open"))
                    .as("first-read breaker-open must increment skipped with reason=breaker-open")
                    .isEqualTo(1.0);
            assertThat(f.skipped("exception")).isZero();
            assertThat(f.regressions()).isZero();
            assertThat(f.misses()).isZero();
            assertThat(cache.lastObservedSeq()).isEqualTo(observedBefore);
            assertThat(logs.messages())
                    .as("skipped cycle must not log the counter-deleted warning")
                    .noneMatch(m -> m.contains("Counter likely deleted by operator"));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void reconcile_redisErrorOnFirstRead_skipsCycle_withReasonException() {
        Fixture f = Fixture.create();
        when(f.distributedSeq.get())
                .thenThrow(new RedisTimeoutException("injected on first read"));

        try (ListAppender logs = ListAppender.attach(DistributedOnlyCache.class)) {
            DistributedOnlyCache cache = f.build();
            long observedBefore = cache.lastObservedSeq();

            cache.reconcile();

            assertThat(f.skipped("exception"))
                    .as("first-read Redis error must increment skipped with reason=exception")
                    .isEqualTo(1.0);
            assertThat(f.skipped("breaker-open")).isZero();
            assertThat(f.regressions()).isZero();
            assertThat(f.misses()).isZero();
            assertThat(cache.lastObservedSeq()).isEqualTo(observedBefore);
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
        Fixture f = Fixture.create();
        when(f.distributedSeq.get()).thenAnswer(invocation -> {
            f.breaker.transitionToOpenState();
            return 0L;
        });

        try (ListAppender logs = ListAppender.attach(DistributedOnlyCache.class)) {
            DistributedOnlyCache cache = f.build();
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
            assertThat(cache.lastObservedSeq()).isEqualTo(observedBefore);
            assertThat(logs.messages())
                    .as("skipped cycle must not log the counter-deleted warning")
                    .noneMatch(m -> m.contains("Counter likely deleted by operator"));

            cache.shutdown();
        }
    }

    @Test
    void reconcile_redisErrorOnRecheck_skipsCycle_withReasonException() {
        Fixture f = Fixture.create();
        when(f.distributedSeq.get())
                .thenReturn(0L)
                .thenThrow(new RedisTimeoutException("injected on recheck"));

        try (ListAppender logs = ListAppender.attach(DistributedOnlyCache.class)) {
            DistributedOnlyCache cache = f.build();
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

    // ----- Fixture --------------------------------------------------------

    private static final class Fixture {
        final RedissonClient redisson = mock(RedissonClient.class);
        final RAtomicLong distributedGeneration = mock(RAtomicLong.class);
        final RAtomicLong distributedSeq = mock(RAtomicLong.class);
        final InvalidationDispatcher dispatcher = mock(InvalidationDispatcher.class);
        final CircuitBreaker breaker = CircuitBreakerRegistry.ofDefaults()
                .circuitBreaker("dist-recon-fail-" + System.nanoTime());
        final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

        static Fixture create() {
            Fixture f = new Fixture();
            when(f.redisson.getAtomicLong(ArgumentMatchers.endsWith(":generation")))
                    .thenReturn(f.distributedGeneration);
            when(f.redisson.getAtomicLong(ArgumentMatchers.endsWith(":seq")))
                    .thenReturn(f.distributedSeq);
            when(f.distributedGeneration.get()).thenReturn(0L);
            when(f.dispatcher.getNodeId()).thenReturn("test-node");
            return f;
        }

        DistributedOnlyCache build() {
            return new DistributedOnlyCache(
                    CACHE_NAME, spec(), null,
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

    private static CacheProperties.CacheSpec spec() {
        return new CacheProperties.CacheSpec(
                CacheProperties.Tier.DISTRIBUTED_ONLY,
                Duration.ofMinutes(10), 100,
                Duration.ofSeconds(2), Duration.ofSeconds(10),
                CacheProperties.Codec.JSON, null, null, null, 0.0,
                null, null,
                new CacheProperties.Reconciliation(true, Duration.ofMinutes(10), 5),
                null, null);
    }
}
