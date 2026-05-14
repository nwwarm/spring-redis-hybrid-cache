package io.github.nwwarm.hybridcache.core;

import io.github.nwwarm.hybridcache.config.CacheProperties;
import io.github.nwwarm.hybridcache.invalidation.InvalidationDispatcher;
import io.github.nwwarm.hybridcache.testutil.ListAppender;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RKeys;
import org.redisson.api.RedissonClient;
import org.redisson.client.RedisTimeoutException;
import org.redisson.misc.CompletableFutureWrapper;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Parallel of {@code NearCacheClearImmediateFailureTest} for the
 * {@link DistributedOnlyCache#clearImmediate()} implementation. The async
 * failure chain is structurally identical; the differences are in the
 * counter names ({@code failures}/{@code breakerOpen}, metered under
 * {@code cache.distributed.*}) and the absence of an L1 tier.
 */
class DistributedOnlyCacheClearImmediateFailureTest {

    private static final String CACHE_NAME = "dist-clear-fail";

    @Test
    void group1_unlinkFailed_bumpsGeneration_andIncrementsFailures() throws Exception {
        Fixture f = Fixture.create();
        f.bumpSucceeds(1L);
        f.unlinkFailsWith(new RedisTimeoutException("injected SCAN/UNLINK failure"));

        try (ListAppender logs = ListAppender.attach(DistributedOnlyCache.class)) {
            DistributedOnlyCache cache = f.build();
            long lastRefreshBefore = f.lastRefreshNanos(cache);
            long localGenBefore = cache.localGeneration();

            cache.clearImmediate();

            assertThat(f.failures()).isEqualTo(1.0);
            assertThat(f.breakerOpen()).isZero();
            assertThat(cache.localGeneration())
                    .as("localGeneration must advance — the bump landed before the unlink failed")
                    .isEqualTo(localGenBefore + 1);
            assertThat(f.lastRefreshNanos(cache))
                    .as("lastRefreshNanos must advance with the successful bump")
                    .isGreaterThan(lastRefreshBefore);
            assertThat(logs.messages())
                    .anyMatch(m -> m.contains("SCAN/UNLINK failed") && m.contains(CACHE_NAME));

            cache.shutdown();
        }
    }

    @Test
    void group2_bumpFailed_leavesLocalStateUnchanged() throws Exception {
        // 2b — generic RuntimeException from the async bump.
        Fixture f2b = Fixture.create();
        f2b.bumpFailsWith(new RedisTimeoutException("injected bump timeout"));
        f2b.unlinkNeverCalled();

        try (ListAppender logs = ListAppender.attach(DistributedOnlyCache.class)) {
            DistributedOnlyCache cache = f2b.build();
            long lastRefreshBefore = f2b.lastRefreshNanos(cache);
            long localGenBefore = cache.localGeneration();

            cache.clearImmediate();

            assertThat(f2b.failures()).isEqualTo(1.0);
            assertThat(f2b.breakerOpen()).isZero();
            assertThat(cache.localGeneration())
                    .as("bump failed — localGeneration must not advance")
                    .isEqualTo(localGenBefore);
            assertThat(f2b.lastRefreshNanos(cache))
                    .as("bump failed — lastRefreshNanos must not advance")
                    .isEqualTo(lastRefreshBefore);
            assertThat(logs.messages())
                    .anyMatch(m -> m.contains("generation bump failed") && m.contains(CACHE_NAME));
            verify(f2b.keys, never()).unlinkByPatternAsync(anyString());

            cache.shutdown();
        }

        // 2a — breaker-open during the async chain. Resilience4j routes the
        // breaker rejection through the returned CompletionStage as a
        // CompletionException(CallNotPermittedException); the exceptionally
        // handler then increments breakerOpen (not failures).
        Fixture f2a = Fixture.create();
        f2a.breaker.transitionToOpenState();
        f2a.unlinkNeverCalled();

        try (ListAppender logs = ListAppender.attach(DistributedOnlyCache.class)) {
            DistributedOnlyCache cache = f2a.build();
            long lastRefreshBefore = f2a.lastRefreshNanos(cache);
            long localGenBefore = cache.localGeneration();

            cache.clearImmediate();

            assertThat(f2a.breakerOpen())
                    .as("breaker rejection in the async chain must hit breakerOpen, not failures")
                    .isEqualTo(1.0);
            assertThat(f2a.failures()).isZero();
            assertThat(cache.localGeneration()).isEqualTo(localGenBefore);
            assertThat(f2a.lastRefreshNanos(cache)).isEqualTo(lastRefreshBefore);
            assertThat(logs.messages())
                    .as("breaker rejection must not produce a 'generation bump failed' log line")
                    .noneMatch(m -> m.contains("generation bump failed"));
            verify(f2a.keys, never()).unlinkByPatternAsync(anyString());

            cache.shutdown();
        }
    }

    @Test
    void group3_breakerOpenAtSubmission_returnsWithoutThrowing_andSkipsAsyncChain() throws Exception {
        // r4j 2.4.0 routes the breaker rejection through the CompletionStage,
        // not a synchronous throw — observable behaviour is therefore identical
        // to sub-branch 2a. This test asserts the user-visible contract:
        // breaker counter ++, no exception, no chain side-effects.
        Fixture f = Fixture.create();
        f.breaker.transitionToOpenState();
        f.unlinkNeverCalled();

        try (ListAppender logs = ListAppender.attach(DistributedOnlyCache.class)) {
            DistributedOnlyCache cache = f.build();
            long lastRefreshBefore = f.lastRefreshNanos(cache);
            long localGenBefore = cache.localGeneration();

            cache.clearImmediate();  // must not throw

            assertThat(f.breakerOpen()).isEqualTo(1.0);
            assertThat(f.failures()).isZero();
            assertThat(cache.localGeneration())
                    .as("breaker rejection — no chain dispatched, localGeneration unchanged")
                    .isEqualTo(localGenBefore);
            assertThat(f.lastRefreshNanos(cache)).isEqualTo(lastRefreshBefore);
            List<String> warnings = logs.messages();
            assertThat(warnings)
                    .as("breaker rejection must not produce a 'generation bump failed' warning")
                    .noneMatch(m -> m.contains("generation bump failed"));
            verify(f.dispatcher, never()).publishAsync(any());
            verify(f.keys, never()).unlinkByPatternAsync(anyString());

            cache.shutdown();
        }
    }

    // ----- Fixture (mock wiring shared across tests) ----------------------

    private static final class Fixture {
        final RedissonClient redisson = mock(RedissonClient.class, Answers.RETURNS_DEEP_STUBS);
        final RAtomicLong distributedGeneration = mock(RAtomicLong.class);
        final RKeys keys = mock(RKeys.class);
        final InvalidationDispatcher dispatcher = mock(InvalidationDispatcher.class);
        final CircuitBreaker breaker = CircuitBreakerRegistry.ofDefaults()
                .circuitBreaker("dist-clear-fail-" + System.nanoTime());
        final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

        static Fixture create() {
            Fixture f = new Fixture();
            when(f.distributedGeneration.get()).thenReturn(0L);
            when(f.redisson.getAtomicLong(anyString())).thenReturn(f.distributedGeneration);
            when(f.redisson.getKeys()).thenReturn(f.keys);
            when(f.dispatcher.getNodeId()).thenReturn("test-node");
            when(f.dispatcher.publishAsync(any())).thenReturn(CompletableFuture.completedFuture(0L));
            return f;
        }

        void bumpSucceeds(long newGen) {
            when(distributedGeneration.incrementAndGetAsync())
                    .thenReturn(new CompletableFutureWrapper<>(CompletableFuture.completedFuture(newGen)));
        }

        void bumpFailsWith(Throwable cause) {
            CompletableFuture<Long> failed = new CompletableFuture<>();
            failed.completeExceptionally(cause);
            when(distributedGeneration.incrementAndGetAsync())
                    .thenReturn(new CompletableFutureWrapper<>(failed));
        }

        void unlinkFailsWith(Throwable cause) {
            CompletableFuture<Long> failed = new CompletableFuture<>();
            failed.completeExceptionally(cause);
            when(keys.unlinkByPatternAsync(anyString()))
                    .thenReturn(new CompletableFutureWrapper<>(failed));
        }

        void unlinkNeverCalled() {
            // Default behaviour returns null which would NPE if invoked; that
            // crash would surface the bug. Leaving unstubbed is the assertion.
        }

        DistributedOnlyCache build() {
            return new DistributedOnlyCache(
                    CACHE_NAME, spec(), null,
                    redisson, breaker, dispatcher, meterRegistry,
                    new KeyLogFormatter(false, "test"));
        }

        double failures() {
            return meterRegistry.find("cache.distributed.failures").tag("cache", CACHE_NAME).counter().count();
        }

        double breakerOpen() {
            return meterRegistry.find("cache.distributed.breaker.open").tag("cache", CACHE_NAME).counter().count();
        }

        long lastRefreshNanos(DistributedOnlyCache cache) {
            try {
                java.lang.reflect.Field field = DistributedOnlyCache.class.getDeclaredField("lastRefreshNanos");
                field.setAccessible(true);
                return ((AtomicLong) field.get(cache)).get();
            } catch (ReflectiveOperationException e) {
                throw new AssertionError("could not read lastRefreshNanos", e);
            }
        }
    }

    private static CacheProperties.CacheSpec spec() {
        return new CacheProperties.CacheSpec(
                CacheProperties.Tier.DISTRIBUTED_ONLY,
                Duration.ofMinutes(10), 100,
                Duration.ofSeconds(2), Duration.ofSeconds(10),
                CacheProperties.Codec.JSON, null, null, null, 0.0,
                null, null, null, null, null);
    }
}
