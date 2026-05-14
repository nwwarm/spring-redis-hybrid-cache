package io.github.nwwarm.hybridcache.core;

import com.github.benmanes.caffeine.cache.Caffeine;
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
import org.springframework.cache.caffeine.CaffeineCache;

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
 * Failure-handling branches in {@link NearCache#clearImmediate()} introduced
 * by the 1.0.1 sync→async conversion. Pure Mockito (no Testcontainers);
 * follows the same mock pattern as {@link GenerationRefreshConcurrencyTest}.
 *
 * <p>Three uncovered branch groups from JaCoCo:
 * <ul>
 *   <li>Group 1 (lines 699-706): bump OK, SCAN/UNLINK failed.</li>
 *   <li>Group 2 (lines 711-719): bump failed — sub-branch 2a (breaker open,
 *       async-failure path) and 2b (other RuntimeException).</li>
 *   <li>Group 3 (lines 721-723): synchronous breaker rejection at submission.
 *       In Resilience4j 2.4.0 {@code executeCompletionStage} never throws
 *       synchronously (verified by bytecode + standalone repro); this catch
 *       is defensive dead code. The test still asserts the breaker-open
 *       observable behavior, which is identical to sub-branch 2a.</li>
 * </ul>
 */
class NearCacheClearImmediateFailureTest {

    private static final String CACHE_NAME = "near-clear-fail";

    @Test
    void group1_unlinkFailed_bumpsGeneration_andIncrementsL2Failures() throws Exception {
        Fixture f = Fixture.create();
        f.bumpSucceeds(1L);
        f.unlinkFailsWith(new RedisTimeoutException("injected SCAN/UNLINK failure"));

        try (ListAppender logs = ListAppender.attach(NearCache.class)) {
            NearCache cache = f.build();
            long lastRefreshBefore = f.lastRefreshNanos(cache);
            long localGenBefore = cache.localGeneration();

            cache.clearImmediate();

            assertThat(f.l2Failures()).isEqualTo(1.0);
            assertThat(f.l2BreakerOpen()).isZero();
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

        try (ListAppender logs = ListAppender.attach(NearCache.class)) {
            NearCache cache = f2b.build();
            long lastRefreshBefore = f2b.lastRefreshNanos(cache);
            long localGenBefore = cache.localGeneration();

            cache.clearImmediate();

            assertThat(f2b.l2Failures()).isEqualTo(1.0);
            assertThat(f2b.l2BreakerOpen()).isZero();
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
        // handler then increments l2BreakerOpen (not l2Failures).
        Fixture f2a = Fixture.create();
        f2a.breaker.transitionToOpenState();
        f2a.unlinkNeverCalled();

        try (ListAppender logs = ListAppender.attach(NearCache.class)) {
            NearCache cache = f2a.build();
            long lastRefreshBefore = f2a.lastRefreshNanos(cache);
            long localGenBefore = cache.localGeneration();

            cache.clearImmediate();

            assertThat(f2a.l2BreakerOpen())
                    .as("breaker rejection in the async chain must hit l2BreakerOpen, not l2Failures")
                    .isEqualTo(1.0);
            assertThat(f2a.l2Failures()).isZero();
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

        try (ListAppender logs = ListAppender.attach(NearCache.class)) {
            NearCache cache = f.build();
            long lastRefreshBefore = f.lastRefreshNanos(cache);
            long localGenBefore = cache.localGeneration();

            cache.clearImmediate();  // must not throw

            assertThat(f.l2BreakerOpen()).isEqualTo(1.0);
            assertThat(f.l2Failures()).isZero();
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
                .circuitBreaker("near-clear-fail-" + System.nanoTime());
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

        NearCache build() {
            return new NearCache(
                    caffeine(CACHE_NAME), spec(), null,
                    redisson, breaker, dispatcher, meterRegistry,
                    new KeyLogFormatter(false, "test"));
        }

        double l2Failures() {
            return meterRegistry.find("cache.l2.failures").tag("cache", CACHE_NAME).counter().count();
        }

        double l2BreakerOpen() {
            return meterRegistry.find("cache.l2.breaker.open").tag("cache", CACHE_NAME).counter().count();
        }

        long lastRefreshNanos(NearCache cache) {
            try {
                java.lang.reflect.Field field = NearCache.class.getDeclaredField("lastRefreshNanos");
                field.setAccessible(true);
                return ((AtomicLong) field.get(cache)).get();
            } catch (ReflectiveOperationException e) {
                throw new AssertionError("could not read lastRefreshNanos", e);
            }
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
                null, null, null, null, null);
    }
}
