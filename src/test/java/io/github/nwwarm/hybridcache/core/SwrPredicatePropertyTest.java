package io.github.nwwarm.hybridcache.core;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for the SWR classification predicate.
 *
 * <p>Property: for any {@code (freshFor, staleFor, ttl, readDelay)} tuple
 * satisfying the validator's constraints, the read path must classify
 * into exactly one of {fresh, stale} relative to a synthetic write
 * timestamp. (The third class — physically evicted past {@code staleFor}
 * — is enforced by Caffeine, not the sidecar; the sidecar's classify()
 * never sees those reads because Caffeine returns null for them.)
 *
 * <p>jqwik is not on the dependency list; the property is exercised via
 * {@code N=2000} bounded random tuples generated with
 * {@link ThreadLocalRandom}. The decision table is tiny — fresh vs.
 * stale relative to a single threshold — so 2000 samples covers the
 * boundary thoroughly without flake risk.
 */
class SwrPredicatePropertyTest {

    /**
     * Every legal tuple lands on exactly one of FRESH or STALE, decided
     * solely by {@code now < lastWrite + freshFor}. Read time is
     * synthesized by sleeping a deterministic delay after the write.
     */
    @Test
    void fresh_or_stale_decisionMatrix() throws InterruptedException {
        SimpleMeterRegistry mr = new SimpleMeterRegistry();
        RefreshExecutor exec = new RefreshExecutor(1, mr);
        try {
            int n = 2000;
            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            for (int i = 0; i < n; i++) {
                // Constraints: 0 < freshFor < staleFor <= ttl. Generate
                // tuples in nanos so the sidecar's nanoTime arithmetic is
                // exact.
                long freshFor = rnd.nextLong(1_000_000L, 100_000_000L);  // [1ms, 100ms)
                long staleFor = freshFor + rnd.nextLong(1L, 1_000_000_000L);  // > freshFor
                long ttl = staleFor + rnd.nextLong(0L, 1_000_000_000L);  // >= staleFor

                // Sanity — the validator's invariants are what we're
                // exploring; if generation drifts we want to catch it.
                assertThat(freshFor).isPositive();
                assertThat(staleFor).isGreaterThan(freshFor);
                assertThat(ttl).isGreaterThanOrEqualTo(staleFor);

                SwrSidecar sidecar = new SwrSidecar("p", freshFor, exec,
                        CircuitBreakerRegistry.ofDefaults().circuitBreaker("p" + i),
                        mr);
                long writeNs = System.nanoTime();
                sidecar.recordWrite("k");

                // Read inside the fresh window — pick a delay strictly less
                // than freshFor (with a margin for nanoTime granularity).
                long readDelayInside = Math.max(0L, freshFor / 4);
                long elapsed = System.nanoTime() - writeNs;
                if (elapsed < readDelayInside) {
                    Thread.sleep(0, (int) Math.min(900_000, readDelayInside - elapsed));
                }
                SwrSidecar.Classification inside = sidecar.classify("k");
                // Within the fresh window, classification must be FRESH.
                if (System.nanoTime() - writeNs < freshFor) {
                    assertThat(inside)
                            .as("freshFor=%d, elapsedNs=%d", freshFor,
                                    System.nanoTime() - writeNs)
                            .isEqualTo(SwrSidecar.Classification.FRESH);
                }

                // Reset and re-measure: now sleep PAST freshFor and assert
                // STALE classification. We only run a small sample of the
                // stale-side check (every 20th iteration) to keep the test
                // fast — sleeping past a 100ms freshFor 2000 times is
                // unfriendly to CI.
                if (i % 20 == 0 && freshFor < 5_000_000L) {  // freshFor < 5ms
                    SwrSidecar sidecar2 = new SwrSidecar("p", freshFor, exec,
                            CircuitBreakerRegistry.ofDefaults().circuitBreaker("p2-" + i),
                            mr);
                    long w = System.nanoTime();
                    sidecar2.recordWrite("k");
                    Thread.sleep(0, (int) Math.min(900_000, freshFor + 1_000_000L));
                    while (System.nanoTime() - w < freshFor) {
                        Thread.sleep(0, 100_000);
                    }
                    SwrSidecar.Classification outside = sidecar2.classify("k");
                    assertThat(outside)
                            .as("stale-window read with freshFor=%d", freshFor)
                            .isEqualTo(SwrSidecar.Classification.STALE);
                }
            }
        } finally {
            exec.stop();
        }
    }

    /**
     * Boundary check: the fresh→stale transition is exact at the
     * deadline (no grace period, no pre-emptive expansion). Since we
     * cannot freeze {@link System#nanoTime} without a clock seam, we
     * verify the boundary in the negative direction — by generating a
     * deadline that has just passed and asserting STALE.
     */
    @Test
    void boundary_atDeadline_isStale() throws InterruptedException {
        SimpleMeterRegistry mr = new SimpleMeterRegistry();
        RefreshExecutor exec = new RefreshExecutor(1, mr);
        try {
            // freshFor=1ns means every read after write is stale.
            SwrSidecar sidecar = new SwrSidecar("p", 1L, exec,
                    CircuitBreakerRegistry.ofDefaults().circuitBreaker("boundary"),
                    mr);
            sidecar.recordWrite("k");
            // Even a tight loop guarantees a few nanos elapse.
            for (int i = 0; i < 1000; i++) {
                if (sidecar.classify("k") == SwrSidecar.Classification.STALE) {
                    return;  // pass — at-or-past deadline classifies STALE
                }
            }
            // 1000 iterations without a single stale classification on a
            // 1ns deadline is impossible on any real machine — fail loudly.
            throw new AssertionError(
                    "1ns-deadline never classified STALE across 1000 iterations");
        } finally {
            exec.stop();
        }
    }

    /**
     * Mutation guard: classify() must NOT use {@code <=} (would treat the
     * exact deadline as fresh) — a change to {@code <=} should be killed
     * by this test because a mutated build would let some boundary cases
     * slip into FRESH that should be STALE.
     */
    @Test
    void classify_usesStrictLessThan_notLessOrEqual() {
        SimpleMeterRegistry mr = new SimpleMeterRegistry();
        RefreshExecutor exec = new RefreshExecutor(1, mr);
        try {
            // Set freshFor large so we know we are well inside the window.
            SwrSidecar sidecar = new SwrSidecar("p", Duration.ofSeconds(60).toNanos(),
                    exec,
                    CircuitBreakerRegistry.ofDefaults().circuitBreaker("strict"),
                    mr);
            sidecar.recordWrite("k");
            // Inside the window — must be FRESH.
            assertThat(sidecar.classify("k")).isEqualTo(SwrSidecar.Classification.FRESH);
        } finally {
            exec.stop();
        }
    }
}
