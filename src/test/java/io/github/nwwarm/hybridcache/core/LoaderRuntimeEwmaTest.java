package io.github.nwwarm.hybridcache.core;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LoaderRuntimeEwmaTest {

    @Test
    void initialState_zero() {
        LoaderRuntimeEwma ewma = new LoaderRuntimeEwma();
        assertThat(ewma.getNanos()).isZero();
    }

    @Test
    void firstSample_seedsExactly() {
        // First sample bypasses the EWMA blend so the estimator
        // initializes to the actual sample, not alpha · sample (which
        // would underestimate by 5x with default alpha=0.2).
        LoaderRuntimeEwma ewma = new LoaderRuntimeEwma();
        ewma.record(50_000_000L);  // 50ms
        assertThat(ewma.getNanos()).isEqualTo(50_000_000L);
    }

    @Test
    void subsequentSamples_blendPerEwmaFormula() {
        // alpha=0.5 makes the blend math easy to verify by hand.
        LoaderRuntimeEwma ewma = new LoaderRuntimeEwma(0.5);
        ewma.record(100L);                  // seed: 100
        assertThat(ewma.getNanos()).isEqualTo(100L);
        ewma.record(200L);                  // 0.5 * 200 + 0.5 * 100 = 150
        assertThat(ewma.getNanos()).isEqualTo(150L);
        ewma.record(50L);                   // 0.5 * 50 + 0.5 * 150 = 100
        assertThat(ewma.getNanos()).isEqualTo(100L);
    }

    @Test
    void zeroOrNegativeSamples_dropped() {
        // Clock-walked-backwards or sub-nanosecond "loaders" are not useful
        // signal; silently dropping keeps the estimator clean without
        // surfacing exceptions on a hot path.
        LoaderRuntimeEwma ewma = new LoaderRuntimeEwma();
        ewma.record(0L);
        ewma.record(-1L);
        ewma.record(-1_000_000L);
        assertThat(ewma.getNanos()).isZero();
        ewma.record(42L);
        assertThat(ewma.getNanos()).isEqualTo(42L);
        ewma.record(-1L);   // still no-op after seeding
        assertThat(ewma.getNanos()).isEqualTo(42L);
    }

    @Test
    void boundaryAroundZero_strictDropAtZero() {
        // Pin the exact boundary: 0 dropped, 1 accepted. Mutation guard —
        // a change from `<= 0` to `< 0` would let zero seed the EWMA.
        LoaderRuntimeEwma ewma = new LoaderRuntimeEwma();
        ewma.record(0L);
        assertThat(ewma.getNanos()).isZero();
        ewma.record(1L);
        assertThat(ewma.getNanos()).isEqualTo(1L);
    }

    @Test
    void alphaAccessor_returnsConstructedValue() {
        // Alpha test seam — pinned so mutation tests can't replace the
        // accessor with `return 0.0d` without test failure.
        assertThat(new LoaderRuntimeEwma(0.5).alpha()).isEqualTo(0.5);
        assertThat(new LoaderRuntimeEwma(0.2).alpha()).isEqualTo(0.2);
        assertThat(new LoaderRuntimeEwma().alpha()).isEqualTo(0.2);
    }

    @Test
    void invalidAlpha_rejected() {
        assertThatThrownBy(() -> new LoaderRuntimeEwma(0.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("alpha must be in (0, 1]");
        assertThatThrownBy(() -> new LoaderRuntimeEwma(-0.1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new LoaderRuntimeEwma(1.5))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void boundaryAlpha_oneMeansNoSmoothing() {
        // alpha=1 means each sample fully replaces the EWMA — useful
        // sanity check that the upper boundary is allowed.
        LoaderRuntimeEwma ewma = new LoaderRuntimeEwma(1.0);
        ewma.record(10L);
        ewma.record(20L);
        ewma.record(30L);
        assertThat(ewma.getNanos()).isEqualTo(30L);
    }

    @Test
    void concurrentRecord_doesNotLoseSamples() throws Exception {
        // 8 threads each record 1000 samples of value 100. Final EWMA must
        // not be far from 100 — under accumulateAndGet, blend retries on
        // CAS contention, and the worst case is "EWMA converges to mean of
        // submitted samples." Since all samples are the same value, the
        // converged estimate must equal that value.
        LoaderRuntimeEwma ewma = new LoaderRuntimeEwma();
        int threads = 8;
        int perThread = 1_000;
        ExecutorService es = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        try {
            for (int t = 0; t < threads; t++) {
                es.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < perThread; i++) {
                            ewma.record(100L);
                        }
                    } catch (InterruptedException ignored) {
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
            // Convergence: blend of 100 with 100 is always 100.
            assertThat(ewma.getNanos()).isEqualTo(100L);
        } finally {
            es.shutdown();
            es.awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
