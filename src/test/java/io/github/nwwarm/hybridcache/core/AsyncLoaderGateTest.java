package io.github.nwwarm.hybridcache.core;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link AsyncLoaderGate} — the non-blocking variant of
 * {@link LoaderGate} for the 0.5.0 async/reactive path.
 *
 * <p>Coverage: fast-path tryAcquire, queue-of-waiters under contention,
 * FIFO release order (guardrail item 4), {@code orTimeout} →
 * {@link LoaderRejectedException}, supplier-throws-synchronously path,
 * gate-unconfigured short-circuit, metrics.
 */
class AsyncLoaderGateTest {

    private final SimpleMeterRegistry mr = new SimpleMeterRegistry();

    @Test
    void unconfiguredGate_shortCircuits_noPermitsTracked() throws Exception {
        AsyncLoaderGate gate = new AsyncLoaderGate("c", null, null, mr);
        assertThat(gate.isConfigured()).isFalse();

        AtomicInteger calls = new AtomicInteger();
        for (int i = 0; i < 10; i++) {
            CompletableFuture<String> f = gate.run("k", () -> {
                calls.incrementAndGet();
                return CompletableFuture.completedFuture("v");
            });
            assertThat(f.get(1, TimeUnit.SECONDS)).isEqualTo("v");
        }
        assertThat(calls.get()).isEqualTo(10);
        assertThat(gate.queueSize()).isZero();
    }

    @Test
    void fastPath_permitAvailable_runsImmediately() throws Exception {
        AsyncLoaderGate gate = new AsyncLoaderGate("c", 1, Duration.ofSeconds(1), mr);

        CompletableFuture<String> f = gate.run("k",
                () -> CompletableFuture.completedFuture("v"));
        assertThat(f.get(1, TimeUnit.SECONDS)).isEqualTo("v");
        assertThat(gate.availablePermits()).isEqualTo(1);
        assertThat(gate.queueSize()).isZero();
    }

    @Test
    void permitsExhausted_waitersQueueFifo_releasedInOrder() throws Exception {
        // 1 permit, 3 callers. The first wins immediately; the second and
        // third register as waiters. We block the first by having it
        // complete on a controlled CompletableFuture. After the first
        // releases, the second waiter is granted; after second releases,
        // the third. Verify the ordering by recording completion order.
        AsyncLoaderGate gate = new AsyncLoaderGate("c", 1, Duration.ofSeconds(2), mr);
        ConcurrentLinkedQueue<Integer> completionOrder = new ConcurrentLinkedQueue<>();

        CompletableFuture<String> first = new CompletableFuture<>();
        CompletableFuture<String> r1 = gate.run("k", () -> first);
        // Now permits are 0; r1 holds the permit. The two below queue.
        CompletableFuture<String> second = new CompletableFuture<>();
        CompletableFuture<String> r2 = gate.run("k", () -> second);
        CompletableFuture<String> third = new CompletableFuture<>();
        CompletableFuture<String> r3 = gate.run("k", () -> third);

        // Both r2 and r3 are queued (permit held by r1).
        await(() -> gate.queueSize() == 2);

        r1.thenAccept(v -> completionOrder.add(1));
        r2.thenAccept(v -> completionOrder.add(2));
        r3.thenAccept(v -> completionOrder.add(3));

        // Release first. Granting cascades: r2 takes the permit, runs
        // (loader returns the held `second` future). Until we complete
        // `second`, r3 stays queued.
        first.complete("v1");
        r1.get(1, TimeUnit.SECONDS);

        await(() -> gate.queueSize() == 1);  // r3 still queued
        // Complete second: r2 finishes, releases the permit, r3 is granted.
        second.complete("v2");
        r2.get(1, TimeUnit.SECONDS);
        third.complete("v3");
        r3.get(1, TimeUnit.SECONDS);

        assertThat(completionOrder).containsExactly(1, 2, 3);
    }

    @Test
    void waiter_timesOutWith_loaderRejectedException() {
        // 1 permit held by the first caller; second caller queues with
        // a 50ms acquire timeout. The first never releases its permit —
        // the second's orTimeout must fire and complete with
        // LoaderRejectedException (guardrail item 3 of async section).
        AsyncLoaderGate gate = new AsyncLoaderGate("c", 1, Duration.ofMillis(50), mr);
        CompletableFuture<String> first = new CompletableFuture<>();
        gate.run("k", () -> first);  // never released
        CompletableFuture<String> rejected = gate.run("k",
                () -> CompletableFuture.completedFuture("never-runs"));
        assertThatThrownBy(() -> rejected.get(2, TimeUnit.SECONDS))
                .hasCauseInstanceOf(LoaderRejectedException.class);
        // Rejections counter ticked exactly once.
        assertThat(rejectionsCount()).isEqualTo(1);
        // Cleanup so the test executor doesn't keep the held permit.
        first.complete("eventual");
    }

    @Test
    void supplierThrowsSync_completesExceptionally_releasesPermit() throws Exception {
        AsyncLoaderGate gate = new AsyncLoaderGate("c", 1, Duration.ofSeconds(1), mr);
        CompletableFuture<String> f = gate.run("k", () -> {
            throw new RuntimeException("supplier exploded");
        });
        assertThatThrownBy(() -> f.get(1, TimeUnit.SECONDS))
                .hasMessageContaining("supplier exploded");
        // Permit must be returned even on supplier-throws-synchronously,
        // otherwise the gate would deadlock on the next call.
        await(() -> gate.availablePermits() == 1);

        // Subsequent call still works — the gate is not poisoned.
        CompletableFuture<String> g = gate.run("k",
                () -> CompletableFuture.completedFuture("v"));
        assertThat(g.get(1, TimeUnit.SECONDS)).isEqualTo("v");
    }

    @Test
    void loaderFutureFails_releasesPermit() throws Exception {
        AsyncLoaderGate gate = new AsyncLoaderGate("c", 1, Duration.ofSeconds(1), mr);
        CompletableFuture<String> f = gate.run("k", () -> {
            CompletableFuture<String> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IllegalStateException("loader-future-failed"));
            return failed;
        });
        assertThatThrownBy(() -> f.get(1, TimeUnit.SECONDS))
                .hasCauseInstanceOf(IllegalStateException.class);
        await(() -> gate.availablePermits() == 1);
    }

    @Test
    void contention_n100_waitersAllRunExactlyOnce() throws Exception {
        // 1 permit, 100 callers. All must eventually run their loader
        // exactly once (no double-runs, no lost runs). Each loader takes
        // 5ms — total wait is bounded but well below the 5s acquire
        // timeout for the slowest waiter.
        int permits = 1;
        int callers = 100;
        AsyncLoaderGate gate = new AsyncLoaderGate("c", permits, Duration.ofSeconds(5), mr);
        AtomicInteger ran = new AtomicInteger();
        ExecutorService es = Executors.newFixedThreadPool(callers);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<CompletableFuture<Integer>> futures = new ArrayList<>();
            for (int i = 0; i < callers; i++) {
                CompletableFuture<Integer> wrapper = new CompletableFuture<>();
                es.submit(() -> {
                    try {
                        start.await();
                        gate.run("k", () -> {
                            int n = ran.incrementAndGet();
                            return CompletableFuture.supplyAsync(() -> {
                                try { Thread.sleep(2); } catch (InterruptedException ignored) {}
                                return n;
                            });
                        }).whenComplete((v, ex) -> {
                            if (ex != null) wrapper.completeExceptionally(ex);
                            else wrapper.complete(v);
                        });
                    } catch (InterruptedException e) {
                        wrapper.completeExceptionally(e);
                    }
                });
                futures.add(wrapper);
            }
            start.countDown();
            for (CompletableFuture<Integer> f : futures) {
                f.get(10, TimeUnit.SECONDS);
            }
            assertThat(ran.get()).isEqualTo(callers);
            assertThat(gate.availablePermits()).isEqualTo(permits);
            assertThat(gate.queueSize()).isZero();
        } finally {
            es.shutdownNow();
            es.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    private double rejectionsCount() {
        return mr.find("cache.loaders.async.rejections").counter().count();
    }

    private static void await(java.util.function.BooleanSupplier cond) {
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (System.nanoTime() < deadline) {
            if (cond.getAsBoolean()) return;
            try { Thread.sleep(2); } catch (InterruptedException ignored) {}
        }
        throw new AssertionError("condition not met within 2s");
    }
}
