package io.github.nwwarm.hybridcache.core;

import io.github.nwwarm.hybridcache.config.CacheProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit-level coverage of {@link ReconciliationCoordinator}'s lifecycle and
 * registration semantics. The detection-logic unit tests live in
 * {@link ReconciliationDecisionTest} (against a stub {@link Reconciler}) — this
 * file targets the coordinator itself so SmartLifecycle behavior is locked
 * down without spinning up a Spring context.
 */
class ReconciliationCoordinatorTest {

    private SimpleMeterRegistry meterRegistry;
    private ReconciliationCoordinator coordinator;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        coordinator = new ReconciliationCoordinator(meterRegistry);
    }

    @AfterEach
    void tearDown() {
        coordinator.stop();
    }

    @Test
    void register_disabled_isNoOp() {
        StubReconciler r = new StubReconciler("disabled");
        coordinator.register(r, new CacheProperties.Reconciliation(false, Duration.ofMillis(50), 0));
        assertThat(coordinator.registrationCount()).isZero();
    }

    @Test
    void register_null_isNoOp() {
        StubReconciler r = new StubReconciler("null-cfg");
        coordinator.register(r, null);
        assertThat(coordinator.registrationCount()).isZero();
    }

    @Test
    void register_idempotent_replacesExisting() {
        StubReconciler r = new StubReconciler("dupes");
        coordinator.register(r, new CacheProperties.Reconciliation(true, Duration.ofMinutes(10), 0));
        coordinator.register(r, new CacheProperties.Reconciliation(true, Duration.ofMinutes(10), 0));
        assertThat(coordinator.registrationCount())
                .as("re-registering the same name replaces, not duplicates")
                .isEqualTo(1);
    }

    @Test
    void deregister_removesRegistration() {
        StubReconciler r = new StubReconciler("removeme");
        coordinator.register(r, new CacheProperties.Reconciliation(true, Duration.ofMinutes(10), 0));
        assertThat(coordinator.registrationCount()).isEqualTo(1);
        coordinator.deregister("removeme");
        assertThat(coordinator.registrationCount()).isZero();
    }

    @Test
    void triggerCycleNow_invokesReconcileSynchronously() {
        StubReconciler r = new StubReconciler("trigger");
        coordinator.register(r, new CacheProperties.Reconciliation(true, Duration.ofMinutes(10), 0));
        coordinator.triggerCycleNow("trigger");
        assertThat(r.cycles.get())
                .as("test-seam triggerCycleNow runs reconcile() inline")
                .isEqualTo(1);
    }

    @Test
    void triggerCycleNow_unknownCache_isNoOp() {
        // No exception, no metric increment, just returns.
        coordinator.triggerCycleNow("never-registered");
    }

    @Test
    void scheduledCycle_fires() throws InterruptedException {
        StubReconciler r = new StubReconciler("sched");
        coordinator.register(r, new CacheProperties.Reconciliation(true, Duration.ofMillis(50), 0));
        // Wait up to 2s for a few cycles to fire.
        long deadline = System.currentTimeMillis() + 2_000;
        while (r.cycles.get() < 2 && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertThat(r.cycles.get())
                .as("at least 2 cycles should fire in 2 seconds at 50ms interval")
                .isGreaterThanOrEqualTo(2);
    }

    @Test
    void scheduledTask_swallowsThrowingReconciler() throws InterruptedException {
        // The Reconciler contract says reconcile() must not throw, but the
        // coordinator's task wrapper is defense-in-depth (item 1 of the
        // reconciliation guardrails). Verify a throwing reconciler doesn't
        // disable the schedule slot.
        StubReconciler r = new StubReconciler("throws") {
            @Override public void reconcile() {
                cycles.incrementAndGet();
                throw new RuntimeException("simulated bad reconciler");
            }
        };
        coordinator.register(r, new CacheProperties.Reconciliation(true, Duration.ofMillis(50), 0));
        // Even though the first cycle throws, the second one must still fire.
        long deadline = System.currentTimeMillis() + 2_000;
        while (r.cycles.get() < 3 && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertThat(r.cycles.get())
                .as("scheduler must keep firing despite throwing reconciler")
                .isGreaterThanOrEqualTo(3);
    }

    @Test
    void getPhase_isAtTheTop() {
        assertThat(coordinator.getPhase())
                .as("phase must match InvalidationDispatcher so cache layer drains together")
                .isEqualTo(Integer.MAX_VALUE - 1024);
    }

    @Test
    void isAutoStartup_isTrue() {
        assertThat(coordinator.isAutoStartup()).isTrue();
    }

    @Test
    void isSmartLifecycle() {
        assertThat(coordinator).isInstanceOf(SmartLifecycle.class);
    }

    @Test
    void stop_isIdempotent() {
        coordinator.stop();
        coordinator.stop();
        assertThat(coordinator.isRunning()).isFalse();
    }

    @Test
    void stopWithCallback_runsCallback() {
        java.util.concurrent.atomic.AtomicBoolean called = new java.util.concurrent.atomic.AtomicBoolean();
        coordinator.stop(() -> called.set(true));
        assertThat(called).isTrue();
    }

    private static class StubReconciler implements Reconciler {
        protected final AtomicInteger cycles = new AtomicInteger();
        private final String name;

        StubReconciler(String name) { this.name = name; }

        @Override public String getName() { return name; }
        @Override public void reconcile() { cycles.incrementAndGet(); }
        @Override public void onMessageObserved(long seq) { }
    }
}
