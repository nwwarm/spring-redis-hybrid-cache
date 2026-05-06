package io.github.nwwarm.hybridcache.invalidation;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.api.listener.MessageListener;
import org.redisson.client.codec.Codec;
import org.springframework.context.SmartLifecycle;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Pure-Mockito coverage of {@link InvalidationDispatcher}'s
 * {@link SmartLifecycle} contract — no Redis container needed.
 *
 * <p>What this verifies:
 * <ul>
 *   <li>Constructor subscribes once via {@code topic.addListener}; the bean
 *       reports {@code isRunning() == true} immediately so non-Spring
 *       callers (every test fixture in this codebase) keep working.</li>
 *   <li>{@code start()} is idempotent — calling it on an already-running
 *       dispatcher does not double-subscribe.</li>
 *   <li>{@code stop()} unsubscribes exactly once and is idempotent.</li>
 *   <li>{@code start()} after {@code stop()} re-subscribes (re-init path).</li>
 *   <li>{@code stop(Runnable)} runs the callback after teardown — and runs
 *       it even if teardown threw, since Spring uses the callback as a
 *       sync signal and a missing callback wedges the LifecycleProcessor.</li>
 *   <li>{@code getPhase()} returns the documented constant; {@code
 *       isAutoStartup()} is true.</li>
 *   <li>{@code wasRedissonAliveAtStop()} captures Redisson liveness at
 *       stop time — {@code true} for the happy path used by the
 *       integration ordering test.</li>
 * </ul>
 */
class InvalidationDispatcherLifecycleTest {

    private RedissonClient redisson;
    private RTopic topic;
    private InvalidationDispatcher dispatcher;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        redisson = mock(RedissonClient.class);
        topic = mock(RTopic.class);
        when(redisson.getTopic(anyString(), any(Codec.class))).thenReturn(topic);
        when(topic.addListener(eq(InvalidationMessage.class),
                any(MessageListener.class))).thenReturn(42);
        when(redisson.isShutdown()).thenReturn(false);

        dispatcher = new InvalidationDispatcher(redisson, "node", new SimpleMeterRegistry());
    }

    @Test
    @SuppressWarnings("unchecked")
    void constructor_subscribesOnce_andReportsRunning() {
        verify(topic, times(1)).addListener(eq(InvalidationMessage.class),
                any(MessageListener.class));
        assertThat(dispatcher.isRunning()).isTrue();
    }

    @Test
    @SuppressWarnings("unchecked")
    void start_isIdempotent_whenAlreadyRunning() {
        dispatcher.start();
        // Constructor subscribed once; explicit start is a CAS-guarded no-op.
        verify(topic, times(1)).addListener(eq(InvalidationMessage.class),
                any(MessageListener.class));
        assertThat(dispatcher.isRunning()).isTrue();
    }

    @Test
    void stop_unsubscribesOnce_andIsIdempotent() {
        dispatcher.stop();
        verify(topic, times(1)).removeListener(42);
        assertThat(dispatcher.isRunning()).isFalse();

        dispatcher.stop();
        verify(topic, times(1)).removeListener(42);
        assertThat(dispatcher.isRunning()).isFalse();
    }

    @Test
    @SuppressWarnings("unchecked")
    void start_afterStop_resubscribes() {
        dispatcher.stop();

        // Override the stub so the re-subscribe returns a fresh listener id.
        // We only get here AFTER the constructor consumed the original 42, so
        // chaining thenReturn(42, 43) at setUp-time wouldn't address this call.
        when(topic.addListener(eq(InvalidationMessage.class),
                any(MessageListener.class))).thenReturn(43);

        dispatcher.start();

        verify(topic, times(2)).addListener(eq(InvalidationMessage.class),
                any(MessageListener.class));
        assertThat(dispatcher.isRunning()).isTrue();

        // The second stop must use the new listener id, not the stale one.
        dispatcher.stop();
        verify(topic).removeListener(43);
    }

    @Test
    void stopWithCallback_runsCallbackAfterTeardown() {
        AtomicBoolean called = new AtomicBoolean();
        dispatcher.stop(() -> {
            // Callback observes a fully-stopped dispatcher.
            assertThat(dispatcher.isRunning()).isFalse();
            called.set(true);
        });
        assertThat(called.get()).isTrue();
        verify(topic).removeListener(42);
    }

    @Test
    void stopWithCallback_runsCallback_evenIfRemoveListenerThrows() {
        // Spring's LifecycleProcessor uses the callback as a sync signal.
        // Failing to invoke it on the unhappy path leaves the processor
        // waiting forever — verify defensive behaviour.
        org.mockito.Mockito.doThrow(new RuntimeException("boom"))
                .when(topic).removeListener(42);

        AtomicBoolean called = new AtomicBoolean();
        dispatcher.stop(() -> called.set(true));

        assertThat(called.get()).isTrue();
        assertThat(dispatcher.isRunning()).isFalse();
    }

    @Test
    void wasRedissonAliveAtStop_isTrue_whenRedissonStillUp() {
        assertThat(dispatcher.wasRedissonAliveAtStop()).isNull();   // not stopped yet

        dispatcher.stop();

        assertThat(dispatcher.wasRedissonAliveAtStop())
                .as("redisson.isShutdown() returned false at stop time → correct ordering")
                .isTrue();
    }

    @Test
    void wasRedissonAliveAtStop_isFalse_whenRedissonAlreadyShutDown() {
        // Inverted-order scenario — the integration test shouldn't observe
        // this, but the diagnostic must report the violation accurately.
        when(redisson.isShutdown()).thenReturn(true);

        dispatcher.stop();

        assertThat(dispatcher.wasRedissonAliveAtStop()).isFalse();
    }

    @Test
    void getPhase_isAtTheTop() {
        assertThat(dispatcher.getPhase())
                .as("phase must be high so SmartLifecycle stops the dispatcher early")
                .isEqualTo(Integer.MAX_VALUE - 1024);
    }

    @Test
    void isAutoStartup_isTrue() {
        assertThat(dispatcher.isAutoStartup()).isTrue();
    }

    @Test
    void isSmartLifecycle_atTheInterfaceLevel() {
        // Sanity: the bean is wired through Spring's SmartLifecycle path.
        assertThat(dispatcher).isInstanceOf(SmartLifecycle.class);
    }
}
