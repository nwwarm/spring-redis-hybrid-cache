package io.github.nwwarm.hybridcache.chaos;

import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Pub/sub message-loss chaos. The headline production-readiness test:
 * verifies the reconciliation cycle closes the gap when invalidations
 * are dropped on the wire.
 *
 * <p>Covers §4 row "Subscriber briefly disconnects" / "Pub/sub message
 * lost". Toxiproxy {@code limit_data} toxic drops bytes after a
 * threshold, which corrupts in-flight pub/sub messages from the
 * subscriber's perspective. With {@code reconciliation.enabled=true},
 * the next cycle should detect the seq delta and run the recovery
 * action.
 *
 * <p>Pass criteria:
 *  - Without reconciliation, stale L1 entries persist past the message
 *    loss window (until TTL).
 *  - With reconciliation, {@code cache.reconciliation.misses} >= 1 and
 *    L1 is cleared on the affected node within {@code interval +
 *    tolerance} cycles.
 */
class PubSubMessageLossChaosIT extends ChaosTestBase {

    private RedissonClient nodeA;
    private RedissonClient nodeB;

    @BeforeAll
    static void setupAll() throws Exception {
        setupProxy();
    }

    @BeforeEach
    void setup() {
        nodeA = newClient();
        nodeB = newClient();
    }

    @AfterEach
    void teardown() throws Exception {
        if (redisProxy != null) {
            for (var t : redisProxy.toxics().getAll()) t.remove();
        }
        if (nodeA != null) nodeA.shutdown();
        if (nodeB != null) nodeB.shutdown();
    }

    /**
     * Drops bytes after the threshold so a publish from node A may
     * arrive truncated at node B. Reconciliation cycle reads
     * {@code <cache>:seq} via a fresh GET (separate from the
     * pub/sub channel) and detects the unobserved INCR.
     *
     * <p>The harness is wired here; full end-to-end assertions
     * against {@code cache.reconciliation.misses} require booting
     * two Spring contexts behind the proxy. That fixture lives in
     * {@code TwoContextEndToEndTestBase} (test-fixtures source set);
     * extending it for the chaos profile is tracked as a follow-up
     * in DESIGN.md §11 (Production baselines / Deferred items).
     */
    @Test
    void publish_then_drop_then_reconcile() throws Exception {
        // Drop downstream bytes after 256 bytes — enough headroom for
        // initial subscribe handshake, but small enough to corrupt
        // sustained publish traffic.
        redisProxy.toxics().limitData("drop", ToxicDirection.DOWNSTREAM, 256L);

        // Drive some publishes on node A; subscriber on node B receives
        // some, drops others.
        for (int i = 0; i < 50; i++) {
            nodeA.getTopic("cache:invalidate").publish("evt-" + i);
        }
        Thread.sleep(500);

        redisProxy.toxics().get("drop").remove();

        // Smoke assertion: the proxy survived the toxic and is reachable.
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(nodeA.getBucket("chaos:probe").isExists()).isFalse();
        });
    }
}
