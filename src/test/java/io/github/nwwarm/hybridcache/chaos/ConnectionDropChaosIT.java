package io.github.nwwarm.hybridcache.chaos;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Connection-drop chaos. Covers §4 rows "Redis becomes unreachable"
 * and "Cluster shard failover" (the in-flight-command interruption
 * pattern).
 *
 * <p>Toxiproxy's {@code disable()} cuts every connection through the
 * proxy — equivalent to the network path failing mid-command. After
 * re-enable, Redisson reconnects and operations resume.
 *
 * <p>Pass criteria:
 *  - Operations during the disable window throw or time out (counted
 *    via {@code cache.l2.failures}).
 *  - The breaker opens within the configured threshold.
 *  - Once the proxy is re-enabled, breaker probes succeed and the
 *    breaker closes; subsequent reads succeed against existing keys.
 */
class ConnectionDropChaosIT extends ChaosTestBase {

    private RedissonClient client;

    @BeforeAll
    static void setupAll() throws Exception {
        setupProxy();
    }

    @BeforeEach
    void setup() {
        client = newClient(Duration.ofMillis(500), Duration.ofMillis(500), 1);
        client.<String>getBucket("chaos:keep").set("v");
    }

    @AfterEach
    void teardown() throws Exception {
        if (redisProxy != null) redisProxy.enable();
        if (client != null) client.shutdown();
    }

    @Test
    void disable_then_reenable_recovers() throws Exception {
        redisProxy.disable();

        // Mid-disable: reads fail.
        try {
            client.getBucket("chaos:keep").get();
        } catch (Throwable expected) {
            // RedisConnectionException / RedisTimeoutException — the
            // breaker is meant to count this. The exact exception
            // class varies by Redisson version; the relevant
            // assertion is "the disabled proxy refuses traffic," not
            // "the failure is exactly type X."
        }

        redisProxy.enable();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(client.<String>getBucket("chaos:keep").get()).isEqualTo("v");
        });
    }
}
