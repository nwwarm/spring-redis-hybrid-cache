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
 * Bandwidth-throttling chaos. Saturates the Redis link with a low
 * bandwidth budget so individual commands queue and slow-call
 * detection fires. Distinct from the latency injection scenario:
 * latency injects a constant delay, throttling slows actual byte
 * throughput.
 *
 * <p>Covers §4 row "Redis becomes slow" alongside the latency
 * scenario.
 */
class BandwidthThrottlingChaosIT extends ChaosTestBase {

    private RedissonClient client;

    @BeforeAll
    static void setupAll() throws Exception {
        setupProxy();
    }

    @BeforeEach
    void setup() {
        client = newClient(Duration.ofSeconds(2), Duration.ofSeconds(2), 1);
    }

    @AfterEach
    void teardown() throws Exception {
        if (redisProxy != null) {
            for (var t : redisProxy.toxics().getAll()) t.remove();
        }
        if (client != null) client.shutdown();
    }

    @Test
    void bandwidth_throttle_does_not_corrupt_state() throws Exception {
        // Throttle to 1 KB/s in both directions; commands serialize but
        // values remain coherent on completion.
        redisProxy.toxics().bandwidth("throttle-up", ToxicDirection.UPSTREAM, 1024);
        redisProxy.toxics().bandwidth("throttle-down", ToxicDirection.DOWNSTREAM, 1024);

        client.<String>getBucket("chaos:band").set("hello");

        // Drop the toxics; subsequent reads return the value written
        // through the throttle.
        redisProxy.toxics().get("throttle-up").remove();
        redisProxy.toxics().get("throttle-down").remove();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(client.<String>getBucket("chaos:band").get()).isEqualTo("hello");
        });
    }
}
