package io.github.nwwarm.hybridcache.chaos;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.junit.jupiter.api.Tag;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.codec.JsonJacksonCodec;
import org.redisson.config.Config;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Duration;

/**
 * Base for chaos integration tests. Wires Redis behind Toxiproxy so
 * tests can inject latency, bandwidth throttling, and disconnects on
 * the application <-> Redis path.
 *
 * <p>Tagged {@code chaos} so the default {@code mvn verify} run skips
 * the suite. Run via {@code mvn -Pchaos verify} (see TESTING.md /
 * Chaos testing) which configures Failsafe to include the
 * {@code chaos} tag.
 *
 * <p>Mapping from §4 failure-mode rows to chaos scenarios is in
 * TESTING.md / Chaos testing.
 */
@Testcontainers
@Tag("chaos")
public abstract class ChaosTestBase {

    protected static final Network NETWORK = Network.newNetwork();

    @Container
    protected static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379)
            .withNetwork(NETWORK)
            .withNetworkAliases("redis");

    /**
     * Toxiproxy is configured as a generic container so we can specify
     * exactly which proxy listener port to expose. ToxiproxyContainer's
     * 1.x default of "16 ports preallocated" was removed in 2.x; using
     * GenericContainer gives a stable shape across testcontainers
     * versions.
     */
    @Container
    protected static final GenericContainer<?> TOXIPROXY = new GenericContainer<>(
            DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.7.0"))
            .withExposedPorts(8474, 8666)   // 8474 = admin API, 8666 = redis proxy
            .withNetwork(NETWORK)
            .withNetworkAliases("toxiproxy");

    protected static Proxy redisProxy;

    /** Call from the test's @BeforeAll. */
    protected static void setupProxy() throws IOException {
        ToxiproxyClient client = new ToxiproxyClient(
                TOXIPROXY.getHost(), TOXIPROXY.getMappedPort(8474));
        redisProxy = client.createProxy("redis", "0.0.0.0:8666", "redis:6379");
    }

    protected RedissonClient newClient() {
        return newClient(Duration.ofSeconds(2), Duration.ofSeconds(2), 1);
    }

    protected RedissonClient newClient(Duration connectTimeout,
                                       Duration readTimeout,
                                       int retryAttempts) {
        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        Config cfg = new Config();
        cfg.setCodec(new JsonJacksonCodec(mapper));
        cfg.useSingleServer()
                .setAddress("redis://" + TOXIPROXY.getHost() + ":" + TOXIPROXY.getMappedPort(8666))
                .setConnectTimeout((int) connectTimeout.toMillis())
                .setTimeout((int) readTimeout.toMillis())
                .setRetryAttempts(retryAttempts)
                .setRetryInterval(100);
        return Redisson.create(cfg);
    }
}
