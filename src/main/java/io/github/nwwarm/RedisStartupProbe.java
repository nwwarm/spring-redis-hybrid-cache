package io.github.nwwarm;

import org.redisson.api.RFuture;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Optional fail-fast startup probe — verifies Redis is reachable before the
 * application is allowed to come up. Default behaviour is disabled; the
 * library otherwise boots lazily and surfaces failures via the per-cache
 * circuit breakers, which is the right behaviour for most deployments.
 *
 * <p>Implemented as {@link InitializingBean} so a probe failure surfaces as
 * a bean-initialisation exception, which Spring Boot translates into a
 * clean context-startup failure (non-zero exit, no half-up application).
 *
 * <h2>Probe action</h2>
 *
 * <p>Issues {@code redisson.getBucket("cache:startup-probe").isExistsAsync()}
 * and waits up to the configured timeout. The bucket is generic — it goes
 * through Redisson's normal connection routing, hits exactly one node (or
 * the slot owner in cluster mode), and returns. The alternative
 * {@code getNodesGroup().pingAll()} is topology-aware but stricter than
 * we need: a single replica being slow would fail the probe even though
 * cache traffic would be served fine. The bucket-exists check is a
 * minimal "can I issue a command" probe, which is the actual
 * pre-condition for the cache layer to function.
 *
 * <h2>Skipped on LOCAL_ONLY-only deployments</h2>
 *
 * <p>If {@code default-spec.tier} is {@code LOCAL_ONLY} <em>and</em> every
 * explicitly-configured cache is also {@code LOCAL_ONLY}, Redis is not on
 * the request path. The probe no-ops with an info log; the
 * {@code enabled=true} flag is honoured but not enforced where it cannot
 * matter.
 */
public class RedisStartupProbe implements InitializingBean {

    private static final Logger log = LoggerFactory.getLogger(RedisStartupProbe.class);
    private static final String PROBE_KEY = "cache:startup-probe";

    private final RedissonClient redisson;
    private final CacheProperties properties;

    public RedisStartupProbe(RedissonClient redisson, CacheProperties properties) {
        this.redisson = redisson;
        this.properties = properties;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (!isRedisOnCriticalPath()) {
            log.info("Startup probe enabled but every configured cache is LOCAL_ONLY; "
                    + "Redis is not on the request path — skipping probe.");
            return;
        }

        CacheProperties.StartupProbe cfg = properties.startupProbe();
        int attempts = cfg.retries() + 1;
        Throwable lastFailure = null;

        for (int attempt = 1; attempt <= attempts; attempt++) {
            try {
                runProbe(cfg.timeout());
                if (attempt > 1) {
                    log.info("Redis startup probe succeeded on attempt {} of {}", attempt, attempts);
                }
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "Redis startup probe interrupted on attempt " + attempt + " of " + attempts, e);
            } catch (Throwable t) {
                lastFailure = t;
                log.warn("Redis startup probe attempt {} of {} failed: {}",
                        attempt, attempts, t.getMessage());
                if (attempt < attempts) {
                    try {
                        Thread.sleep(cfg.retryDelay().toMillis());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(
                                "Redis startup probe interrupted between attempts", e);
                    }
                }
            }
        }

        throw new IllegalStateException(
                "Redis startup probe failed after " + attempts + " attempt(s)"
                        + " against topology " + describeTopology()
                        + " (per-attempt timeout " + cfg.timeout()
                        + ", retry-delay " + cfg.retryDelay() + ")."
                        + " Cause: " + (lastFailure == null
                                ? "<unknown>"
                                : lastFailure.getClass().getName() + ": " + lastFailure.getMessage()),
                lastFailure);
    }

    /**
     * Issues an {@code EXISTS} on a fixed sentinel key and waits up to
     * {@code timeout}. Throws {@link TimeoutException} on timeout,
     * {@link ExecutionException} (or its cause) on a Redis-level failure.
     */
    private void runProbe(Duration timeout)
            throws InterruptedException, ExecutionException, TimeoutException {
        RFuture<Boolean> future = redisson.getBucket(PROBE_KEY).isExistsAsync();
        future.toCompletableFuture().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    private boolean isRedisOnCriticalPath() {
        CacheProperties.CacheSpec defaultSpec = properties.defaultSpec();
        if (defaultSpec != null && defaultSpec.tier() != CacheProperties.Tier.LOCAL_ONLY) {
            return true;
        }
        return properties.caches().values().stream()
                .filter(java.util.Objects::nonNull)
                .anyMatch(s -> s.tier() != CacheProperties.Tier.LOCAL_ONLY);
    }

    private String describeTopology() {
        CacheProperties.Server server = properties.server();
        if (server == null) return "<unconfigured>";
        return switch (server.mode()) {
            case SINGLE -> "SINGLE " + server.address();
            case CLUSTER -> "CLUSTER (" + server.addresses().size() + " seed node(s))";
            case SENTINEL -> "SENTINEL master=" + server.masterName()
                    + " (" + server.addresses().size() + " sentinel(s))";
        };
    }
}
