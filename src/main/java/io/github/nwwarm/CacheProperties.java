package io.github.nwwarm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Cache configuration loaded from application.yml under the {@code cache} prefix.
 *
 * <p>Each cache name is mapped to a {@link CacheSpec} which controls its tier
 * (local-only, distributed-only, or near-cache), TTL, size, and lock behavior.
 * Caches not explicitly configured fall back to {@link #defaultSpec()}.
 *
 * <p>{@link #allowedPackages()} narrows the Jackson polymorphic type validator
 * used by the default Redisson codec. If empty, a permissive default is used
 * with a warning at startup — strongly recommended to configure this for
 * production deployments to limit the deserialization attack surface.
 */
@ConfigurationProperties(prefix = "cache")
public record CacheProperties(
        Server server,
        Map<String, CacheSpec> caches,
        CacheSpec defaultSpec,
        String nodeId,
        List<String> allowedPackages) {

    private static final Logger log = LoggerFactory.getLogger(CacheProperties.class);
    private static final Set<String> WARNED_NAMES = ConcurrentHashMap.newKeySet();

    public CacheProperties {
        if (caches == null) caches = Map.of();
        if (allowedPackages == null) allowedPackages = List.of();
        if (defaultSpec == null) {
            defaultSpec = new CacheSpec(
                    Tier.NEAR_CACHE,
                    Duration.ofHours(1),
                    10_000,
                    Duration.ofSeconds(5),
                    Duration.ofSeconds(30),
                    Codec.JSON);
        }
    }

    public CacheSpec specFor(String cacheName) {
        CacheSpec spec = caches.get(cacheName);
        if (spec == null) {
            if (WARNED_NAMES.add(cacheName)) {
                log.warn("No explicit configuration for cache '{}'; using default spec ({})",
                        cacheName, defaultSpec.tier());
            }
            return defaultSpec;
        }
        return spec;
    }

    public record Server(String address, String password) {}

    public record CacheSpec(
            Tier tier,
            Duration ttl,
            long maximumSize,
            Duration lockWait,
            Duration lockLease,
            Codec codec) {

        public CacheSpec {
            if (tier == null) tier = Tier.NEAR_CACHE;
            if (ttl == null) ttl = Duration.ofHours(1);
            if (maximumSize <= 0) maximumSize = 10_000;
            if (lockWait == null) lockWait = Duration.ofSeconds(5);
            if (lockLease == null) lockLease = Duration.ofSeconds(30);
            if (codec == null) codec = Codec.JSON;
        }
    }

    /**
     * Per-cache wire-format codec for L2.
     *
     * <ul>
     *   <li>{@link #JSON} — default. Debuggable from {@code redis-cli}, schema-tolerant,
     *       but ~2-3x larger payloads and ~2-3x slower than Kryo.</li>
     *   <li>{@link #KRYO} — compact binary. ~3-5x smaller, ~2-3x faster. Tradeoffs:
     *       payloads are not human-readable, schema evolution requires care, and the
     *       cache contents are tied to the JVM (no polyglot consumers).</li>
     * </ul>
     *
     * <p><b>Switching codec on an existing cache requires clearing it.</b> Old payloads
     * written with the previous codec cannot be deserialized with the new one. Either
     * call {@code @CacheEvict(allEntries = true)} once after deploy, or wait one TTL.
     */
    public enum Codec {
        JSON,
        KRYO
    }

    /**
     * Cache tier selection.
     *
     * <ul>
     *   <li>{@link #LOCAL_ONLY} — Caffeine only. Per-node, no cross-node coherence.
     *       Use for caches whose values are inherently per-instance (rate limiters,
     *       per-node ephemeral state).</li>
     *   <li>{@link #DISTRIBUTED_ONLY} — Redis only. No L1. Use when working set is
     *       too large for per-node memory or when cross-node coherence matters more
     *       than latency (sessions, idempotency keys).</li>
     *   <li>{@link #NEAR_CACHE} — both layers with topic-based invalidation.
     *       Default. The right choice for most read-heavy caches.</li>
     * </ul>
     */
    public enum Tier {
        LOCAL_ONLY,
        DISTRIBUTED_ONLY,
        NEAR_CACHE
    }
}
