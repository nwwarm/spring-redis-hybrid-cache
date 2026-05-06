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
        List<String> allowedPackages,
        Kryo kryo,
        Health health,
        Resilience resilience,
        boolean logKeys,
        String logKeySalt,
        StartupProbe startupProbe,
        Invalidation invalidation) {

    private static final Logger log = LoggerFactory.getLogger(CacheProperties.class);
    private static final Set<String> WARNED_NAMES = ConcurrentHashMap.newKeySet();

    public CacheProperties {
        if (caches == null) caches = Map.of();
        if (allowedPackages == null) allowedPackages = List.of();
        if (kryo == null) kryo = new Kryo(List.of());
        if (health == null) health = new Health(Duration.ofMillis(500));
        if (resilience == null) resilience = new Resilience(null);
        if (startupProbe == null) startupProbe = new StartupProbe(false, null, 1, null);
        if (invalidation == null) invalidation = new Invalidation(false);
        if (defaultSpec == null) {
            defaultSpec = new CacheSpec(
                    Tier.NEAR_CACHE,
                    Duration.ofHours(1),
                    10_000,
                    Duration.ofSeconds(5),
                    Duration.ofSeconds(30),
                    Codec.JSON,
                    null,
                    null,
                    null,
                    0.0,
                    null);
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

    /**
     * Redis connection configuration. The {@link Mode} selects which
     * Redisson topology is used; required fields differ per mode.
     *
     * <ul>
     *   <li>{@link Mode#SINGLE} — {@link #address} required.</li>
     *   <li>{@link Mode#CLUSTER} — {@link #addresses} required (one entry per
     *       node address); {@link #scanInterval} optional, defaults to 2000ms.</li>
     *   <li>{@link Mode#SENTINEL} — {@link #masterName} required;
     *       {@link #addresses} required (sentinel addresses).</li>
     * </ul>
     *
     * <p>{@link #password} is optional in all modes. {@link #mode} defaults to
     * {@link Mode#SINGLE} when unset, preserving backwards compatibility with
     * the original two-field schema.
     */
    public record Server(
            Mode mode,
            String address,
            List<String> addresses,
            String masterName,
            String password,
            Integer scanInterval) {

        public Server {
            if (mode == null) mode = Mode.SINGLE;
            if (addresses == null) addresses = List.of();
            switch (mode) {
                case SINGLE -> {
                    if (address == null || address.isBlank()) {
                        throw new IllegalArgumentException(
                                "cache.server.address is required when cache.server.mode=SINGLE");
                    }
                }
                case CLUSTER -> {
                    if (addresses.isEmpty()) {
                        throw new IllegalArgumentException(
                                "cache.server.addresses must contain at least one node "
                                        + "when cache.server.mode=CLUSTER");
                    }
                    if (addresses.stream().anyMatch(a -> a == null || a.isBlank())) {
                        throw new IllegalArgumentException(
                                "cache.server.addresses contains a blank entry "
                                        + "(cache.server.mode=CLUSTER)");
                    }
                }
                case SENTINEL -> {
                    if (masterName == null || masterName.isBlank()) {
                        throw new IllegalArgumentException(
                                "cache.server.master-name is required when cache.server.mode=SENTINEL");
                    }
                    if (addresses.isEmpty()) {
                        throw new IllegalArgumentException(
                                "cache.server.addresses must list sentinel addresses "
                                        + "when cache.server.mode=SENTINEL");
                    }
                    if (addresses.stream().anyMatch(a -> a == null || a.isBlank())) {
                        throw new IllegalArgumentException(
                                "cache.server.addresses contains a blank entry "
                                        + "(cache.server.mode=SENTINEL)");
                    }
                }
            }
        }
    }

    public enum Mode {
        SINGLE,
        CLUSTER,
        SENTINEL
    }

    /**
     * Kryo codec configuration. The Kryo wire format requires class
     * registration to be safe — without it, a payload can encode an
     * arbitrary class name and Kryo will instantiate it via reflection,
     * which is the same threat model as Jackson's permissive default
     * typing. The library enforces registration: any cache configured
     * with {@link Codec#KRYO} requires {@link #registeredClasses} to be
     * non-empty, validated at startup.
     *
     * <p>Each entry must be a fully-qualified class name resolvable via
     * the application classloader. Order matters for Kryo's registration
     * IDs across deployments; if you add a new class to the list, append
     * rather than insert in the middle, otherwise existing payloads will
     * deserialize with the wrong class id.
     */
    public record Kryo(List<String> registeredClasses) {
        public Kryo {
            if (registeredClasses == null) registeredClasses = List.of();
        }
    }

    /**
     * Health-indicator tuning.
     *
     * <p>{@link #pingTimeout} bounds the Redis EXISTS round-trip used by
     * the health endpoint. /actuator/health must not block on a Redis
     * incident — if the ping doesn't return within the timeout, the
     * indicator reports the breaker state and the ping as
     * "timeout" rather than waiting on the full Redis call.
     */
    public record Health(Duration pingTimeout) {
        public Health {
            if (pingTimeout == null || pingTimeout.isZero() || pingTimeout.isNegative()) {
                pingTimeout = Duration.ofMillis(500);
            }
        }
    }

    /**
     * Resilience-layer configuration. Currently scopes the circuit-breaker
     * defaults that every per-cache breaker inherits from. Additional
     * resilience primitives (bulkhead, rate limiter) can be added here without
     * a top-level schema change.
     */
    public record Resilience(CircuitBreaker circuitBreaker) {}

    /**
     * Optional fail-fast Redis reachability check at application startup.
     *
     * <p>Default behaviour is disabled — the library otherwise boots lazily
     * and surfaces Redis incidents via the per-cache circuit breakers, which
     * is the right behaviour for most deployments (a cache is allowed to
     * lose data; the application must not refuse to start because of a
     * dependency further down). Enable this for environments where
     * <em>fail-to-boot</em> is explicitly preferred over
     * <em>boot-and-then-fail-requests</em> — for example, blue/green deploys
     * where a misconfigured slot should never accept traffic.
     *
     * <p><b>Skipped on LOCAL_ONLY-only deployments.</b> If every configured
     * cache (and {@code default-spec}) is {@code tier=LOCAL_ONLY}, Redis
     * is not on the request path and the probe does not run regardless of
     * {@link #enabled}. The bean still loads — it just no-ops with an info
     * log explaining why.
     *
     * <p>Total bound on probe time:
     * {@code (retries + 1) * timeout + retries * retryDelay}. With
     * defaults: {@code 2 * 5s + 1 * 1s = 11s}.
     *
     * @param enabled    enables the probe. Default {@code false}.
     * @param timeout    per-attempt connection timeout. Default 5s.
     * @param retries    number of additional attempts after the first
     *                   failure (so {@code attempts = retries + 1}).
     *                   Default {@code 1}.
     * @param retryDelay sleep between attempts. Default 1s.
     */
    public record StartupProbe(
            boolean enabled,
            Duration timeout,
            int retries,
            Duration retryDelay) {

        public StartupProbe {
            if (timeout == null) timeout = Duration.ofSeconds(5);
            if (retryDelay == null) retryDelay = Duration.ofSeconds(1);
        }
    }

    /**
     * Cross-node invalidation transport tuning.
     *
     * <p>The default ({@code shardedPubsub=false}) uses Redisson's
     * {@code RTopic}, which maps to Redis {@code PUBLISH}/{@code SUBSCRIBE}.
     * In a Redis Cluster every published message is gossipped to every
     * node via the cluster bus before delivery, regardless of which shard
     * owns the channel.
     *
     * <p>With {@code shardedPubsub=true} the dispatcher switches to
     * {@code RShardedTopic} ({@code SPUBLISH}/{@code SSUBSCRIBE}, Redis
     * 7.0+). The channel is pinned to one shard (chosen by hashing the
     * channel name), and every subscriber connects to that shard
     * directly. All nodes still receive every invalidation message —
     * which is what the library needs — but the cluster-bus
     * fan-out is eliminated, lowering inter-shard traffic on
     * deployments with high invalidation rates.
     *
     * <p><b>Cluster-only.</b> The flag is rejected at startup on
     * {@code mode=SINGLE} and {@code mode=SENTINEL} — sharded pub/sub
     * is a Redis Cluster feature.
     *
     * <p><b>Heterogeneous deployments are unsupported.</b> Either every
     * node uses sharded pub/sub or none do. A mix of {@code RTopic} and
     * {@code RShardedTopic} subscribers will not see each other's
     * messages.
     *
     * <p><b>No automatic Redis-version probe.</b> If you enable this
     * against a {@code <}7.0 server, Redisson will surface a clear error
     * on first publish — adding our own version check would be more
     * code than it's worth.
     */
    public record Invalidation(boolean shardedPubsub) {}

    /**
     * Circuit-breaker configuration. Used in two places:
     *
     * <ul>
     *   <li>{@code cache.resilience.circuit-breaker.*} — defaults that apply to
     *       every per-cache breaker. Missing fields fall back to the library's
     *       hardcoded defaults (sliding-window=20, min-calls=10,
     *       failure-rate=50, slow-call-duration=500ms, slow-call-rate=80,
     *       wait-in-open=30s, half-open-permitted=3).</li>
     *   <li>{@code cache.caches.<name>.circuit-breaker.*} — per-cache overlay.
     *       Any field set here overrides the corresponding default; unset
     *       fields inherit.</li>
     * </ul>
     *
     * <p>All fields are boxed so {@code null} can mean "inherit" — a primitive
     * default of 0 would be ambiguous with a deliberate 0 (and would fail
     * Resilience4j's own range checks anyway).
     *
     * <p>{@code recordExceptions} is intentionally not exposed — it is a
     * property of what "Redis is unhealthy" means (see README), not a
     * per-deployment knob.
     */
    public record CircuitBreaker(
            Integer slidingWindowSize,
            Integer minimumNumberOfCalls,
            Float failureRateThreshold,
            Duration slowCallDurationThreshold,
            Float slowCallRateThreshold,
            Duration waitDurationInOpenState,
            Integer permittedNumberOfCallsInHalfOpenState) {}

    public record CacheSpec(
            Tier tier,
            Duration ttl,
            long maximumSize,
            Duration lockWait,
            Duration lockLease,
            Codec codec,
            CircuitBreaker circuitBreaker,
            Integer maxConcurrentLoaders,
            Duration loaderAcquireTimeout,
            double ttlJitterRatio,
            Duration maxIdle) {

        public CacheSpec {
            if (tier == null) tier = Tier.NEAR_CACHE;
            if (ttl == null) ttl = Duration.ofHours(1);
            if (maximumSize <= 0) maximumSize = 10_000;
            if (lockWait == null) lockWait = Duration.ofSeconds(5);
            if (lockLease == null) lockLease = Duration.ofSeconds(30);
            if (codec == null) codec = Codec.JSON;
            // maxConcurrentLoaders null = unlimited (back-compat default).
            // loaderAcquireTimeout defaults to lockWait so users who only set
            // maxConcurrentLoaders get a sensible wait without an extra knob,
            // but the two are independently overridable.
            if (loaderAcquireTimeout == null) loaderAcquireTimeout = lockWait;
            // ttlJitterRatio defaults to 0.0 (no jitter, pre-0.4.0 byte-identical
            // path). Range/tier validation happens in CacheSpecValidator so the
            // failure surfaces alongside other config violations.
            // maxIdle defaults to null (no idle eviction). Range/tier validation
            // also lives in CacheSpecValidator.
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
