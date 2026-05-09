package io.github.nwwarm.hybridcache.config;

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
        Invalidation invalidation,
        PreloaderGlobal preloader,
        Refresh refresh) {

    private static final Logger log = LoggerFactory.getLogger(CacheProperties.class);
    private static final Set<String> WARNED_NAMES = ConcurrentHashMap.newKeySet();

    public CacheProperties {
        if (caches == null) caches = Map.of();
        if (allowedPackages == null) allowedPackages = List.of();
        if (kryo == null) kryo = new Kryo(List.of());
        if (health == null) health = new Health(Duration.ofSeconds(2));
        if (resilience == null) resilience = new Resilience(null);
        if (startupProbe == null) startupProbe = new StartupProbe(false, null, 1, null);
        if (invalidation == null) invalidation = new Invalidation(false);
        if (preloader == null) preloader = new PreloaderGlobal(4);
        if (refresh == null) refresh = new Refresh(4);
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
                    null,
                    null,
                    null,
                    null,
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
     *
     * <p>Default 2s. Sub-millisecond on a warm Redisson connection in
     * steady state, so 2s is roughly three orders of magnitude of
     * headroom: enough to absorb GC pauses and scheduler jitter on a
     * loaded JVM (a CI runner mid-suite, a node with co-tenants) without
     * crossing the timeout typical of a Kubernetes liveness probe (which
     * is the indicator's real production caller, and runs on multi-second
     * budgets). Operators wanting a tighter signal can drop this; the
     * 500ms default that shipped in 0.2.0 was tight enough that a normal
     * loaded-JVM stall could trip it and report L2 as broken when it
     * wasn't.
     */
    public record Health(Duration pingTimeout) {
        public Health {
            if (pingTimeout == null || pingTimeout.isZero() || pingTimeout.isNegative()) {
                pingTimeout = Duration.ofSeconds(2);
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
     * Global preloader knobs. The per-cache configuration lives on
     * {@link CacheSpec#preloader()}; this record carries settings that span
     * every preloader-enabled cache.
     *
     * @param schedulerPoolSize threads in the shared scheduler that runs
     *        <em>periodic stores only</em>. Sized for steady-state idle work
     *        (every cache stores once per {@code store-interval}, default
     *        10m), not for the boot prefetch burst — the burst runs on a
     *        separate executor, sized by the sum of per-cache
     *        {@code prefetch-concurrency}, that exists only until prefetch
     *        deadlines fire. Default {@code 4}; see
     *        {@code docs/preloader-design.md} §7 for rationale.
     */
    public record PreloaderGlobal(int schedulerPoolSize) {
        public PreloaderGlobal {
            if (schedulerPoolSize <= 0) schedulerPoolSize = 4;
        }
    }

    /**
     * Shared refresh-executor sizing (0.5.0).
     *
     * <p>Single fixed-size pool that drains SWR stale-window dispatches,
     * RA probabilistic dispatches, and async-loader hops off the Netty
     * event loop. Sized to absorb a small burst of concurrent refreshes;
     * sustained queueing on {@code cache.refresh.queue.size} is the
     * signal to tune up.
     *
     * @param schedulerPoolSize threads. Default {@code 4}; minimum 1
     *                          (zero or negative coerces to default).
     */
    public record Refresh(int schedulerPoolSize) {
        public Refresh {
            if (schedulerPoolSize <= 0) schedulerPoolSize = 4;
        }
    }

    /**
     * Per-cache near-cache preloader configuration.
     *
     * <p>When {@link #enabled} is {@code true} on a {@code NEAR_CACHE} tier,
     * the library persists the L1 key set to a snapshot file on disk every
     * {@link #storeInterval}; on startup the file is loaded and a bounded
     * prefetch fan-out re-populates L1 from L2 — closing the cold-start
     * latency gap for applications with predictable working sets.
     *
     * <p><b>L2-only prefetch.</b> At startup the cache is repopulated from
     * L2 only. Keys whose values have already expired in L2 stay cold until
     * first access; the loader runs through the normal {@code @Cacheable}
     * path. There is no knob to opt into loader-driven warmup — invoking
     * the loader during startup would create a thundering-herd against the
     * source-of-truth at exactly the worst moment.
     *
     * <p><b>Tiers.</b> Allowed only on {@code NEAR_CACHE}. Rejected on
     * {@code LOCAL_ONLY} (no L2 to prefetch from) and
     * {@code DISTRIBUTED_ONLY} (no L1 to populate).
     *
     * <p>See {@code docs/preloader-design.md} for the full design.
     *
     * @param enabled            opt-in. Default {@code false}.
     * @param directory          where the snapshot file lives. {@code null}
     *                           defaults to
     *                           {@code ${java.io.tmpdir}/hybrid-cache/<application-name>/<cache-name>}.
     *                           Library creates it if missing, owner-only on
     *                           POSIX ({@code 0700} directory, {@code 0600}
     *                           file). On Windows, snapshot file permissions
     *                           are not enforced; the snapshot inherits the
     *                           parent directory's ACL. Operators concerned
     *                           about disk-resident key sets should choose a
     *                           {@code directory} whose ACL restricts access
     *                           appropriately.
     * @param storeInterval      how often the key set is written. Default
     *                           {@code 10m}.
     * @param storeInitialDelay  delay before the first store. Default
     *                           {@code 1m}. Avoids a write storm during
     *                           warm-up.
     * @param prefetchConcurrency bounded concurrency for the startup
     *                           prefetch fan-out, per cache. Default
     *                           {@code 16}.
     * @param prefetchTimeout    cap on total prefetch time at startup;
     *                           remaining keys are skipped after the
     *                           deadline. Default {@code 30s}.
     * @param maxStoredKeys      hard cap on lines written per snapshot.
     *                           {@code null} (default) means no cap beyond
     *                           {@code maximum-size}. <b>Truncation order
     *                           is not guaranteed</b> — the library does
     *                           not promise any particular order when the
     *                           cap is hit.
     */
    public record Preloader(
            boolean enabled,
            String directory,
            Duration storeInterval,
            Duration storeInitialDelay,
            int prefetchConcurrency,
            Duration prefetchTimeout,
            Integer maxStoredKeys) {

        public Preloader {
            if (storeInterval == null) storeInterval = Duration.ofMinutes(10);
            if (storeInitialDelay == null) storeInitialDelay = Duration.ofMinutes(1);
            if (prefetchConcurrency <= 0) prefetchConcurrency = 16;
            if (prefetchTimeout == null) prefetchTimeout = Duration.ofSeconds(30);
            // maxStoredKeys: null = unbounded (beyond maximum-size)
        }
    }

    /**
     * Per-cache stale-while-revalidate tuning (0.5.0).
     *
     * <p>L1 entries gain two deadlines: <b>fresh-until</b>
     * ({@code now + freshFor}) and <b>stale-until</b> ({@code now + staleFor},
     * also the Caffeine {@code expireAfterWrite}). Reads inside
     * {@code [write, fresh-until)} return synchronously and do nothing
     * extra. Reads inside {@code [fresh-until, stale-until)} return the
     * stale value <em>and</em> dispatch an async refresh on the shared
     * refresh executor. Reads past {@code stale-until} miss L1, fall
     * through to L2, then to the loader.
     *
     * <p><b>Tier rules.</b> Allowed on {@code NEAR_CACHE} and
     * {@code LOCAL_ONLY}. Rejected on {@code DISTRIBUTED_ONLY} (no L1
     * to apply dual deadlines to).
     *
     * <p><b>Per-call configuration was dropped from 0.5.0.</b> SWR is
     * configured per cache, not per call. Operators who need both modes
     * for the same logical data configure two cache names. See §10
     * decision log for the rationale (custom annotations rejected by §9).
     *
     * @param freshFor in-window where reads return synchronously without
     *                 dispatching a refresh. Validator rule: {@code > 0} and
     *                 {@code < staleFor}.
     * @param staleFor outer window — reads past this fall through to L2 and
     *                 the loader. Equals the Caffeine {@code expireAfterWrite}.
     *                 Validator rule: {@code > freshFor} and {@code <= ttl}.
     */
    public record Swr(Duration freshFor, Duration staleFor) {}

    /**
     * Per-cache refresh-ahead tuning (0.5.0).
     *
     * <p>Refresh-ahead is implemented as a <b>probabilistic firing mode of
     * SWR</b>, not as an independent feature. It reuses SWR's async refresh
     * pipeline, in-flight collapse, executor, and breaker — only the
     * predicate that decides "refresh now" differs. Inside the fresh window
     * (i.e. before the SWR stale window opens), every read evaluates the
     * XFetch probability function:
     *
     * <pre>{@code
     * now() − lastWrite + β · loaderRuntimeEstimate · (−ln(rand())) ≥ freshFor
     * }</pre>
     *
     * <p>{@code loaderRuntimeEstimate} is a per-cache exponentially-weighted
     * moving average of measured loader durations — there is no per-key
     * estimate, since per-key would duplicate Caffeine's frequency sketch
     * (see §10 0.5.0 decisions and §2 RA architecture).
     *
     * <p><b>Tier rules.</b> Allowed on {@code NEAR_CACHE} and
     * {@code LOCAL_ONLY} (the same set as SWR; RA is a SWR mode). Rejected
     * on {@code DISTRIBUTED_ONLY} — no L1 means no fresh-window predicate
     * to evaluate.
     *
     * <p><b>Requires SWR.</b> {@code refresh-ahead.enabled=true} requires
     * {@code swr.fresh-for} and {@code swr.stale-for} to be configured for
     * the same cache. The validator rejects RA-without-SWR at boot, rather
     * than silently treating "RA does nothing" as a runtime mystery.
     *
     * <p><b>{@code beta=0} is a misconfiguration.</b> The XFetch predicate
     * with β=0 degenerates to "now − lastWrite ≥ freshFor" — equivalent to
     * "the entry is already stale" and fires nothing extra. Operators who
     * want RA off set {@code refresh-ahead.enabled=false}; β=0 is rejected
     * by the validator. See {@code # implementation guardrails / ## Refresh-ahead}.
     *
     * @param enabled opt-in. Default {@code false}.
     * @param beta    XFetch aggressiveness multiplier — higher β fires
     *                refresh earlier in the fresh window. Default
     *                {@code 1.0} (XFetch's recommended default per Vattani
     *                et al.). Validator rule: {@code > 0}.
     */
    public record RefreshAhead(boolean enabled, double beta) {
        public RefreshAhead {
            // No defaulting of beta here: a user-supplied beta=0 must
            // reach the validator unchanged so the misconfiguration
            // surfaces at boot. Only the all-defaults factory below
            // applies the 1.0 default for "enabled with no explicit beta."
        }

        /** Convenience factory: enabled with default beta=1.0. */
        public static RefreshAhead enabledWithDefaults() {
            return new RefreshAhead(true, 1.0);
        }
    }

    /**
     * Per-cache reconciliation tuning (0.5.0).
     *
     * <p>Pub/sub is at-most-once. A subscriber that drops an
     * {@code OP_INVALIDATE} or {@code OP_CLEAR} message currently serves
     * stale L1 entries until TTL. Reconciliation closes that gap by having
     * each subscriber periodically read the per-cache publish-sequence
     * counter ({@code <cache>:seq}) and compare it against the locally
     * observed maximum from received messages. When
     * {@code redisSeq − lastObservedSeq > miss-tolerance}, the cache
     * declares a missed-message event and runs the per-tier recovery
     * action (clear local L1 on {@code NEAR_CACHE}; force a generation
     * pointer refresh on {@code DISTRIBUTED_ONLY}).
     *
     * <p><b>Tier rules.</b> Allowed on {@code NEAR_CACHE} (the headline
     * case — recovers missed per-key invalidations) and
     * {@code DISTRIBUTED_ONLY} (recovers missed {@code OP_CLEAR}).
     * Rejected on {@code LOCAL_ONLY} — there is no cross-node coherence
     * problem to reconcile against, and the {@code <cache>:seq} counter
     * would never advance on a single-node cache.
     *
     * <p><b>Cost on the publish path.</b> Each successful publish gains
     * one extra Redis op ({@code INCR <cache>:seq}) and ~16 bytes
     * additional payload (the {@code seq} long on the
     * {@link io.github.nwwarm.hybridcache.invalidation.InvalidationMessage}). Cold-load completions do not publish,
     * so they do not {@code INCR}; the cold-load suppression semantics
     * from 0.3.0 are preserved.
     *
     * @param enabled        opt-in. Default {@code false}.
     * @param interval       period between cycles. Default {@code 60s} —
     *                       matches Hazelcast's
     *                       {@code hazelcast.invalidation.reconciliation.interval.seconds}
     *                       default. Validator rule: {@code > 0}.
     * @param missTolerance  threshold below which a delta between
     *                       canonical and locally-observed seq is treated
     *                       as in-flight noise rather than a miss. Default
     *                       {@code 5}. Higher values forgive larger
     *                       in-flight bursts; lower values catch losses
     *                       sooner. Validator rule: {@code >= 0}.
     */
    public record Reconciliation(
            boolean enabled,
            Duration interval,
            int missTolerance) {

        public Reconciliation {
            if (interval == null) interval = Duration.ofSeconds(60);
            // missTolerance: 0 is valid — operators who genuinely want to
            // catch every gap can set it to 0 and accept the false-positive
            // exposure during in-flight bursts. Negative is rejected by the
            // validator, not coerced here.
        }
    }

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
            Duration maxIdle,
            Preloader preloader,
            Reconciliation reconciliation,
            Swr swr,
            RefreshAhead refreshAhead) {

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
            // preloader defaults to null = disabled. Tier validation
            // (NEAR_CACHE-only) also lives in CacheSpecValidator.
            // reconciliation defaults to null = disabled. Tier validation
            // (no LOCAL_ONLY) lives in CacheSpecValidator.
            // swr defaults to null = disabled. Validator enforces
            // 0 < freshFor < staleFor <= ttl and rejects DISTRIBUTED_ONLY.
            // refreshAhead defaults to null = disabled. Validator enforces
            // tier ∈ {NEAR_CACHE, LOCAL_ONLY}, beta > 0, and the requires-SWR
            // dependency at boot rather than letting RA silently never fire.
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
