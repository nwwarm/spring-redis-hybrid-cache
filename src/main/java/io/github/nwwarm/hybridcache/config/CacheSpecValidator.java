package io.github.nwwarm.hybridcache.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Semantic validator for all configured cache specs. Runs at startup
 * (called from the registry bean in {@link CacheConfig} before any
 * cache is constructed) and collects every violation into a single
 * {@link IllegalArgumentException} so operators see the complete list
 * on the first failed boot rather than one error per deploy cycle.
 *
 * <p>Structural validation (required fields, type coercion) is handled by
 * Spring's {@link org.springframework.boot.context.properties.ConfigurationProperties}
 * binding. This class checks semantic constraints between fields.
 *
 * <h2>Constraints checked</h2>
 *
 * <b>Per CacheSpec</b> (applies to {@code default-spec} and every named cache):
 * <ul>
 *   <li>E1: {@code ttl > 0} — zero/negative TTL means nothing is ever cached.</li>
 *   <li>E2: {@code lockWait == 0 OR lockLease > lockWait} — when
 *       {@code lockWait > 0}, a lease shorter than the wait makes it impossible
 *       to hold the lock long enough for the operation to complete.</li>
 *   <li>E3: {@code tier=LOCAL_ONLY} with {@code codec=KRYO} — LOCAL_ONLY has no
 *       Redis layer, so the codec is never invoked. Declaring KRYO here forces
 *       {@code registered-classes} to be maintained for a codec that does nothing,
 *       creating a maintenance trap (E4) without any benefit.</li>
 *   <li>E4: {@code codec=KRYO} (on a tier that has an L2) requires
 *       {@code cache.kryo.registered-classes} to be non-empty. Kryo without
 *       class registration accepts arbitrary class names from the wire —
 *       the same threat model as Jackson's permissive default typing.</li>
 *   <li>E15: {@code ttlJitterRatio ∈ [0.0, 0.5]}. Negative ratios make no
 *       physical sense; ratios above 0.5 produce expiries that can fall
 *       below half the configured TTL, which stops being "spread the spike"
 *       and starts being "halve the cache lifetime."</li>
 *   <li>E16: {@code ttlJitterRatio > 0} is rejected on
 *       {@code tier=DISTRIBUTED_ONLY}. Jitter only applies to the local
 *       Caffeine layer, and {@code DISTRIBUTED_ONLY} has no L1 — there is
 *       nothing for the value to apply to.</li>
 *   <li>E17: {@code maxIdle}, when set, must be positive. Zero or
 *       negative values express no useful semantic.</li>
 *   <li>E18: {@code maxIdle} is rejected on {@code tier=DISTRIBUTED_ONLY}.
 *       Same rationale as E16 — idle eviction acts on the L1 layer, and
 *       {@code DISTRIBUTED_ONLY} has no L1.</li>
 *   <li>E19: {@code maxIdle &gt; ttl} is rejected. A cold entry would
 *       expire on TTL before max-idle could ever fire, so the field is a
 *       no-op and almost certainly misconfiguration.</li>
 * </ul>
 *
 * <b>Per circuit-breaker block</b> (applies to
 * {@code cache.resilience.circuit-breaker.*} defaults and every
 * {@code cache.caches.<name>.circuit-breaker.*} overlay; null fields mean
 * "inherit" and are skipped):
 * <ul>
 *   <li>E5: {@code failureRateThreshold ∈ (0, 100]}</li>
 *   <li>E6: {@code slowCallRateThreshold ∈ (0, 100]}</li>
 *   <li>E7: {@code slidingWindowSize ≥ 1}</li>
 *   <li>E8: {@code minimumNumberOfCalls ≥ 1}</li>
 *   <li>E9: {@code minimumNumberOfCalls ≤ slidingWindowSize} — only checked
 *       when <em>both</em> fields appear in the same configuration block.
 *       A merged default+overlay combination violating this is not detected.</li>
 *   <li>E10: {@code slowCallDurationThreshold > 0}</li>
 *   <li>E11: {@code waitDurationInOpenState > 0}</li>
 *   <li>E12: {@code permittedNumberOfCallsInHalfOpenState ≥ 1}</li>
 * </ul>
 *
 * <b>Per reconciliation block</b> (only checked when {@code enabled=true},
 * mirroring the startup-probe philosophy):
 * <ul>
 *   <li>E29: {@code reconciliation.interval} must be positive. Zero would
 *       schedule the cycle at zero delay = continuous loop = the cache
 *       hammers Redis with GETs and the application starves; negative is
 *       nonsense.</li>
 *   <li>E30: {@code reconciliation.miss-tolerance} must be {@code >= 0}.
 *       Zero is valid (operators who want maximum sensitivity); negative
 *       inverts the comparison and would declare a miss on every cycle
 *       where the local seq slightly exceeded the canonical (impossible
 *       in steady state but possible during regression — see seq.regressions
 *       counter).</li>
 *   <li>E31: {@code reconciliation.enabled=true} requires {@code tier=NEAR_CACHE}
 *       or {@code tier=DISTRIBUTED_ONLY}. Rejected on {@code LOCAL_ONLY} —
 *       per-node by design, no cross-node coherence problem to reconcile
 *       against, and the {@code <cache>:seq} counter would never advance
 *       on a single-node cache.</li>
 * </ul>
 *
 * <b>Per preloader block</b> (only checked when {@code enabled=true},
 * mirroring the startup-probe philosophy):
 * <ul>
 *   <li>E24: {@code preloader.enabled=true} requires {@code tier=NEAR_CACHE}.
 *       Rejected on {@code LOCAL_ONLY} (no L2 to prefetch from) and
 *       {@code DISTRIBUTED_ONLY} (no L1 to populate).</li>
 *   <li>E25: {@code store-interval}, {@code store-initial-delay}, and
 *       {@code prefetch-timeout} must each be positive.</li>
 *   <li>E26: {@code prefetch-concurrency} must be {@code >= 1}.</li>
 *   <li>E27: directory creatability is checked at {@code PreloaderCoordinator.start()},
 *       not in this validator — the validator does not touch the filesystem.</li>
 *   <li>E28: {@code max-stored-keys}, when set, must be {@code >= 1}. {@code null} means unbounded.</li>
 * </ul>
 *
 * <b>Per invalidation block</b>:
 * <ul>
 *   <li>E23: {@code invalidation.shardedPubsub=true} is allowed only when
 *       {@code cache.server.mode=CLUSTER}. {@code SPUBLISH}/{@code SSUBSCRIBE}
 *       is a Redis Cluster–only capability; in {@code SINGLE} or
 *       {@code SENTINEL} the server has no shard concept, and Redisson would
 *       surface a confusing protocol error per publish at runtime.</li>
 * </ul>
 *
 * <b>Per startup-probe block</b> (only checked when
 * {@code cache.startup-probe.enabled=true} — a disabled probe with bogus
 * timings is harmless and we don't want to gate boot on unrelated typos):
 * <ul>
 *   <li>E20: {@code timeout} must be positive. Zero would surface as an
 *       immediate {@code TimeoutException} on every attempt, indistinguishable
 *       from "Redis is unreachable."</li>
 *   <li>E21: {@code retries} must be {@code >= 0}.</li>
 *   <li>E22: {@code retry-delay} must be {@code >= 0}.</li>
 * </ul>
 *
 * <b>Warnings</b> (logged, not thrown):
 * <ul>
 *   <li>W1: circuit-breaker overrides on {@code tier=LOCAL_ONLY} have no effect.</li>
 * </ul>
 *
 * <b>Intentionally not checked:</b>
 * <ul>
 *   <li>{@code maximumSize} — the compact constructor already clamps {@code ≤ 0}
 *       to 10,000; re-validating something already fixed would be contradictory.</li>
 *   <li>{@code failureRateThreshold} and {@code slowCallRateThreshold} not both
 *       null — both null simply inherits defaults, which is the normal path.</li>
 *   <li>Fields requiring Redis reachability — out of scope for startup validation.</li>
 * </ul>
 */
final class CacheSpecValidator {

    private static final Logger log = LoggerFactory.getLogger(CacheSpecValidator.class);

    private CacheSpecValidator() {}

    static void validate(CacheProperties properties) {
        List<String> violations = new ArrayList<>();

        // Global circuit-breaker defaults (null = all defaults, nothing to check)
        if (properties.resilience() != null && properties.resilience().circuitBreaker() != null) {
            validateCircuitBreaker("global circuit-breaker defaults",
                    properties.resilience().circuitBreaker(), violations);
        }

        // default-spec
        if (properties.defaultSpec() != null) {
            validateSpec("default-spec", properties.defaultSpec(), properties, violations);
        }

        // Named caches
        for (Map.Entry<String, CacheProperties.CacheSpec> entry : properties.caches().entrySet()) {
            if (entry.getValue() != null) {
                validateSpec("cache '" + entry.getKey() + "'",
                        entry.getValue(), properties, violations);
            }
        }

        // Startup probe (only validate timing fields when the probe is enabled —
        // a disabled probe with bogus timings is harmless, and validating them
        // anyway forces operators to fix unrelated typos before they can boot
        // with the probe off).
        if (properties.startupProbe() != null && properties.startupProbe().enabled()) {
            validateStartupProbe(properties.startupProbe(), violations);
        }

        // Invalidation transport (sharded pub/sub is a Redis Cluster feature;
        // SINGLE and SENTINEL deployments must reject it at startup).
        if (properties.invalidation() != null && properties.invalidation().shardedPubsub()) {
            validateShardedPubsub(properties, violations);
        }

        if (!violations.isEmpty()) {
            throw new IllegalArgumentException(
                    violations.size() + " cache configuration violation(s):\n  - "
                            + String.join("\n  - ", violations));
        }
    }

    private static void validatePreloader(String label,
                                          CacheProperties.CacheSpec spec,
                                          List<String> violations) {
        CacheProperties.Preloader p = spec.preloader();

        // E24: NEAR_CACHE-only. LOCAL_ONLY has no L2 to prefetch from;
        // DISTRIBUTED_ONLY has no L1 to populate. Either reject is loud.
        if (spec.tier() != CacheProperties.Tier.NEAR_CACHE) {
            violations.add(label + ": preloader.enabled=true requires"
                    + " tier=NEAR_CACHE (got " + spec.tier() + "). LOCAL_ONLY"
                    + " has no L2 to prefetch from; DISTRIBUTED_ONLY has no"
                    + " L1 to populate.");
        }

        // E25: every Duration must be positive. Zero is the no-useful-semantic
        // case (e.g. zero store-interval = scheduled-fixed-delay rejection).
        if (p.storeInterval() == null || p.storeInterval().isZero() || p.storeInterval().isNegative()) {
            violations.add(label + ": preloader.store-interval must be positive"
                    + " (got " + p.storeInterval() + ")");
        }
        if (p.storeInitialDelay() == null
                || p.storeInitialDelay().isZero()
                || p.storeInitialDelay().isNegative()) {
            violations.add(label + ": preloader.store-initial-delay must be positive"
                    + " (got " + p.storeInitialDelay() + ")");
        }
        if (p.prefetchTimeout() == null
                || p.prefetchTimeout().isZero()
                || p.prefetchTimeout().isNegative()) {
            violations.add(label + ": preloader.prefetch-timeout must be positive"
                    + " (got " + p.prefetchTimeout() + ")");
        }

        // E26: prefetch-concurrency must be >= 1.
        if (p.prefetchConcurrency() < 1) {
            violations.add(label + ": preloader.prefetch-concurrency must be >= 1"
                    + " (got " + p.prefetchConcurrency() + ")");
        }

        // E27 (directory creatability): NOT enforced here. The validator does
        // not touch the filesystem — that would side-effect from a unit-tested
        // class. The check lives in PreloaderCoordinator.start(): if the
        // directory cannot be created or is unwritable, that bean's start()
        // throws and Spring fails context refresh. Same "fail at startup, not
        // on first store" guarantee, just one layer up.

        // E28: max-stored-keys, when set, must be >= 1. null = unbounded.
        if (p.maxStoredKeys() != null && p.maxStoredKeys() < 1) {
            violations.add(label + ": preloader.max-stored-keys must be >= 1"
                    + " when set (got " + p.maxStoredKeys()
                    + "); leave unset for unbounded");
        }
    }

    private static void validateReconciliation(String label,
                                                CacheProperties.CacheSpec spec,
                                                List<String> violations) {
        CacheProperties.Reconciliation r = spec.reconciliation();

        // E29: interval must be positive.
        if (r.interval() == null || r.interval().isZero() || r.interval().isNegative()) {
            violations.add(label + ": reconciliation.interval must be positive"
                    + " (got " + r.interval() + "); zero would schedule the cycle at"
                    + " zero delay (continuous loop, hammering Redis with GETs)");
        }

        // E30: miss-tolerance must be >= 0.
        if (r.missTolerance() < 0) {
            violations.add(label + ": reconciliation.miss-tolerance must be >= 0"
                    + " (got " + r.missTolerance() + "); a negative tolerance inverts"
                    + " the comparison and would declare a miss on every cycle");
        }

        // E31: not allowed on LOCAL_ONLY. NEAR_CACHE and DISTRIBUTED_ONLY both
        // participate; LOCAL_ONLY has no cross-node coherence to reconcile.
        if (spec.tier() == CacheProperties.Tier.LOCAL_ONLY) {
            violations.add(label + ": reconciliation.enabled=true is not allowed on"
                    + " tier=LOCAL_ONLY. Reconciliation recovers cross-node missed"
                    + " invalidations, and LOCAL_ONLY caches have no peers to fall"
                    + " behind — the <cache>:seq counter would never advance and the"
                    + " cycle would do nothing useful while still consuming a Redis"
                    + " GET per interval. Either disable reconciliation or change the"
                    + " tier to NEAR_CACHE or DISTRIBUTED_ONLY.");
        }
    }

    private static void validateShardedPubsub(CacheProperties properties,
                                              List<String> violations) {
        // E23: sharded pub/sub is a Redis Cluster–only capability. Allowing
        // it in SINGLE or SENTINEL would silently degrade — Redisson would
        // try SPUBLISH on a non-cluster server and surface a confusing
        // protocol error per publish. Fail at startup instead.
        CacheProperties.Mode mode = properties.server() == null
                ? null : properties.server().mode();
        if (mode != CacheProperties.Mode.CLUSTER) {
            violations.add("invalidation.sharded-pubsub=true requires"
                    + " cache.server.mode=CLUSTER (got " + mode + ");"
                    + " sharded pub/sub (SPUBLISH/SSUBSCRIBE) is a Redis"
                    + " Cluster–only capability and is not available in"
                    + " single-server or Sentinel deployments");
        }
    }

    private static void validateStartupProbe(CacheProperties.StartupProbe probe,
                                             List<String> violations) {
        // E20: timeout must be positive (Duration.ZERO would surface as an
        // immediate TimeoutException on every attempt, which is indistinguishable
        // from "Redis is unreachable" — i.e. always fails to boot).
        if (probe.timeout() == null || probe.timeout().isZero() || probe.timeout().isNegative()) {
            violations.add("startup-probe.timeout must be positive (got " + probe.timeout() + ")");
        }
        // E21: retries must be >= 0 (negative is a configuration mistake; 0 is
        // valid — single attempt, no retries).
        if (probe.retries() < 0) {
            violations.add("startup-probe.retries must be >= 0 (got "
                    + probe.retries() + ")");
        }
        // E22: retry-delay must be >= 0 (zero means "retry immediately"; negative
        // is nonsense).
        if (probe.retryDelay() == null || probe.retryDelay().isNegative()) {
            violations.add("startup-probe.retry-delay must be >= 0 (got "
                    + probe.retryDelay() + ")");
        }
    }

    private static void validateSpec(String label, CacheProperties.CacheSpec spec,
                                     CacheProperties properties, List<String> violations) {
        // E1
        if (spec.ttl() != null && (spec.ttl().isZero() || spec.ttl().isNegative())) {
            violations.add(label + ": ttl must be positive (got " + spec.ttl() + ")");
        }

        // E2
        if (spec.lockWait() != null && spec.lockLease() != null && !spec.lockWait().isZero()) {
            if (spec.lockLease().compareTo(spec.lockWait()) <= 0) {
                violations.add(label + ": lock-lease (" + spec.lockLease()
                        + ") must be greater than lock-wait (" + spec.lockWait()
                        + "); with lease <= wait the lock expires before the"
                        + " wait timeout, making safe acquisition impossible");
            }
        }

        // E3
        if (spec.tier() == CacheProperties.Tier.LOCAL_ONLY
                && spec.codec() == CacheProperties.Codec.KRYO) {
            violations.add(label + ": codec=KRYO has no effect on tier=LOCAL_ONLY (no Redis"
                    + " layer); use codec=JSON or switch to a tier that uses Redis."
                    + " Remove this cache's classes from cache.kryo.registered-classes"
                    + " if it is the only KRYO cache.");
        }

        // E4 (only checked for tiers that actually have an L2)
        if (spec.tier() != CacheProperties.Tier.LOCAL_ONLY
                && spec.codec() == CacheProperties.Codec.KRYO) {
            List<String> registered = properties.kryo() == null
                    ? List.of()
                    : properties.kryo().registeredClasses();
            if (registered == null || registered.isEmpty()) {
                violations.add(label + ": codec=KRYO requires cache.kryo.registered-classes"
                        + " to be non-empty; Kryo without class registration accepts"
                        + " arbitrary class names from the payload (same threat as Jackson's"
                        + " permissive default typing). Declare every class the cache will"
                        + " store, in a stable order.");
            }
        }

        // E13: max-concurrent-loaders, when set, must be > 0.
        if (spec.maxConcurrentLoaders() != null && spec.maxConcurrentLoaders() <= 0) {
            violations.add(label + ": max-concurrent-loaders must be > 0 when set"
                    + " (got " + spec.maxConcurrentLoaders() + "); leave unset for unlimited");
        }

        // E14: loader-acquire-timeout must be > 0 when the gate is actually
        // in use. With max-concurrent-loaders unset, the timeout is not
        // consulted, so a zero default cascaded from lockWait=0 is harmless.
        if (spec.maxConcurrentLoaders() != null
                && spec.loaderAcquireTimeout() != null
                && (spec.loaderAcquireTimeout().isZero()
                    || spec.loaderAcquireTimeout().isNegative())) {
            violations.add(label + ": loader-acquire-timeout must be positive when"
                    + " max-concurrent-loaders is set (got " + spec.loaderAcquireTimeout() + ")");
        }

        // E15: ttl-jitter-ratio must be in [0.0, 0.5].
        double ratio = spec.ttlJitterRatio();
        if (ratio < 0.0 || ratio > 0.5) {
            violations.add(label + ": ttl-jitter-ratio must be in [0.0, 0.5]"
                    + " (got " + ratio + "); 0.0 disables jitter, 0.5 is the upper"
                    + " bound — beyond that, expiries can fall below half the"
                    + " configured TTL and the value stops behaving like jitter");
        }

        // E16: ttl-jitter-ratio > 0 only meaningful on tiers with an L1 layer.
        if (ratio > 0.0 && spec.tier() == CacheProperties.Tier.DISTRIBUTED_ONLY) {
            violations.add(label + ": ttl-jitter-ratio > 0 has no effect on"
                    + " tier=DISTRIBUTED_ONLY. Jitter applies only to the local"
                    + " Caffeine (L1) layer; DISTRIBUTED_ONLY has no L1, so"
                    + " jittering would have nothing to apply to. Either set"
                    + " ttl-jitter-ratio to 0.0 or change the tier to NEAR_CACHE"
                    + " or LOCAL_ONLY.");
        }

        // E17: max-idle, when set, must be positive.
        if (spec.maxIdle() != null
                && (spec.maxIdle().isZero() || spec.maxIdle().isNegative())) {
            violations.add(label + ": max-idle must be positive when set"
                    + " (got " + spec.maxIdle() + "); leave unset to disable"
                    + " idle eviction");
        }

        // E18: max-idle only meaningful on tiers with an L1 layer.
        if (spec.maxIdle() != null
                && spec.tier() == CacheProperties.Tier.DISTRIBUTED_ONLY) {
            violations.add(label + ": max-idle has no effect on"
                    + " tier=DISTRIBUTED_ONLY. Idle eviction acts on the local"
                    + " Caffeine (L1) layer; DISTRIBUTED_ONLY has no L1, so"
                    + " there is nothing for max-idle to evict from. Either"
                    + " unset max-idle or change the tier to NEAR_CACHE or"
                    + " LOCAL_ONLY.");
        }

        // E19: max-idle > ttl is a no-op (TTL fires first on a cold entry)
        // and almost certainly misconfiguration. Fail loudly at startup.
        if (spec.maxIdle() != null && spec.ttl() != null
                && !spec.maxIdle().isZero() && !spec.maxIdle().isNegative()
                && !spec.ttl().isZero() && !spec.ttl().isNegative()
                && spec.maxIdle().compareTo(spec.ttl()) > 0) {
            violations.add(label + ": max-idle (" + spec.maxIdle()
                    + ") must be <= ttl (" + spec.ttl() + "); a cold entry"
                    + " expires on ttl before max-idle could ever fire, so"
                    + " the field would be a no-op as configured");
        }

        // Preloader (only validated when enabled — disabled-with-bogus-values
        // mirrors the startup-probe philosophy: no gating on unrelated typos).
        if (spec.preloader() != null && spec.preloader().enabled()) {
            validatePreloader(label, spec, violations);
        }

        // Reconciliation (only validated when enabled — same philosophy).
        if (spec.reconciliation() != null && spec.reconciliation().enabled()) {
            validateReconciliation(label, spec, violations);
        }

        // W1
        if (spec.tier() == CacheProperties.Tier.LOCAL_ONLY && spec.circuitBreaker() != null) {
            log.warn("{}: circuit-breaker overrides have no effect on tier=LOCAL_ONLY"
                    + " (the breaker guards L2 operations; LOCAL_ONLY has no L2)", label);
        }

        // Per-cache CB overlay
        if (spec.circuitBreaker() != null) {
            validateCircuitBreaker(label, spec.circuitBreaker(), violations);
        }
    }

    private static void validateCircuitBreaker(String label, CacheProperties.CircuitBreaker cb,
                                               List<String> violations) {
        // E5
        if (cb.failureRateThreshold() != null) {
            float v = cb.failureRateThreshold();
            if (v <= 0 || v > 100) {
                violations.add(label + ": circuit-breaker.failure-rate-threshold must be in"
                        + " (0, 100] (got " + v + ")");
            }
        }

        // E6
        if (cb.slowCallRateThreshold() != null) {
            float v = cb.slowCallRateThreshold();
            if (v <= 0 || v > 100) {
                violations.add(label + ": circuit-breaker.slow-call-rate-threshold must be in"
                        + " (0, 100] (got " + v + ")");
            }
        }

        // E7
        if (cb.slidingWindowSize() != null && cb.slidingWindowSize() < 1) {
            violations.add(label + ": circuit-breaker.sliding-window-size must be >= 1"
                    + " (got " + cb.slidingWindowSize() + ")");
        }

        // E8
        if (cb.minimumNumberOfCalls() != null && cb.minimumNumberOfCalls() < 1) {
            violations.add(label + ": circuit-breaker.minimum-number-of-calls must be >= 1"
                    + " (got " + cb.minimumNumberOfCalls() + ")");
        }

        // E9 — only when both are present in the same block
        if (cb.minimumNumberOfCalls() != null && cb.slidingWindowSize() != null
                && cb.minimumNumberOfCalls() > cb.slidingWindowSize()) {
            violations.add(label + ": circuit-breaker.minimum-number-of-calls ("
                    + cb.minimumNumberOfCalls() + ") must be <= sliding-window-size ("
                    + cb.slidingWindowSize() + "); with minimum > window the failure rate"
                    + " is never evaluated and the breaker can never open."
                    + " (Note: this constraint is only checked when both fields appear"
                    + " in the same configuration block; a merged default+overlay"
                    + " combination is not detected.)");
        }

        // E10
        if (cb.slowCallDurationThreshold() != null
                && (cb.slowCallDurationThreshold().isZero()
                    || cb.slowCallDurationThreshold().isNegative())) {
            violations.add(label + ": circuit-breaker.slow-call-duration-threshold must be"
                    + " positive (got " + cb.slowCallDurationThreshold() + ")");
        }

        // E11
        if (cb.waitDurationInOpenState() != null
                && (cb.waitDurationInOpenState().isZero()
                    || cb.waitDurationInOpenState().isNegative())) {
            violations.add(label + ": circuit-breaker.wait-duration-in-open-state must be"
                    + " positive (got " + cb.waitDurationInOpenState() + ")");
        }

        // E12
        if (cb.permittedNumberOfCallsInHalfOpenState() != null
                && cb.permittedNumberOfCallsInHalfOpenState() < 1) {
            violations.add(label
                    + ": circuit-breaker.permitted-number-of-calls-in-half-open-state"
                    + " must be >= 1 (got " + cb.permittedNumberOfCallsInHalfOpenState() + ")");
        }
    }
}
