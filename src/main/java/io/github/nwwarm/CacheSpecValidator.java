package io.github.nwwarm;

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

        if (!violations.isEmpty()) {
            throw new IllegalArgumentException(
                    violations.size() + " cache configuration violation(s):\n  - "
                            + String.join("\n  - ", violations));
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
