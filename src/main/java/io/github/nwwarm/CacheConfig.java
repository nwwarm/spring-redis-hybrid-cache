package io.github.nwwarm;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.client.RedisException;
import org.redisson.codec.JsonJacksonCodec;
import org.redisson.codec.Kryo5Codec;
import org.redisson.config.Config;
import org.redisson.config.ReadMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * Primary Spring configuration. Wires Redisson, the tier-aware
 * {@link HybridCacheManager}, the per-cache circuit-breaker registry, and the
 * invalidation dispatcher.
 *
 * <p>All beans are exposed as {@link ConditionalOnMissingBean} so applications
 * can override any single piece (e.g., supply a custom {@link RedissonClient}
 * with a different codec for JPA entities) without forking the rest.
 */
@Configuration(proxyBeanMethods = false)
@EnableCaching
@EnableConfigurationProperties(CacheProperties.class)
public class CacheConfig {

    private static final Logger log = LoggerFactory.getLogger(CacheConfig.class);

    @Bean(destroyMethod = "shutdown")
    @ConditionalOnMissingBean
    public RedissonClient redissonClient(CacheProperties properties) {
        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .activateDefaultTyping(
                        buildPolymorphicTypeValidator(properties),
                        ObjectMapper.DefaultTyping.NON_FINAL,
                        JsonTypeInfo.As.PROPERTY);

        Config config = new Config();
        config.setCodec(new JsonJacksonCodec(mapper));
        applyTopology(config, properties.server());
        config.setLazyInitialization(true);
        return Redisson.create(config);
    }

    /**
     * Applies the topology-specific Redisson configuration based on
     * {@link CacheProperties.Server#mode()}. Required-field validation has
     * already happened in the {@link CacheProperties.Server} compact
     * constructor, so this method only translates configuration into Redisson
     * builder calls.
     */
    private static void applyTopology(Config config, CacheProperties.Server server) {
        String password = server.password() == null ? null : server.password().trim();
        switch (server.mode()) {
            case SINGLE -> config.useSingleServer()
                    .setAddress(server.address())
                    .setPassword(password);
            case CLUSTER -> {
                // ReadMode.MASTER preserves read-your-writes against the
                // invalidation pipeline. Redisson's default is SLAVE, which
                // routes reads to async-replicating replicas and breaks the
                // <10ms cross-node coherence the library commits to: a node
                // whose L1 was just invalidated would read a stale value
                // from a not-yet-replicated replica. Applications wanting
                // higher read throughput in exchange for eventual-
                // consistency reads can override the RedissonClient bean.
                var cluster = config.useClusterServers()
                        .setReadMode(ReadMode.MASTER)
                        .setPassword(password)
                        .setScanInterval(server.scanInterval() != null
                                ? server.scanInterval()
                                : 2000);
                server.addresses().forEach(cluster::addNodeAddress);
            }
            case SENTINEL -> {
                // See CLUSTER branch comment — same rationale for ReadMode.MASTER.
                var sentinel = config.useSentinelServers()
                        .setReadMode(ReadMode.MASTER)
                        .setMasterName(server.masterName())
                        .setPassword(password);
                server.addresses().forEach(sentinel::addSentinelAddress);
            }
        }
    }

    /**
     * Builds the polymorphic type validator used by the default codec.
     *
     * <p>Fails fast at startup if {@code cache.allowed-packages} is empty.
     * Jackson's default-typing-with-permissive-validator pattern has been
     * the foundation of 50+ deserialization CVEs (see CVE-2017-7525); a
     * library that ships with this configuration as a default would invite
     * RCE in any deployment that forgot to set the property.
     *
     * <p>Each entry must end with {@code '.'} (package prefix) or be a
     * fully-qualified class name. {@code com.example} alone is rejected:
     * Jackson's {@code allowIfBaseType} matches via {@code startsWith}, so
     * {@code com.example} would also accept {@code com.examplerogue.evil}.
     * The trailing dot enforces the package boundary.
     *
     * <p>{@code java.util.}, {@code java.time.}, and {@code java.lang.} are
     * always added regardless of user configuration; collections, dates,
     * and primitive wrappers are part of every realistic value graph.
     */
    private static BasicPolymorphicTypeValidator buildPolymorphicTypeValidator(CacheProperties properties) {
        if (properties.allowedPackages() == null || properties.allowedPackages().isEmpty()) {
            throw new IllegalStateException(
                    "cache.allowed-packages is not configured. The library will not start with "
                            + "a permissive Jackson polymorphic type validator: that pattern has "
                            + "been the basis of 50+ Jackson deserialization CVEs (canonical "
                            + "example: CVE-2017-7525) and is unsafe to deploy. Choose one:\n"
                            + "  (a) Set cache.allowed-packages to a list of your domain package "
                            + "prefixes — each entry must end with '.' (e.g., 'com.example.domain.').\n"
                            + "  (b) Switch to KRYO codec (cache.default-spec.codec=KRYO and "
                            + "cache.kryo.registered-classes=[...]).\n"
                            + "  (c) Provide a custom RedissonClient bean with a different codec.");
        }

        BasicPolymorphicTypeValidator.Builder builder = BasicPolymorphicTypeValidator.builder();
        for (String entry : properties.allowedPackages()) {
            validateAllowedPackagesEntry(entry);
            builder.allowIfBaseType(entry);
        }
        // Always allow JDK collection, date, and primitive-wrapper types — values
        // typically contain these even when the top-level type is in user packages.
        builder.allowIfBaseType("java.util.")
               .allowIfBaseType("java.time.")
               .allowIfBaseType("java.lang.");
        log.info("Polymorphic type validator restricted to packages: {} (plus java.util., java.time., java.lang.)",
                properties.allowedPackages());
        return builder.build();
    }

    /**
     * An entry is valid if it ends with {@code '.'} (package prefix) or its
     * last dotted segment looks like a class name (starts with an uppercase
     * letter). Anything else — typically a bare package without a trailing
     * dot — is rejected with the boundary-collision footgun spelled out.
     */
    private static void validateAllowedPackagesEntry(String entry) {
        if (entry == null || entry.isBlank()) {
            throw new IllegalArgumentException(
                    "cache.allowed-packages contains a blank entry");
        }
        if (entry.endsWith(".")) return;

        int lastDot = entry.lastIndexOf('.');
        String lastSegment = lastDot < 0 ? entry : entry.substring(lastDot + 1);
        if (!lastSegment.isEmpty() && Character.isUpperCase(lastSegment.charAt(0))) {
            // Looks like a fully-qualified class name (e.g., com.example.User).
            return;
        }

        throw new IllegalArgumentException(
                "cache.allowed-packages entry '" + entry + "' is invalid. Jackson matches via "
                        + "startsWith(), so '" + entry + "' would also accept '" + entry
                        + "rogue' or '" + entry + "evil'. Use '" + entry + ".' to enforce a "
                        + "package boundary, or supply a fully-qualified class name.");
    }

    /**
     * The library's circuit-breaker registry. Default config is built from
     * {@code cache.resilience.circuit-breaker.*} (with library fallbacks for
     * any missing field) and applies to every per-cache breaker unless that
     * cache supplies its own overlay under
     * {@code cache.caches.<name>.circuit-breaker.*}.
     *
     * <p>{@code recordExceptions} is hardcoded to the universe of "L2 is
     * unhealthy" — see README. Not a per-deployment knob.
     */
    @Bean
    @ConditionalOnMissingBean(name = "redisCacheCircuitBreakerRegistry")
    public CircuitBreakerRegistry redisCacheCircuitBreakerRegistry(CacheProperties properties) {
        CacheProperties.CircuitBreaker overlay = properties.resilience() == null
                ? null
                : properties.resilience().circuitBreaker();
        CircuitBreakerConfig defaultConfig = buildDefaultBreakerConfig(overlay);
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(defaultConfig);

        // Pre-register breakers for every explicitly-configured cache. This
        // validates per-cache overrides at startup (Resilience4j's builder
        // throws on out-of-range values) and exposes metrics before first
        // traffic. Caches that fall through to default-spec are still
        // resolved lazily on first @Cacheable access.
        CircuitBreakerFactory factory = new CircuitBreakerFactory(registry);
        for (Map.Entry<String, CacheProperties.CacheSpec> entry : properties.caches().entrySet()) {
            String name = entry.getKey();
            CacheProperties.CacheSpec spec = entry.getValue();
            if (spec == null) continue;
            try {
                factory.resolve(name, spec.circuitBreaker());
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Invalid circuit-breaker configuration for cache '" + name
                                + "' (cache.caches." + name + ".circuit-breaker.*): " + e.getMessage(), e);
            }
        }
        return registry;
    }

    private static CircuitBreakerConfig buildDefaultBreakerConfig(CacheProperties.CircuitBreaker overlay) {
        CircuitBreakerConfig.Builder builder = CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(20)
                .minimumNumberOfCalls(10)
                .failureRateThreshold(50.0f)
                .slowCallDurationThreshold(Duration.ofMillis(500))
                .slowCallRateThreshold(80.0f)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(3)
                .recordExceptions(
                        RedisException.class,
                        TimeoutException.class,
                        java.net.ConnectException.class);

        if (overlay == null) return builder.build();

        if (overlay.slidingWindowSize() != null) builder.slidingWindowSize(overlay.slidingWindowSize());
        if (overlay.minimumNumberOfCalls() != null) builder.minimumNumberOfCalls(overlay.minimumNumberOfCalls());
        if (overlay.failureRateThreshold() != null) builder.failureRateThreshold(overlay.failureRateThreshold());
        if (overlay.slowCallDurationThreshold() != null) builder.slowCallDurationThreshold(overlay.slowCallDurationThreshold());
        if (overlay.slowCallRateThreshold() != null) builder.slowCallRateThreshold(overlay.slowCallRateThreshold());
        if (overlay.waitDurationInOpenState() != null) builder.waitDurationInOpenState(overlay.waitDurationInOpenState());
        if (overlay.permittedNumberOfCallsInHalfOpenState() != null)
            builder.permittedNumberOfCallsInHalfOpenState(overlay.permittedNumberOfCallsInHalfOpenState());

        return builder.build();
    }

    @Bean
    @ConditionalOnMissingBean
    public TaggedCircuitBreakerMetrics circuitBreakerMetrics(
            CircuitBreakerRegistry registry,
            MeterRegistry meterRegistry) {
        TaggedCircuitBreakerMetrics metrics =
                TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(registry);
        metrics.bindTo(meterRegistry);
        return metrics;
    }

    @Bean
    @ConditionalOnMissingBean
    InvalidationDispatcher invalidationDispatcher(RedissonClient redisson,
                                                  CacheProperties properties) {
        String nodeId = properties.nodeId() != null && !properties.nodeId().isBlank()
                ? properties.nodeId()
                : UUID.randomUUID().toString();
        return new InvalidationDispatcher(redisson, nodeId);
    }

    /**
     * Resolves per-cache codec overrides. KRYO is locked down — class
     * registration is mandatory because Kryo with no registration accepts
     * arbitrary class names from the payload, the same threat model as
     * Jackson's permissive default typing.
     *
     * <p>If any cache (default-spec or per-name) is configured to use KRYO,
     * {@link CacheProperties.Kryo#registeredClasses()} must be non-empty.
     * Each class name is loaded via {@code Class.forName} at startup — a
     * missing class fails the context with the offending name. The
     * resulting {@link Kryo5Codec} runs with {@code registrationRequired=true}
     * (set by Redisson when classes are passed to its constructor); a
     * payload referencing an unregistered class will fail to deserialize.
     */
    @Bean
    @ConditionalOnMissingBean
    CodecResolver codecResolver(CacheProperties properties) {
        if (!anyCacheUsesKryo(properties)) {
            return new CodecResolver(null);
        }

        List<String> registered = properties.kryo() == null
                ? List.of()
                : properties.kryo().registeredClasses();
        if (registered == null || registered.isEmpty()) {
            throw new IllegalStateException(
                    "Cache configured with codec=KRYO but cache.kryo.registered-classes is empty. "
                            + "Kryo without registration accepts arbitrary class names from the "
                            + "payload — same threat model as Jackson's permissive default "
                            + "typing. Declare every class the cache will store, in a stable "
                            + "order (changing the order changes Kryo registration ids and "
                            + "breaks deserialization of payloads written by older deployments).");
        }
        // Pre-load each class so a typo or stale entry fails the context here,
        // not on the first cache miss in production.
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null) cl = CacheConfig.class.getClassLoader();
        for (String className : registered) {
            try {
                Class.forName(className, false, cl);
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(
                        "cache.kryo.registered-classes entry '" + className + "' could not be "
                                + "loaded by the application classloader: " + e.getMessage(), e);
            }
        }
        // LinkedHashSet preserves the configured order so Kryo registration ids
        // are stable across deployments — changing the order of an existing
        // entry would break deserialization of payloads written by older nodes.
        // The boolean is Redisson's `useReferences` (circular-graph support);
        // we keep it false because cache values are domain DTOs, not graphs.
        return new CodecResolver(new Kryo5Codec(cl, new LinkedHashSet<>(registered), false));
    }

    private static boolean anyCacheUsesKryo(CacheProperties properties) {
        if (properties.defaultSpec() != null
                && properties.defaultSpec().codec() == CacheProperties.Codec.KRYO) {
            return true;
        }
        if (properties.caches() == null) return false;
        for (CacheProperties.CacheSpec spec : properties.caches().values()) {
            if (spec != null && spec.codec() == CacheProperties.Codec.KRYO) return true;
        }
        return false;
    }

    @Bean
    @ConditionalOnMissingBean(CacheManager.class)
    public HybridCacheManager cacheManager(CacheProperties properties,
                                           RedissonClient redisson,
                                           CircuitBreakerRegistry circuitBreakerRegistry,
                                           InvalidationDispatcher invalidationDispatcher,
                                           MeterRegistry meterRegistry,
                                           CodecResolver codecResolver) {
        return new HybridCacheManager(
                properties,
                redisson,
                circuitBreakerRegistry,
                invalidationDispatcher,
                meterRegistry,
                codecResolver);
    }

    /**
     * Spring Boot Actuator integration. Only registered when
     * {@link HealthIndicator} is on the classpath, so applications that
     * exclude {@code spring-boot-starter-actuator} aren't forced to add it.
     */
    @Bean
    @ConditionalOnClass(HealthIndicator.class)
    @ConditionalOnMissingBean(name = "hybridCacheHealthIndicator")
    public HybridCacheHealthIndicator hybridCacheHealthIndicator(
            RedissonClient redisson,
            CircuitBreakerRegistry circuitBreakerRegistry,
            HybridCacheManager cacheManager,
            CacheProperties properties) {
        return new HybridCacheHealthIndicator(
                redisson,
                circuitBreakerRegistry,
                cacheManager,
                properties.health().pingTimeout());
    }
}
