package io.github.nwwarm;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.client.RedisException;
import org.redisson.codec.JsonJacksonCodec;
import org.redisson.config.Config;
import org.redisson.config.ReadMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * Primary Spring configuration. Wires Redisson, the tier-aware
 * {@link HybridCacheManager}, the circuit breaker, and the invalidation
 * dispatcher.
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
     * <p>If {@code cache.allowed-packages} is configured, only base types whose
     * fully-qualified name starts with one of those prefixes can be deserialized.
     * This is the recommended production configuration — narrow to the application's
     * domain and DTO packages plus {@code java.util} / {@code java.time} for
     * collections and dates.
     *
     * <p>If unset, falls back to a permissive validator that accepts any
     * {@link Object}-derived type. A startup warning is logged. The permissive
     * default exists for backwards compatibility and ease of first use; production
     * deployments should always set {@code cache.allowed-packages} explicitly to
     * limit the deserialization attack surface.
     */
    private static BasicPolymorphicTypeValidator buildPolymorphicTypeValidator(CacheProperties properties) {
        BasicPolymorphicTypeValidator.Builder builder = BasicPolymorphicTypeValidator.builder();

        if (properties.allowedPackages() == null || properties.allowedPackages().isEmpty()) {
            log.warn("cache.allowed-packages is not configured. Using permissive polymorphic type validator " +
                    "(allows any Object-derived type for deserialization). Configure cache.allowed-packages " +
                    "to narrow the deserialization attack surface for production deployments.");
            return builder.allowIfBaseType(Object.class).build();
        }

        for (String prefix : properties.allowedPackages()) {
            builder.allowIfBaseType(prefix);
        }
        // Always allow JDK collection and date types — values typically contain these
        // even when the top-level type is in user packages.
        builder.allowIfBaseType("java.util.")
               .allowIfBaseType("java.time.")
               .allowIfBaseType("java.lang.");
        log.info("Polymorphic type validator restricted to packages: {} (plus java.util., java.time., java.lang.)",
                properties.allowedPackages());
        return builder.build();
    }

    @Bean
    @ConditionalOnMissingBean(name = "redisCacheCircuitBreakerRegistry")
    public CircuitBreakerRegistry redisCacheCircuitBreakerRegistry() {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
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
                        java.net.ConnectException.class)
                .build();
        return CircuitBreakerRegistry.of(config);
    }

    @Bean
    @ConditionalOnMissingBean(name = "redisCacheCircuitBreaker")
    public CircuitBreaker redisCacheCircuitBreaker(CircuitBreakerRegistry registry) {
        return registry.circuitBreaker("redis-cache");
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

    @Bean
    @ConditionalOnMissingBean(CacheManager.class)
    public HybridCacheManager cacheManager(CacheProperties properties,
                                           RedissonClient redisson,
                                           CircuitBreaker redisCacheCircuitBreaker,
                                           InvalidationDispatcher invalidationDispatcher,
                                           MeterRegistry meterRegistry) {
        return new HybridCacheManager(
                properties,
                redisson,
                redisCacheCircuitBreaker,
                invalidationDispatcher,
                meterRegistry);
    }
}
