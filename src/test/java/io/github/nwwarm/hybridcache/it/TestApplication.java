package io.github.nwwarm.hybridcache.it;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test-only Spring Boot application used by the end-to-end suite.
 *
 * <p>Exists so {@link org.springframework.cache.annotation.Cacheable @Cacheable}
 * goes through Spring's real cache wiring (interceptor → resolver → manager)
 * rather than direct {@code NearCache} construction in test helpers. Wiring
 * bugs that the unit-flavored suite cannot see (e.g., the auto-config not
 * being discovered, or the library's {@code CacheResolver} being ignored)
 * surface here.
 *
 * <p>Service methods cover the key types exercised by the parameterized
 * cross-node invalidation test:
 *
 * <ul>
 *   <li>{@code findById(Long)} — single primitive-wrapper key (the default
 *       Spring cache key for a one-arg primitive-wrapper method).</li>
 *   <li>{@code findByName(String)} — single string key.</li>
 *   <li>{@code findByUuid(UUID)} — single UUID key (toString() round-trips).</li>
 *   <li>{@code findBySimpleKey(Long)} — same logical input as findById, but
 *       wrapped in a {@code SimpleKey} via SpEL so the single-component
 *       SimpleKey path is covered.</li>
 *   <li>{@code findByRegion(Long, String)} — two-arg method; Spring wraps the
 *       arguments in a multi-component {@code SimpleKey} by default.</li>
 * </ul>
 */
@SpringBootApplication
@EnableCaching
public class TestApplication {

    public static void main(String[] args) {
        SpringApplication.run(TestApplication.class, args);
    }

    public record Product(Long id, String name) {}

    @Service
    public static class ProductService {

        private final AtomicLong loaderCallCount = new AtomicLong();

        @Cacheable("products")
        public Product findById(Long id) {
            loaderCallCount.incrementAndGet();
            return new Product(id, "name-" + id);
        }

        @Cacheable("products-by-name")
        public Product findByName(String name) {
            loaderCallCount.incrementAndGet();
            return new Product((long) name.hashCode(), name);
        }

        @Cacheable("products-by-uuid")
        public Product findByUuid(UUID id) {
            loaderCallCount.incrementAndGet();
            return new Product((long) id.hashCode(), id.toString());
        }

        @Cacheable(cacheNames = "products-simple",
                key = "new org.springframework.cache.interceptor.SimpleKey(#id)")
        public Product findBySimpleKey(Long id) {
            loaderCallCount.incrementAndGet();
            return new Product(id, "simple-" + id);
        }

        @Cacheable("products-by-region")
        public Product findByRegion(Long id, String region) {
            loaderCallCount.incrementAndGet();
            return new Product(id, region + "-" + id);
        }

        @CacheEvict("products")
        public void invalidate(Long id) {
        }

        @CacheEvict("products-by-name")
        public void invalidateByName(String name) {
        }

        @CacheEvict("products-by-uuid")
        public void invalidateByUuid(UUID id) {
        }

        @CacheEvict(cacheNames = "products-simple",
                key = "new org.springframework.cache.interceptor.SimpleKey(#id)")
        public void invalidateBySimpleKey(Long id) {
        }

        @CacheEvict("products-by-region")
        public void invalidateByRegion(Long id, String region) {
        }

        @CacheEvict(cacheNames = "products", allEntries = true)
        public void clearAll() {
        }

        public long loaderCallCount() {
            return loaderCallCount.get();
        }
    }
}
