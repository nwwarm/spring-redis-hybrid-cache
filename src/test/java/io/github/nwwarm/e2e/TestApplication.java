package io.github.nwwarm.e2e;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.stereotype.Service;

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

        @CacheEvict("products")
        public void invalidate(Long id) {
            // Eviction handled by Spring's cache abstraction.
        }

        @CacheEvict(cacheNames = "products", allEntries = true)
        public void clearAll() {
            // Eviction handled by Spring's cache abstraction.
        }

        public long loaderCallCount() {
            return loaderCallCount.get();
        }
    }
}
