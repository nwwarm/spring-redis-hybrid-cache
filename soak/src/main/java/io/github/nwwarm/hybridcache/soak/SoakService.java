package io.github.nwwarm.hybridcache.soak;

import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Loaders for the soak smoke app. Three flavours:
 *
 * <ul>
 *   <li>{@link #findProduct(long)} — plain sync NEAR_CACHE read</li>
 *   <li>{@link #findUser(long)} — async (Mono) NEAR_CACHE read; exercises
 *       {@code Cache.retrieve(...)} and the loader hop to refreshExecutor</li>
 *   <li>{@link #findSession(String)} — DISTRIBUTED_ONLY shared state</li>
 * </ul>
 */
@Service
public class SoakService {

    private final AtomicLong loaderInvocations = new AtomicLong();

    @Cacheable("products")
    public String findProduct(long id) {
        loaderInvocations.incrementAndGet();
        return "product-" + id + "-" + UUID.randomUUID();
    }

    @Cacheable("users")
    public Mono<String> findUser(long id) {
        loaderInvocations.incrementAndGet();
        return Mono.just("user-" + id + "-" + UUID.randomUUID());
    }

    @Cacheable("sessions")
    public String findSession(String token) {
        loaderInvocations.incrementAndGet();
        return "session-" + token;
    }

    @CacheEvict(value = "products", allEntries = true)
    public void clearProducts() {
        // no-op — exercises the @CacheEvict path
    }

    public long loaderInvocations() {
        return loaderInvocations.get();
    }
}
