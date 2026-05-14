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

    /**
     * Reactive {@code @CacheEvict(allEntries=true)} on a Mono-returning
     * method. Drives {@code NearCache.clear()} from inside a Reactor
     * pipeline; under load Spring's reactive cache adapter completes the
     * continuation on a Redisson Netty event-loop thread. Added in 1.0.1
     * so the soak exercises the reactive-evict path — without this, the
     * soak would never surface event-loop reachability on the clear /
     * generation-refresh code paths that 1.0.1 fixed.
     */
    @CacheEvict(value = "users", allEntries = true)
    public Mono<Void> clearUsers() {
        return Mono.empty();
    }

    public long loaderInvocations() {
        return loaderInvocations.get();
    }
}
