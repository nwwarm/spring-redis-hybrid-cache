package io.github.nwwarm.hybridcache.core;

import io.github.nwwarm.hybridcache.testfixtures.RedisTestBase;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.springframework.cache.Cache;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pin {@link NearCache}'s synchronous loader-failure translation: a loader
 * that throws a non-{@link LoaderRejectedException} is wrapped in
 * {@code NearCache.LoaderException} internally and surfaces to the caller
 * as {@link Cache.ValueRetrievalException}, with the original cause
 * unwrapped to its bottom layer.
 *
 * <p>Without this test the private nested {@code LoaderException} type
 * shows as 0% coverage even though the surrounding exception-translation
 * code is exercised by other ITs. The contract is small but load-bearing
 * for adopters that pattern-match on {@code ValueRetrievalException} with
 * the original cause.
 */
class NearCacheLoaderExceptionIT extends RedisTestBase {

    private RedissonClient redisson;
    private NearCache cache;
    private final String name = "near-loader-fail-" + System.nanoTime();

    @BeforeEach
    void setUp() {
        redisson = newRedisson();
        cache = newNearCache(name, redisson, defaultBreaker(), "node-cov");
    }

    @AfterEach
    void tearDown() {
        if (cache != null) cache.shutdown();
        if (redisson != null) redisson.shutdown();
    }

    @Test
    void getWithLoader_loaderThrows_surfaceAsValueRetrievalException_withRootCause() {
        assertThatThrownBy(() -> cache.get("k", () -> {
            throw new IllegalStateException("loader-blew-up");
        }))
                .isInstanceOf(Cache.ValueRetrievalException.class)
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("loader-blew-up");
    }
}
