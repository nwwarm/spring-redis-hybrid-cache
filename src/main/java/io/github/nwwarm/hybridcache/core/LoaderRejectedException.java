package io.github.nwwarm.hybridcache.core;

/**
 * Thrown when a loader call is rejected because the per-cache concurrency
 * cap ({@code cache.specs.<name>.max-concurrent-loaders}) is saturated and
 * a permit could not be acquired within {@code loaderAcquireTimeout}.
 *
 * <p>Distinct from {@link org.springframework.cache.Cache.ValueRetrievalException}
 * — that wraps a loader that ran and failed. {@code LoaderRejectedException}
 * means the loader never ran. Treat it as a transient backpressure signal:
 * the result is not cached (Caffeine does not cache exceptions from its
 * compute function), so a retry once the storm subsides will succeed.
 */
public class LoaderRejectedException extends RuntimeException {

    public LoaderRejectedException(String cacheName, Object key) {
        super("Loader rejected for cache='" + cacheName + "' key=" + key
                + " — max-concurrent-loaders saturated and acquire timed out");
    }
}
