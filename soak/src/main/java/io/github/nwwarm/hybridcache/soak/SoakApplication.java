package io.github.nwwarm.hybridcache.soak;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;

/**
 * WebFlux smoke app for the soak harness. Exercises every cache tier
 * (LOCAL_ONLY / DISTRIBUTED_ONLY / NEAR_CACHE) and every 0.5.0 feature
 * (reconciliation, SWR, refresh-ahead, async/reactive loaders).
 *
 * <p>Configuration is in {@code application.yml}: caches are wired to
 * cover the full {@link io.github.nwwarm.hybridcache.config.CacheProperties}
 * surface, including the SWR/RA + reconciliation interaction (a
 * NEAR_CACHE with both enabled).
 */
@SpringBootApplication
@EnableCaching
public class SoakApplication {

    public static void main(String[] args) {
        SpringApplication.run(SoakApplication.class, args);
    }
}
