package io.github.nwwarm.hybridcache.chaos;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Cluster shard failover under sustained load. Covers §4 row
 * "Cluster shard failover": Redisson auto-reconnects to the new
 * primary, hash-tag collocation holds (value and lock follow each
 * other to the new owner shard), no deadlock on lock waiters whose
 * holders crashed during failover.
 *
 * <p>This scenario requires bringing the existing cluster
 * docker-compose topology up behind Toxiproxy (or simply killing one
 * primary mid-load), which is not possible in the toxiproxy-and-one-
 * Redis pattern the rest of the chaos suite uses. The harness here is
 * a placeholder that documents the missing fixture.
 *
 * <p>The fixture is deferred past 0.5.0 — see DESIGN.md §11
 * (Production baselines / Deferred items) for the schedule. Until
 * then, the existing {@code ClusterCacheIT.slotReshard_keepsCacheCoherent}
 * is the closest-shaped automated coverage; a manual procedure is
 * documented in TESTING.md.
 */
@Tag("chaos")
@Disabled("Cluster failover under load requires the cluster topology behind Toxiproxy; "
        + "deferred past 0.5.0 (DESIGN.md §11 / Deferred items)")
class ClusterFailoverChaosIT {
    @Test
    void placeholder() {
        // Intentionally empty — the @Disabled annotation surfaces the
        // gap in CI reports without polluting pass/fail counts.
    }
}
