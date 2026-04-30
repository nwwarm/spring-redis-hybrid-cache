# Testing

## Automated integration tests

Each Redis topology has a Testcontainers-driven integration test that exercises the production `CacheConfig.redissonClient` path against a real container of that topology.

| Suite | What it covers |
|---|---|
| `SingleServerCacheIT` | Production wiring, `@Cacheable` semantics, `@CacheEvict`, cross-node invalidation, mode-defaults-to-SINGLE backwards compatibility, mode-aware validation failure. |
| `ClusterCacheIT` | Same four behaviors against a 6-node cluster (3 primaries + 3 replicas), plus a slot-reshard test that verifies Redisson follows `MOVED` redirects transparently. |
| `SentinelCacheIT` | Same four behaviors against a 1 primary + 1 replica + 3 sentinels topology. Sentinel-driven failover is **not** part of the automated suite — see "Manual sentinel failover test" below. |

### Running

```
mvn verify
```

All tests run via the Failsafe plugin (`*IT.java`). Docker must be available; Linux is required for the cluster and sentinel suites because both use `network_mode: host` for cluster-bus and Sentinel announce-IP reachability.

### Compose files

- Cluster: `src/test/resources/cluster/docker-compose.yml`
- Sentinel: `src/test/resources/sentinel/docker-compose.yml`

Both use only the official `redis:7-alpine` image. Service names avoid trailing `-<digit>` (e.g. `redis1` rather than `redis-1`) because Testcontainers' `ComposeDelegate.getServiceInstanceName` regex would otherwise misread the dash-suffix as the compose instance index.

## Manual sentinel failover test

The automated `SentinelCacheIT` covers connection, cache semantics, and cross-node invalidation. Sentinel-driven failover (kill the primary, observe the cache recovering automatically once Sentinels promote a replica) is intentionally manual: it is timing-sensitive in CI and depends on the host's container networking, and a flaky failover test would erode trust in the rest of the suite.

To verify failover by hand:

1. Start the topology used by the test:

   ```
   docker compose -f src/test/resources/sentinel/docker-compose.yml -p sentinel-manual up -d
   ```

2. Wait ~10 seconds for Sentinels to converge (`sentinel ckquorum mymaster` should return OK from each Sentinel):

   ```
   for p in 26379 26380 26381; do
     docker exec sentinel-manual-sentinel1-1 redis-cli -p $p sentinel ckquorum mymaster
   done
   ```

3. Run a small client against the topology that does a `set` followed by repeated `get`s in a loop. Use the `cache.server.mode=SENTINEL` configuration from the README's Deployment topologies section, with addresses pointing at `redis://127.0.0.1:26379..26381`.

4. Kill the current primary:

   ```
   docker stop sentinel-manual-redismaster-1
   ```

5. Observe the client. Expectations:
   - For ~5-30s the circuit breaker opens and reads degrade to L1 (the read path documented in the README's "Failure behavior" table under "Sentinel primary failover").
   - Once Sentinels elect the replica as the new primary (typically <30s), Redisson reconnects and L2 reads resume.
   - The cache state from before the failover is intact (the replica has it).

6. Tear down:

   ```
   docker compose -f src/test/resources/sentinel/docker-compose.yml -p sentinel-manual down -v
   ```

### Why this is not automated

The `failover-timeout` and `down-after-milliseconds` knobs in the Sentinel configuration interact with Testcontainers' container start ordering, the host's docker daemon scheduling, and the JVM's GC pauses in a way that produces ~5-15% flake rates without substantial scaffolding (multiple election attempts, retry budgets, looser assertions on timing). The cost of that scaffolding outweighs the value compared to a documented manual procedure run during release validation.
