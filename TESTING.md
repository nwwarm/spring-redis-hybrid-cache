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

## JMH microbenchmarks

`cache-benchmarks/` is a sibling Maven project (no parent reactor pom) that exercises the steady-state read-path latencies the library publishes baselines for. The categories cover L1 hit, L2 hit (Testcontainers Redis), single-flight collapse, SWR fresh / stale hits, refresh-ahead dispatch overhead, async `Cache.retrieve(...)` paths, and the reconciliation cycle.

### Run locally

```
mvn -DskipTests install                     # install the library snapshot
mvn -f cache-benchmarks/pom.xml verify      # build + shade benchmarks.jar
java -jar cache-benchmarks/target/benchmarks.jar -rf json -rff results.json
```

Each benchmark is annotated with the JMH mode (`AverageTime`, `SampleTime`, `SingleShotTime`) appropriate to what it measures. Filter with the standard JMH selector, e.g., `java -jar benchmarks.jar L1HitBenchmark`.

### Pass criteria

Per-benchmark targets are pinned in `DESIGN.md` `## 11. Production baselines`. CI (`.github/workflows/benchmarks.yml`) runs a short sweep on every PR that touches `src/main/java/**` or `cache-benchmarks/**`, posts the JSON results as a workflow artifact, and compares against the pinned baseline. The job is **non-blocking** today; the 1.0.0 trigger criteria (DESIGN.md §8) flip it to a blocking gate when the published baseline is locked in.

### Why benchmarks live in a separate module

The default `mvn verify` path stays under five minutes; JMH benchmarks alone take longer. A separate module with its own pom keeps them runnable on demand without paying for the build cost on every commit.

## Chaos testing

Toxiproxy sits between the JVM and Redis on a shared docker network. Tests inject toxics on the proxy (`latency`, `bandwidth`, `limit_data`, `disable`) to simulate the §4 failure modes against a real Redis topology. Tests live under `src/test/java/io/github/nwwarm/hybridcache/chaos/` and are tagged `@Tag("chaos")`.

### Testcontainers wiring

`ChaosTestBase` manages two containers on a shared `Network`: a `redis:7-alpine` Redis with the network alias `redis`, and a `ghcr.io/shopify/toxiproxy:2.7.0` container exposing ports 8474 (admin API) and 8666 (the proxy listener). The cache client is pointed at Toxiproxy's mapped 8666 port, so every command flows through the proxy.

### Run

```
mvn -Pchaos verify
```

The `chaos` profile flips Failsafe's `<groups>` to include `chaos`; the default `mvn verify` excludes the tag (per the `<excludedGroups>chaos,soak</excludedGroups>` in the build config) so the slow Toxiproxy container start does not pay on every commit. CI runs the suite weekly via `.github/workflows/chaos.yml` and on demand via workflow_dispatch.

### Scenarios

| Scenario | Class | What it covers |
|---|---|---|
| **Latency injection** | `RedisLatencyInjectionChaosIT` | 100ms / 500ms / 2s downstream latency via the `latency` toxic. Verifies the per-cache breaker's slow-call detection trips at the configured threshold and recovers to CLOSED once latency returns to normal. |
| **Bandwidth throttling** | `BandwidthThrottlingChaosIT` | 1 KB/s upstream + downstream caps via the `bandwidth` toxic. Verifies cache state is coherent after the throttle is removed (no corruption from queued partial writes). |
| **Connection drops** | `ConnectionDropChaosIT` | `proxy.disable()` cuts every connection mid-command. Verifies Redisson reconnects after re-enable and the breaker closes once probe calls succeed. |
| **Pub/sub message loss** | `PubSubMessageLossChaosIT` | `limit_data` toxic drops bytes after a threshold, corrupting in-flight pub/sub. Reconciliation cycle must close the gap (full two-context assertion deferred — see DESIGN.md §11 / Deferred items). |
| **Cluster shard failover** | `ClusterFailoverChaosIT` | `@Disabled` placeholder; cluster-behind-Toxiproxy fixture deferred past 0.5.0 (DESIGN.md §11 / Deferred items). The existing `ClusterCacheIT.slotReshard_keepsCacheCoherent` is the closest-shaped automated coverage; manual procedure documented in the Sentinel-failover section above is the analogue for cluster. |

### Mapping from §4 failure modes to chaos scenarios

| §4 row | Covered by |
|---|---|
| Redis becomes unreachable | `ConnectionDropChaosIT` |
| Redis becomes slow | `RedisLatencyInjectionChaosIT`, `BandwidthThrottlingChaosIT` |
| Redis recovers | All four scenarios assert recovery after the toxic is removed. |
| Pub/sub message lost | `PubSubMessageLossChaosIT` |
| Subscriber briefly disconnects | `PubSubMessageLossChaosIT` |
| Cluster shard failover | `ClusterFailoverChaosIT` (deferred); `ClusterCacheIT.slotReshard_keepsCacheCoherent` is current coverage. |
| Cluster slot migration | `ClusterCacheIT.slotReshard_keepsCacheCoherent` (existing). |
| Sentinel primary failover | Manual procedure (above). |

## Soak testing

24-hour run of mixed read/write traffic against a Spring Boot WebFlux smoke app exercising every cache tier and every 0.5.0 feature.

### Smoke app location

`soak/` (sibling Maven project; same separation rationale as `cache-benchmarks/`).

`soak/src/main/java/.../SoakApplication.java` boots a `@SpringBootApplication` with three caches configured (NEAR_CACHE with SWR + RA + reconciliation, NEAR_CACHE async with reconciliation, DISTRIBUTED_ONLY with reconciliation). `SoakService` exposes `@Cacheable` methods covering sync, reactive (`Mono`), and `DISTRIBUTED_ONLY` flavours.

### Load harness

`soak/src/main/java/.../SoakDriver.java` is a WebClient-driven driver that sustains the configured req/s for the configured duration. Default arguments: `http://localhost:8080 24h 1000`. The driver is intentionally lightweight; deep introspection (heap, FD, thread counts) is the CI workflow's job.

### Pass criteria

Verbatim from DESIGN.md §7 / Soak tests:

- [ ] Heap stable: no leak; allocate/sec stable; GC pause distribution stable.
- [ ] File descriptor count stable.
- [ ] Thread count stable. No executor leak (the 0.4.0 prefetch-executor leak guardrail's class of bug).
- [ ] Metric counters monotonic; no resets.
- [ ] No `WARN`/`ERROR` log spikes.
- [ ] Preloader snapshots and lock files clean on graceful shutdown.

The CI workflow collects `jcmd GC.heap_info` and `jcmd Thread.print` snapshots at start and end, plus the actuator `/metrics` snapshot, and compares against the published baseline in DESIGN.md §11 / "Soak run summary."

### CI schedule

`.github/workflows/soak.yml` runs nightly at 02:00 UTC. Not on PRs — the run takes 24 hours and would block merges. `workflow_dispatch` trigger lets a maintainer kick a run manually before a release.

Harness is wired; first scheduled soak run produces the baseline numbers that populate DESIGN.md §11 / "Soak run summary." Until that run completes, the table there reads "pending: first run scheduled."

## Load testing

[k6](https://k6.io) scenario at `load/k6-scenario.js` against the same WebFlux smoke app `soak/` boots. Reports per-tier P50/P99 latency and throughput.

### Run

```
docker run -d --name redis -p 6379:6379 redis:7-alpine
mvn -DskipTests install
mvn -f soak/pom.xml spring-boot:run &
k6 run load/k6-scenario.js
```

Per-tier numbers are published in DESIGN.md `## 11. Production baselines`. The thresholds inside `k6-scenario.js` are sanity guards (`p(99)<50` for the near-cache, `p(99)<100` for distributed-only) — the published baseline is the canonical source.

### Why k6 over Gatling

Gatling needs a JVM-based scenario runner. k6 is a single binary, scenarios are plain JavaScript, and the metric tags on each request map cleanly to the per-tier rows in §11. The choice is operational, not architectural.

## Mutation testing

PIT, library-wide, mutation kill-rate gate at 80%.

### Run

```
mvn org.pitest:pitest-maven:mutationCoverage
```

PIT is **not** part of default `mvn verify` — the run is too slow. CI (`.github/workflows/ci.yml` / `mutation-testing` job) runs it as a dedicated job and uploads the HTML report as a workflow artifact.

### PR-blocking gate

`mutationThreshold` and `coverageThreshold` are both set to 80 in `pom.xml`. The CI job fails when either bar drops below 80%. The HTML report is the artefact that drives the per-class triage.

### Exclusions

The `<excludedClasses>` list in `pom.xml` covers the trivial classes whose mutations either survive trivially (data carriers with no behaviour) or are exercised by integration tests rather than mutation-killable unit tests. The per-class rationale lives in DESIGN.md §7 / "Mutation testing exclusions"; this section references it without duplicating.

## Property-based testing

jqwik, run as part of standard `mvn test` / `mvn verify`. Property tests live alongside their subject-under-test in `src/test/java/io/github/nwwarm/hybridcache/core/`.

### Catalog

Every invariant called out in DESIGN.md §4 (failure modes), §5 (configuration model), and §10 (decision log) has at least one property test. The catalog mapping is in DESIGN.md §7 / "Property-based testing catalog" and is the authority — this section references it rather than duplicating the table.

The current property suites:

| Class | Invariant |
|---|---|
| `SwrPredicatePropertyTest` | SWR fresh / stale / evicted classification is total and disjoint over `(now, lastWrite, freshFor, staleFor)` quadruples. |
| `RefreshAheadMonotonicityPropertyTest` | XFetch refresh probability is non-decreasing in `(age, β, runtimeEstimate)` for fixed RNG seed. |
| `ReconciliationDecisionPropertyTest` | `ReconciliationDecision.classify` is total and disjoint over `(redisSeq, observedSeq, tolerance)` triples; regression is never reclassified as miss. |

New invariants land here as new classes; the catalog requirement ensures every cache-correctness rule has a property covering it.
