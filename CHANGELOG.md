# Changelog

## 0.5.0 (in progress)

The production-readiness milestone. Closes the missed-invalidation
gap, adds the read-path features hot caches need, brings reactive
applications onto the same primitives the sync path uses. All four
features off by default; sync `@Cacheable` behavior is byte-identical
when no opt-in YAML is set; no 0.4.x configuration breaks.

### Migrating from 0.4.x

**No breaking changes.** Every 0.5.0 feature is opt-in. The required
work to upgrade from 0.4.x is to bump the version and that's it.

The optional work, depending on which features you adopt:

#### Reconciliation (per-cache opt-in)

```yaml
cache:
  caches:
    products:
      reconciliation:
        enabled: true        # default false
        interval: 60s        # default 60s
        miss-tolerance: 5    # default 5
```

Effect: every successful `RTopic.publish(...)` is preceded by one
`INCR` on a per-cache `RAtomicLong`; the new value rides on the
`InvalidationMessage`. A periodic cycle reads the canonical seq and
recovers locally on detected miss. New metrics:
`cache.reconciliation.cycles{cache}`,
`cache.reconciliation.misses{cache}`,
`cache.reconciliation.skipped{cache, reason}`,
`cache.reconciliation.seq.regressions{cache}`.

Cost: `+1 op, +~16 bytes payload, +0 round-trips` per published
invalidation. One extra `GET` per cache per cycle (default 60s).
Rejected at startup on `tier=LOCAL_ONLY` (no cross-node coherence
problem to reconcile against).

#### Stale-while-revalidate (per-cache opt-in)

```yaml
cache:
  caches:
    products:
      ttl: 1h
      swr:
        fresh-for: 5m       # required when swr block is present
        stale-for: 30m      # required; <= ttl
```

Effect: reads inside `[write, fresh-until)` return synchronously;
reads inside `[fresh-until, stale-until)` return the stale value
synchronously *and* dispatch an async refresh on a shared executor.
Reads past `stale-until` see physical eviction.

New metrics: `cache.swr.refreshes{cache, result=success|failure}`,
`cache.swr.refresh.failures{cache}`,
`cache.swr.refreshes.skipped{cache, reason=in_flight}`. Rejected at
startup on `tier=DISTRIBUTED_ONLY`.

#### Refresh-ahead (per-cache opt-in; requires SWR)

```yaml
cache:
  caches:
    products:
      swr:
        fresh-for: 5m
        stale-for: 30m
      refresh-ahead:
        enabled: true       # default false
        beta: 1.0           # default 1.0; XFetch β
```

Effect: every read inside `[write, fresh-until)` evaluates the
XFetch predicate; if it fires, dispatches an async refresh through
the same executor SWR uses. Hotter keys access more often, so they
roll the dice more times. RA without SWR is rejected at startup.

New metrics: `cache.refresh.ahead.fires{cache}`,
`cache.refresh.ahead.failures{cache}`.

#### Async / reactive loader support (no YAML)

Spring 6.1's `Cache.retrieve(...)` is now overridden on every tier
(`LocalOnlyCache`, `DistributedOnlyCache`, `NearCache`). The cache
emits `CompletableFuture<ValueWrapper>`; Spring's `CacheInterceptor`
adapts to/from `Mono`/`Flux` via `ReactiveAdapterRegistry`
upstream. **No code change required** in your application — Spring
uses `retrieve(...)` automatically when your `@Cacheable`-annotated
method has a reactive return type.

The cache layer holds zero Reactor dependencies (banned via the
`maven-enforcer-plugin` rule landed in 0.5.0). `Mono`/`Flux` interop
happens entirely upstream of the cache.

`Flux<T>` results are materialized to `List<T>` before storage —
this is Spring's adaptation, not a library decision. Per-cache
value-size design constraint (~100 KB) applies to the materialized
list. Large `Flux` results push past the constraint and likely
shouldn't be cached as the materialized form at all.

#### Refresh-executor knob (rare)

```yaml
cache:
  refresh:
    scheduler-pool-size: 4   # default 4; shared by SWR / RA / async
```

Effect: shared `FixedThreadPool` for SWR refreshes, RA refreshes,
and async-loader hops off the Netty event loop. Sustained non-zero
`cache.refresh.queue.size` is the signal to tune up.

### Added

- Per-cache reconciliation. YAML: `cache.caches.<name>.reconciliation.{enabled, interval, miss-tolerance}`. Closes the missed-invalidation gap; recovery is local-only (no cross-cluster cascade). Documented in DESIGN.md §3 / Reconciliation and §10 0.5.0 entries.
- Stale-while-revalidate. YAML: `cache.caches.<name>.swr.{fresh-for, stale-for}`. L1 entries gain a fresh-until deadline alongside Caffeine's stale-until.
- Refresh-ahead as a mode of SWR. YAML: `cache.caches.<name>.refresh-ahead.{enabled, beta}`. XFetch (Vattani et al.) probability function, β tunes aggressiveness.
- Async / reactive loader support. `Cache.retrieve(...)` overrides on every tier; no Reactor dependency in the cache layer.
- Top-level refresh-executor knob `cache.refresh.scheduler-pool-size` (default 4).
- New metrics for every feature above; full list in DESIGN.md §6.
- `cache-benchmarks/` sibling Maven module with JMH microbenchmarks for L1/L2 hit, single-flight collapse, SWR, RA, async, and reconciliation.
- `soak/` sibling module with a WebFlux smoke app and 24h soak driver.
- `load/k6-scenario.js` for k6-driven load testing against the smoke app.
- Chaos integration suite under `src/test/java/.../chaos/` (Toxiproxy-based; `@Tag("chaos")`-gated; opt-in via `mvn -Pchaos verify`).
- jqwik dependency on test classpath; `ReconciliationDecisionPropertyTest` and the existing SWR / XFetch property tests now run as part of `mvn verify`.
- `.github/renovate.json` for dependency update automation; banned-deps list pinned (no Reactor in the cache layer).
- `maven-enforcer-plugin` `bannedDependencies` rule rejecting `spring-boot-starter-data-redis-reactive`, `resilience4j-reactor`, `reactor-core`, and `lettuce-core` from runtime classpaths.
- PIT mutation testing now runs library-wide with an 80% kill-rate threshold.
- DESIGN.md §11 / "Production baselines" — the published evidence layer behind the production-ready claim.
- README "Production deployment checklist" section.

### Changed

- `forceRefreshDue()` and `localGeneration()` on `NearCache` and `DistributedOnlyCache` are now public — part of the recovery contract. Previously package-private.
- The `pitest-maven` plugin scope widened from per-class to library-wide. Trivial classes (records, exceptions, auto-config wiring) listed in `<excludedClasses>`; rationale in DESIGN.md §7 / "Mutation testing exclusions."
- Default `mvn verify` now excludes JUnit-tagged categories (`chaos`, `soak`) so the slow opt-in suites stay off the default path.

## 0.4.0 

### ⚠️ Source-incompatible: package reorganization

**Downstream `import` statements must be updated.** All public types
moved from `io.github.nwwarm.*` to new sub-packages under
`io.github.nwwarm.hybridcache.*` (`config`, `core`, `invalidation`,
`metrics`, `preloader`, `probe`). The old packages are gone — there is
no deprecated re-export shim. The migration table is appended at the
end of the Changed section below; the change is mechanical (rewrite
imports) and Spring auto-configuration discovery has been re-pointed
so no YAML changes are required.

### Added

- Per-cache TTL jitter (`cache.caches.<name>.ttl-jitter-ratio`, range `[0.0, 0.5]`). Spreads coordinated L1 reload spikes when many entries expire together. Applies to L1 only; rejected at startup on `tier=DISTRIBUTED_ONLY`.
- Per-cache `max-idle` (`cache.caches.<name>.max-idle`). Optional L1 idle-eviction window — entries not accessed within this duration are evicted regardless of remaining TTL. Must be `<= ttl`; rejected on `tier=DISTRIBUTED_ONLY`.
- `clearImmediate()` method on a new `HybridCache` interface. Eager SCAN + UNLINK of old-generation keys after the generation bump — stronger guarantee than `clear()` for caches cleared frequently relative to TTL where orphan memory matters. Programmatic-only; cast `Cache` to `HybridCache` to invoke.
- Metric `cache.invalidations.published{cache, op=put|evict|clear}` — counts successful `RTopic.publish()` returns. Not incremented on breaker-open or transient publish failure.
- Metric `cache.invalidations.received{cache, op=invalidate|clear}` — counts messages received from peers after self-skip and cache-name resolution. Wire op `OP_INVALIDATE` covers both `put` and `evict`, hence `invalidate` as the receiver-side label.
- Metric `cache.invalidations.received.unknown{cache, op}` — counts messages received for cache names this node doesn't host. Non-zero values flag deployment drift (one node configured with caches the other isn't).
- Optional Redis startup probe (`cache.startup-probe.enabled=true`, default `false`). Verifies Redis reachability at startup; failure surfaces as bean-init failure (clean context-startup failure). Tunable via `cache.startup-probe.timeout` (default `5s`), `cache.startup-probe.retries` (default `1`), `cache.startup-probe.retry-delay` (default `1s`). Skipped on `LOCAL_ONLY`-only deployments.
- Optional sharded pub/sub for Redis 7+ (`cache.invalidation.sharded-pubsub=true`, default `false`). Switches the invalidation transport from `RTopic` (`PUBLISH`/`SUBSCRIBE`) to `RShardedTopic` (`SPUBLISH`/`SSUBSCRIBE`). Eliminates the cluster-bus replication of every message at high invalidation rates. Cluster-only — rejected at startup on `cache.server.mode=SINGLE` and `mode=SENTINEL`.
- Per-cache near-cache preloader (`cache.caches.<name>.preloader.enabled=true`, default `false`). Persists `Caffeine.asMap().keySet()` to disk every `store-interval`; on startup loads the snapshot and prefetches from L2 with bounded concurrency. Closes the cold-start latency gap for predictable working sets. Tunable via `directory`, `store-interval` (default `10m`), `store-initial-delay` (default `1m`), `prefetch-concurrency` (default `16`), `prefetch-timeout` (default `30s`), `max-stored-keys` (default unbounded). NEAR_CACHE-only — rejected on `LOCAL_ONLY` and `DISTRIBUTED_ONLY`. Strictly local disk; pointing two nodes at the same `directory` is undefined behavior, partially detected by a `.preloader.lock` file.
- Top-level preloader knob `cache.preloader.scheduler-pool-size` (default `4`). Threads in the shared scheduler that runs periodic preloader stores; sized for steady-state idle work, not for the boot prefetch burst (the burst runs on a separate cached-style executor).
- Metric `cache.preloader.snapshot.size{cache}` — gauge, last-observed snapshot key count, updated post-store.
- Metric `cache.preloader.store.duration{cache}` — timer, wall-clock per store.
- Metric `cache.preloader.store.failures{cache}` — counter, IO failures during periodic store.
- Metric `cache.preloader.prefetch{cache, result=hit|miss|fail|skipped}` — counter, one increment per key in the prefetch fan-out. `skipped` = `prefetch-timeout` reached.
- Metric `cache.preloader.prefetch.duration{cache}` — timer, full-prefetch wall-clock.

### Changed

- `InvalidationDispatcher` now implements `SmartLifecycle`. Topic listener is removed before the bean-destruction phase that disposes `RedissonClient`, making shutdown ordering deterministic instead of dependent on bean-graph traversal order. The phase value (`Integer.MAX_VALUE - 1024`) pins the dispatcher near the front of any chain of `SmartLifecycle` stops; the actual ordering vs Redisson is enforced by Spring's lifecycle-vs-destruction split, not by the phase number.
- Source-incompatible: every public type moved to a new package under `io.github.nwwarm.hybridcache.*`. Mechanical migration — rewrite `import` statements; no YAML changes required. Spring auto-configuration imports file repointed to the new fully-qualified name. Migration table:

  | Old FQN | New FQN |
  |---|---|
  | `io.github.nwwarm.CacheConfig` | `io.github.nwwarm.hybridcache.config.CacheConfig` |
  | `io.github.nwwarm.CacheProperties` | `io.github.nwwarm.hybridcache.config.CacheProperties` |
  | `io.github.nwwarm.CacheSpecValidator` | `io.github.nwwarm.hybridcache.config.CacheSpecValidator` |
  | `io.github.nwwarm.HybridCacheManager` | `io.github.nwwarm.hybridcache.core.HybridCacheManager` |
  | `io.github.nwwarm.HybridCache` | `io.github.nwwarm.hybridcache.core.HybridCache` |
  | `io.github.nwwarm.NearCache` | `io.github.nwwarm.hybridcache.core.NearCache` |
  | `io.github.nwwarm.DistributedOnlyCache` | `io.github.nwwarm.hybridcache.core.DistributedOnlyCache` |
  | `io.github.nwwarm.LocalOnlyCache` | `io.github.nwwarm.hybridcache.core.LocalOnlyCache` |
  | `io.github.nwwarm.CacheKeys` | `io.github.nwwarm.hybridcache.core.CacheKeys` |
  | `io.github.nwwarm.CodecResolver` | `io.github.nwwarm.hybridcache.core.CodecResolver` |
  | `io.github.nwwarm.KeyLogFormatter` | `io.github.nwwarm.hybridcache.core.KeyLogFormatter` |
  | `io.github.nwwarm.CircuitBreakerFactory` | `io.github.nwwarm.hybridcache.core.CircuitBreakerFactory` |
  | `io.github.nwwarm.LoaderGate` | `io.github.nwwarm.hybridcache.core.LoaderGate` |
  | `io.github.nwwarm.LoaderRejectedException` | `io.github.nwwarm.hybridcache.core.LoaderRejectedException` |
  | `io.github.nwwarm.JitteredExpiry` | `io.github.nwwarm.hybridcache.core.JitteredExpiry` |
  | `io.github.nwwarm.JitteredMaxIdleExpiry` | `io.github.nwwarm.hybridcache.core.JitteredMaxIdleExpiry` |
  | `io.github.nwwarm.InvalidationDispatcher` | `io.github.nwwarm.hybridcache.invalidation.InvalidationDispatcher` |
  | `io.github.nwwarm.InvalidationMessage` | `io.github.nwwarm.hybridcache.invalidation.InvalidationMessage` |
  | `io.github.nwwarm.InvalidationListener` | `io.github.nwwarm.hybridcache.invalidation.InvalidationListener` |
  | `io.github.nwwarm.HybridCacheHealthIndicator` | `io.github.nwwarm.hybridcache.metrics.HybridCacheHealthIndicator` |
  | `io.github.nwwarm.PreloaderCoordinator` | `io.github.nwwarm.hybridcache.preloader.PreloaderCoordinator` |
  | `io.github.nwwarm.NearCachePreloader` | `io.github.nwwarm.hybridcache.preloader.NearCachePreloader` |
  | `io.github.nwwarm.PreloaderFile` | `io.github.nwwarm.hybridcache.preloader.PreloaderFile` |
  | `io.github.nwwarm.RedisStartupProbe` | `io.github.nwwarm.hybridcache.probe.RedisStartupProbe` |

- The package split forced a small set of types and members to bump from package-private to `public` so cross-sub-package callers could reach them. These were not part of the documented public surface in 0.3.0 but are now reachable; consider them implementation surface, not API. Affected types: `CacheKeys`, `CodecResolver`, `KeyLogFormatter`, `CircuitBreakerFactory`, `InvalidationListener`, `NearCachePreloader`. Affected methods: `InvalidationDispatcher.{getNodeId, register, deregister, publish}`, `InvalidationDispatcher` constructors, `PreloaderCoordinator.register`, `NearCache.{localGeneration, getBreaker}`, `KeyLogFormatter` constructor, `CodecResolver` constructor, `CircuitBreakerFactory` constructor + `breakerName` + `resolve`, `CacheKeys.MAX_KEY_BYTES`. Internal-only test seam `PreloaderCoordinator.triggerStoreNow` deliberately stays package-private — see `docs/0.4.0-guardrails.md` guardrail #1.

## 0.3.0

### ⚠️ Wire-incompatible: Redis key format changed

**0.3.0 cannot read entries written by 0.2.0.** The Redis key layout changed
to use Redis Cluster hash tags so the value and lock for a single logical
key collocate on the same Cluster slot.

| | 0.2.0 | 0.3.0 |
|---|---|---|
| value | `<cache>:<gen>:<key>` | `{<cache>:<key>}:v:<gen>` |
| lock | `<cache>:lock:<key>` | `{<cache>:<key>}:lock` |
| gen | `<cache>:generation` | `<cache>:generation` (unchanged) |

**Impact:**

- Entries written by 0.2.0 nodes will not be visible to 0.3.0 nodes. They
  remain in Redis until their TTL expires; they are not reclaimed by 0.3.0.
  Acceptable for a cache, but if the working set is large, expect a brief
  cold-start period.
- **Locks also changed**, so during a rolling 0.2.0 → 0.3.0 deploy, nodes
  on different versions cannot coordinate single-flight on the same key.
  The local Caffeine compute lock still serializes loaders within each
  JVM. The worst case is one extra loader call per node per cold key
  during the rollout window.

**Recommended migration paths:**

1. **Stop-the-world** — drain traffic, deploy all nodes, resume traffic.
   Simplest. Recommended unless rolling deploys are mandatory.
2. **Rolling, accept temporary loss of cross-node single-flight** — deploy
   normally; loss is bounded by the rollout window. Acceptable for caches
   whose source can absorb a transient ~Nx load (where N = node count).
3. **Pre-warm via TTL expiry** — deploy 0.3.0 just before low-traffic
   period; let 0.2.0 entries decay; 0.3.0 lazy-loads on first read.

**Cache name validation tightening:** cache names now reject `{` and `}`
in addition to `:`. A name like `tenant{a}` would shift the Cluster
hash-tag boundary and route every key in that cache to a single shard.
This is caught at startup; any existing config using `{` or `}` in cache
names must be renamed before upgrading.

### Added

- Per-cache concurrency cap on loader calls
  (`cache.specs.<name>.max-concurrent-loaders`,
  `cache.specs.<name>.loader-acquire-timeout`). Protects the source of
  truth when the distributed lock is unavailable (Redis down, breaker
  open, or `LOCAL_ONLY` tier). Rejection surfaces as
  `LoaderRejectedException`.
- Sub-second cross-node propagation of `clear()` on
  `DistributedOnlyCache` via the existing invalidation topic; the 1s
  generation-poll backstop remains for dropped messages.
- Per-cache circuit breakers via the Resilience4j registry pattern. A
  noisy cache trips only its own breaker.
- Startup validation of cache specs (`CacheSpecValidator`) — TTL,
  lock-wait/lease, codec/tier compatibility, and per-cache circuit-breaker
  fields are validated and reported in a single error.
- `cache.log-keys` (default `false`) controlling whether keys appear
  verbatim in log output. When false, replace key occurrences in logs
  with a stable hash so PII in keys (user IDs, emails, tokens) doesn't
  leak into log aggregation.
- New metric `cache.invalidations.suppressed.cold_load` —
  cold-load completions no longer publish an invalidation (peers have
  no stale L1 to evict); the counter increments on each suppressed
  publish.
- New metrics `cache.loaders.permits.available` (gauge) and
  `cache.loaders.rejections` (counter) — registered only when
  `max-concurrent-loaders` is set.

### Changed

- CAS-guarded generation refresh (was a `volatile long`); under
  contention only the thread that wins the CAS issues the Redis
  round-trip, the rest return the cached generation. Eliminates a
  thundering herd on the generation poll.
- Cold-load completions in `NearCache.loadWithDistributedLock` no longer
  publish an invalidation. Peer L1 is either empty (lazy-loads on next
  read from now-warm L2) or holds a value the peer wrote itself —
  neither is stale. `put` and `evict` continue to publish.

## 0.2.0

Initial public release.
