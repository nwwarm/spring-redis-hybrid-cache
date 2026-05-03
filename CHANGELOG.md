# Changelog

## 0.3.0 (unreleased)

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
