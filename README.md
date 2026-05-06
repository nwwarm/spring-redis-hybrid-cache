# spring-redis-hybrid-cache

**Multi-layer caching for Spring Boot with cross-node invalidation, stampede protection, and graceful Redis degradation.**

Drop-in replacement for the typical `@Cacheable` + Redis setup that addresses the failure modes most home-grown solutions miss: stale local copies after remote writes, thundering herds on cold keys, and cascading outages when Redis is slow.

```xml
<dependency>
  <groupId>io.github.nwwarm</groupId>
  <artifactId>hybrid-cache-spring-boot-starter</artifactId>
  <version>0.4.0</version>
</dependency>
```

```java
@Cacheable("products")
public Product findById(Long id) { ... }
```

That's the entire integration. Existing `@Cacheable` annotations work unchanged; the library plugs in below Spring's cache abstraction.

---

## What this library guarantees

These are the operational properties this library commits to. Each is implemented with a specific mechanism, documented below.

| Guarantee | Mechanism |
|---|---|
| **Cross-node coherence on writes.** A write on one node invalidates the local L1 on every other node, bounded by pub/sub propagation latency (typically <10ms in healthy Redis). | Redis pub/sub topic with node-id self-skip; subscribers evict L1 entries and lazy-load from L2 on next read. |
| **Stampede protection on cold keys.** Concurrent requests for an unpopulated key result in exactly one loader call per JVM, and exactly one across the cluster when Redis is healthy. | Two-tier single-flight: Caffeine's atomic compute lock for per-JVM serialization, Redisson `RLock` for cross-node serialization. |
| **Graceful degradation during Redis incidents.** Reads degrade to local-cache-only with a circuit breaker; the application stays available. Writes are suppressed; cross-node coherence is bounded by TTL during the incident. | Resilience4j circuit breaker wrapping all L2 operations. Open breaker → L2 calls skipped, application continues serving from L1. |
| **O(1) cache clears.** `@CacheEvict(allEntries = true)` does not block on keyspace iteration regardless of cache size. | Generation-counter prefix on every key; clear bumps the counter, old keys orphan and reclaim via Redis TTL. |
| **Per-cache configuration.** TTL, size, codec, and consistency are configured per cache name in YAML, not globally. | `cache.caches.<name>.*` properties, per-name `Caffeine` and Redisson codec. |

---

## When to use

This library is the right choice when:

- **Read-heavy workloads** with hot keys: catalogs, configuration, reference data, user profiles, lookups. The L1 layer eliminates Redis round-trips for the common case; cross-node invalidation keeps the data coherent.
- **Multi-instance deployments** behind a load balancer, where any node may serve any request. The invalidation pipeline ensures a write on one instance doesn't leave other instances serving stale data.
- **Expensive backend calls** that you can't afford to thunder. A cold key spike on a database-backed loader, with N application instances each fielding M concurrent requests, becomes 1 backend call instead of N×M without single-flight protection.
- **Mixed cache workloads** in the same application: some shared (sessions, rate limiters across instances), some per-node (request-scoped memoization), some classical near-cache (product catalogs). Three tiers — `LOCAL_ONLY`, `DISTRIBUTED_ONLY`, `NEAR_CACHE` — pick per cache name in YAML.

## When not to use

This library is the wrong choice when:

- **Strong consistency is a correctness requirement.** Financial balances during a transfer, inventory counts during checkout, anywhere stale reads have a real cost. Use direct database reads or the `DISTRIBUTED_ONLY` tier with synchronous writes — both have failure modes you should think through carefully.
- **Single-instance deployments with no Redis.** The library degrades correctly to local-only mode, but you're paying for distributed-cache machinery you don't use. Use Caffeine directly via Spring's `CaffeineCacheManager`.
- **Write-heavy workloads.** A near-cache assumes reads vastly outnumber writes. If you're writing more than ~10% of operations, the invalidation overhead and L1 churn cost more than the L1 hit savings. Use a write-through Redis cache or skip the L1 layer entirely (`DISTRIBUTED_ONLY` tier).
- **Strict TTL semantics.** The default O(1) `clear()` leaves orphaned entries in Redis until they age out via TTL. If your auditing requires "after `clear()`, no entry exists in Redis," cast to `HybridCache` and call `clearImmediate()` instead — it bumps the generation, then SCAN+UNLINKs every old-generation key on every reachable shard before returning. Use it when caches are cleared frequently relative to TTL and orphan memory matters.
- **Cached values larger than ~1 MB.** Redis is not optimized for large values, and serialization round-trips over a 1 MB payload erase the latency benefit of caching. Cache the identifier, fetch the payload directly.

---

## Read and write flows

### Read

```
@Cacheable method invoked
        │
        ▼
   ┌─────────────┐   hit    ┌─────────────────┐
   │ L1 (Caffeine)├─────────►│ return cached   │
   └─────┬───────┘           └─────────────────┘
         │ miss
         ▼
   ┌─────────────┐   hit    ┌─────────────────┐
   │ L2 (Redis)  ├─────────►│ populate L1,    │
   └─────┬───────┘          │ return cached   │
         │ miss             └─────────────────┘
         ▼
   ┌─────────────┐
   │ acquire     │   ◄── single-flight: only one loader call
   │ RLock       │       per JVM, and one per cluster when
   └─────┬───────┘       Redis is healthy
         ▼
   ┌─────────────┐
   │ load from   │
   │ source      │
   └─────┬───────┘
         ▼
   populate L2 → populate L1 → return
```

### Write

```
@CachePut / Cache.put method invoked
        │
        ▼
   ┌─────────────┐
   │ update L1   │   ◄── this node's read-after-write is immediate
   └─────┬───────┘
         ▼
   ┌─────────────┐
   │ update L2   │   ◄── authoritative copy
   └─────┬───────┘
         ▼
   ┌──────────────────┐
   │ publish          │   ◄── one global topic; node-id self-skip
   │ invalidation     │       prevents this node from invalidating its
   │ message          │       own L1 entry
   └─────┬────────────┘
         ▼
   other nodes evict L1 → lazy-load from L2 on next read
```

### Evict (NearCache)

```
@CacheEvict method invoked
        │
        ▼
   ┌─────────────┐   success    ┌──────────────────┐
   │ delete L2   ├─────────────►│ publish          │
   └─────┬───────┘              │ invalidation     │
         │ failure              │ message          │
         │ (breaker open        └─────┬────────────┘
         │  or exception)             ▼
         │                       other nodes evict L1
         ▼
   ┌─────────────────────────┐
   │ skip publish            │ ◄── if L2 still has the value, telling
   │ log WARN                │     remote nodes to invalidate would let
   │ increment l2Failures    │     them re-populate L1 from L2 with the
   └─────┬───────────────────┘     stale value. Better to leave them
         │                          serving from L1 until TTL.
         ▼
   evict local L1 (always)        ◄── this node's L1 ends in a
                                      strict-or-equal state vs L2
```

The local L1 is evicted on both branches — this node's L1 cannot end up holding a value that's no longer in L2. Cross-node coherence is bounded by TTL when the L2 delete failed.

### Staleness boundaries

| Question | Answer |
|---|---|
| Can this node see its own writes immediately? | Yes. L1 is updated synchronously before the write returns. |
| Can other nodes see the write immediately? | No. Other nodes serve from their L1 until the invalidation message arrives, typically <10ms in healthy Redis. After invalidation, the next read on those nodes hits L2 (which has the new value). |
| What if the invalidation message is lost? | Bounded by L1 TTL. Other nodes eventually expire their L1 entry and re-read from L2. |
| What if Redis is down during a write? | This node's L1 has the new value; other nodes do not. Bounded by L1 TTL. Document this as expected behavior; design around it for write-sensitive code paths. |
| Can `@CacheEvict(allEntries = true)` fail to take effect on remote nodes? | No. The clear bumps a distributed generation counter; remote nodes refresh their generation lazily (~1s) and immediately on receipt of the clear message. Even if the message is lost, the next read on remote nodes uses a stale generation prefix, finds no entry, and lazy-loads. |

---

## Failure behavior

This is the section that distinguishes a production cache from a demo. Every scenario below is implemented and tested.

| Scenario | Behavior |
|---|---|
| **Redis becomes unreachable** | Circuit breaker opens after configurable failure threshold (default: 50% failure rate over 20 calls). Reads degrade to L1-only — hits return cached values, misses invoke the loader and populate only L1. Writes update L1 only; cross-node coherence is suspended for the duration of the outage. |
| **Redis becomes slow** | Slow-call detection (default: >500ms threshold, 80% slow rate triggers open). Same behavior as unreachable: open breaker, L1-only. The application does not block on slow Redis calls. |
| **Redis recovers** | Breaker enters half-open state after configured wait (default: 30s). A small number of probe calls (default: 3) test Redis health. On success, breaker closes and full L2 operation resumes. On failure, breaker re-opens. |
| **Pub/sub message lost** | L1 entries on remote nodes serve stale values until L1 TTL expiration. Bounded by configured TTL per cache (default: 1 hour). For caches where this window is unacceptable, configure shorter TTLs. |
| **L2 delete fails during evict** (`NearCache` only) | The `NearCache` evict path runs as `bucket.delete(key)` → publish invalidation → evict local L1. If the L2 delete fails (breaker open, connection error), the cross-node invalidation message is **not** published — publishing would tell remote nodes to drop a still-coherent L1 entry that would then re-populate from the still-present L2 value. The local L1 is always evicted; this node ends in a strict-or-equal state vs L2. Cross-node coherence is bounded by TTL until the next successful evict or write. The originating node logs `WARN  L2 evict failed for key '...'; cross-node coherence not guaranteed until TTL`. |
| **Cache stampede on cold key** | Local single-flight via Caffeine: only one thread per JVM enters the loader. Cross-node single-flight via Redisson `RLock`: only one node loads when Redis is healthy. During Redis outage, cross-node coordination is suspended; worst case becomes N database loads across N nodes (still bounded, much better than N × threads-per-node). |
| **Loader exception** | Exception propagates to the caller. No partial cache state is written. Subsequent calls retry the loader. |
| **Lock holder crashes mid-load** | Redisson watchdog releases the lock automatically (default: 30s lease, renewed every 10s while holder is alive). Waiting threads wake up and one becomes the new holder. |
| **Two nodes write the same key concurrently** | Last write wins at L2 (Redis single-master serializes). Both nodes' L1 entries are invalidated by each other's pub/sub messages. Both nodes lazy-load on next read and see the winning write. No corruption; eventual consistency. |
| **Cluster shard failover** | Brief unavailability for keys hashing to the failing shard (typically 5-30s while the replica is promoted). Redisson auto-reconnects to the new primary. Single-flight lock holders on the affected shard may lose their lock during failover; acceptable for a cache (single-flight is throughput protection, not correctness). Cache values on other shards continue serving normally. |
| **Cluster slot migration during reshard** | Operations on migrating keys may receive `ASK` or `MOVED` redirects from Redis. Redisson handles these transparently — the operation completes against the new owner shard. No application-visible failure during reshard. |
| **Sentinel primary failover** | Brief pause (typically 5-30s) while Sentinels detect failure and elect a new primary. During the pause, L2 reads fail and the circuit breaker may open. Reads degrade to L1 until Redis is reachable again. Recovery is automatic once the new primary is elected and Redisson reconnects. |
| **Sentinel quorum loss** | If fewer than the configured quorum of Sentinels are reachable, no failover can occur. The current primary remains writable; the cache continues operating normally. If the primary then fails while quorum is lost, manual operator intervention is required. The library cannot detect this state directly — monitor Sentinel quorum health separately via your Redis monitoring. |
| **Cluster/Sentinel read routing** | Reads are routed to masters by default to preserve read-your-writes coherence with the invalidation pipeline. Applications can override via custom `RedissonClient` bean if eventual-consistency reads are acceptable in exchange for higher read throughput. |
| **Network partition between nodes** | Each side of the partition serves from L1 within TTL bounds. After partition heals, normal pub/sub propagation resumes. Stale entries written before the partition expire via TTL. This is a correctness-vs-availability tradeoff: the library favors availability. |

---

## Quick start

### 1. Add the dependency

```xml
<dependency>
  <groupId>io.github.nwwarm</groupId>
  <artifactId>hybrid-cache-spring-boot-starter</artifactId>
  <version>0.4.0</version>
</dependency>
```

### 2. Configure caches in `application.yml`

```yaml
cache:
  server:
    address: redis://localhost:6379
  allowed-packages:
    - com.example.domain.     # security: narrow polymorphic deserialization
                              # entries MUST end with '.' (see Security)
  default-spec:
    tier: NEAR_CACHE
    ttl: 1h
    maximum-size: 10000
  caches:
    products:
      tier: NEAR_CACHE
      ttl: 6h
      maximum-size: 50000
    user-sessions:
      tier: DISTRIBUTED_ONLY  # shared across nodes, no L1
      ttl: 30m
    request-rate-limits:
      tier: LOCAL_ONLY        # per-node, no Redis
      ttl: 1m
```

`cache.allowed-packages` is **required** — the application will not start without it. See the Security section for why and what alternatives exist.

### 3. Use `@Cacheable` as normal

```java
@Service
public class ProductService {

  @Cacheable("products")
  public Product findById(Long id) {
    return productRepository.findById(id).orElseThrow();
  }

  @Cacheable(cacheNames = "products", sync = true)  // single-flight protection
  public Product findExpensive(Long id) {
    return slowExternalCall(id);
  }

  @CacheEvict("products")
  public void invalidate(Long id) { /* ... */ }

  @CacheEvict(cacheNames = "products", allEntries = true)
  public void reload() { /* ... */ }
}
```

That's the full integration. The `CacheResolver` is wired automatically as the default resolver; no extra annotations needed on services.

---

## Deployment topologies

The library connects to Redis through Redisson and supports three topologies. Select the topology with `cache.server.mode`. If `mode` is not set, `SINGLE` is used — the existing schema continues to work without modification.

Validation runs at application startup. A misconfigured deployment (e.g., `CLUSTER` without `addresses`) fails fast with a mode-aware `IllegalArgumentException`, not at first cache operation.

### Single server (default)

A standalone Redis instance. Appropriate for development and single-instance production deployments.

```yaml
cache:
  server:
    mode: SINGLE                           # optional; this is the default
    address: redis://redis.internal:6379
    password: ${REDIS_PASSWORD:}
```

### Redis Cluster

A horizontally sharded deployment. Use when the working set exceeds a single primary's memory or when you need horizontal write scalability. Redisson handles `MOVED`/`ASK` redirects and shard failover transparently.

```yaml
cache:
  server:
    mode: CLUSTER
    addresses:
      - redis://redis-1.internal:6379
      - redis://redis-2.internal:6379
      - redis://redis-3.internal:6379
    password: ${REDIS_PASSWORD:}
    scan-interval: 2000                    # optional, milliseconds; default 2000
```

`addresses` should list at least one master per shard; Redisson discovers replicas via `CLUSTER NODES`. The `scan-interval` controls how often Redisson refreshes its slot map to detect topology changes.

### Sentinel

A high-availability deployment with a primary, one or more replicas, and a Sentinel quorum that monitors and elects a new primary on failure. Use when you need automatic failover but don't need horizontal sharding.

```yaml
cache:
  server:
    mode: SENTINEL
    master-name: mymaster
    addresses:
      - redis://sentinel-1.internal:26379
      - redis://sentinel-2.internal:26379
      - redis://sentinel-3.internal:26379
    password: ${REDIS_PASSWORD:}
```

`addresses` lists Sentinel addresses (not Redis data nodes). The `master-name` must match the `sentinel monitor` configuration on the Sentinels.

---

## Tier model

| Tier | L1 | L2 | When to choose |
|---|---|---|---|
| **`NEAR_CACHE`** (default) | ✓ Caffeine | ✓ Redis | Read-heavy data, hot keys, multi-instance deployments. Most caches. |
| **`LOCAL_ONLY`** | ✓ Caffeine | — | Per-node state with no cross-node coherence requirement. Rate limiters, request-scoped memoization. |
| **`DISTRIBUTED_ONLY`** | — | ✓ Redis | Shared state where consistency matters more than latency, or working sets too large for per-node memory. Sessions, idempotency keys. |

Tier is selected per cache name in `application.yml`. Application code is unchanged regardless of tier.

---

## Performance characteristics

This library has not been formally benchmarked in production conditions and the README will not invent numbers. What can be said honestly:

- **L1 hits** complete in nanoseconds — Caffeine's published benchmarks consistently show this and apply directly. No serialization, no network.
- **L2 hits** add the cost of one Redis round-trip (typically <1ms LAN, <5ms WAN) plus deserialization. Compare against the cost of the underlying source — usually 5-50ms for a database query. The L1+L2 layering means most reads avoid both.
- **Misses with single-flight** add the cost of one `RLock` acquisition (~1 Redis round-trip for the lock, ~1 for the value, ~1 for unlock). On a cold key with high concurrency, this turns N synchronized loader calls into 1 loader call plus N-1 cache reads after the holder populates.
- **Memory cost** in L1 is bounded by configured `maximumSize` per cache. Caffeine's overhead per entry is approximately 96 bytes plus key and value sizes.

For production sizing, measure your specific workload. Micrometer integration is described in the Operations section.

---

## Operations

### Metrics

The library exposes the following Micrometer metrics, tagged by cache name:

| Metric | Type | Meaning |
|---|---|---|
| `cache.gets{cache, result=hit\|miss}` | counter | L1 hits and misses (Caffeine layer). |
| `cache.l2.gets{cache, result=hit\|miss}` | counter | L2 hits and misses (Redis layer). Hit rate here measures how often Redis saves a backend call. |
| `cache.l2.get.latency{cache}` | timer | L2 read latency. Watch p99 — sustained increase signals Redis or network degradation. |
| `cache.l2.failures{cache}` | counter | Exceptions during L2 operations. |
| `cache.l2.breaker.open{cache}` | counter | Number of L2 calls rejected by an open breaker. |
| `cache.invalidations.published{cache, op=put\|evict\|clear}` | counter | Successful pub/sub publishes. Increments after `RTopic.publish()` returns; not incremented on breaker-open or transient publish failure. |
| `cache.invalidations.received{cache, op=invalidate\|clear}` | counter | Messages received from peers (after self-skip). Wire op `OP_INVALIDATE` covers both `put` and `evict`, hence `invalidate` as the receiver-side label. |
| `cache.invalidations.received.unknown{cache, op}` | counter | Messages received for cache names this node doesn't host. A non-zero value means deployment drift — one node has a cache the other doesn't, or a forged/stale message is on the topic. |
| `cache.invalidations.suppressed.cold_load{cache}` | counter | Cold-load completions that intentionally did not publish (peer L1 has nothing stale to drop). Different question from the publish/receive pair — leaves the publish path quieter than naive write-through would. |
| `resilience4j.circuitbreaker.state{name=redis-cache}` | gauge | Breaker state. 0=closed, 1=half-open, 2=open. |
| `cache.size{cache}` | gauge | Current Caffeine entry count. |

### Alerts

The three signals worth alerting on, with example Prometheus rules:

```promql
# Breaker open for sustained period — Redis incident in progress
resilience4j_circuitbreaker_state{name="redis-cache"} == 2
# alert: for 1m

# L1 hit rate collapse — cache thrashing or misconfiguration
sum(rate(cache_gets_total{result="hit"}[5m])) by (cache)
  / sum(rate(cache_gets_total[5m])) by (cache) < 0.5
# alert: for 10m, severity warning

# L2 latency p99 — Redis or network degradation
histogram_quantile(0.99, rate(cache_l2_get_latency_seconds_bucket[5m])) > 0.1
# alert: for 5m
```

### Health

The library registers a Spring Boot Actuator `HealthIndicator` named `hybridCache` when Actuator is on the classpath. `/actuator/health` reports the operational state of the L2 path:

| Breaker state | Redis ping | Reported status |
|---|---|---|
| `CLOSED` | success | `UP` |
| `CLOSED` | timeout/unreachable | `DOWN` |
| `OPEN` / `FORCED_OPEN` | (any) | `OUT_OF_SERVICE` |
| `HALF_OPEN` | (any) | `UNKNOWN` |

Details include:

- `breakerState`, `breakerFailureRate`, `breakerSlowCallRate`
- `redisPing` (`ok`, `timeout after Nms`, or `unreachable: <message>`) and `redisPingMs` when reachable
- `caches.<name>` map with per-cache `size`, `hitCount`, `missCount`, `hitRate` from the underlying Caffeine when present

The Redis ping is a single async `EXISTS` against a sentinel key, bounded by `cache.health.ping-timeout` (default `500ms`). The endpoint never blocks longer than the timeout — `/actuator/health` stays responsive during a Redis incident, which matters because Kubernetes liveness probes and load balancers hit it.

Useful for Kubernetes readiness probes and monitoring tool health checks.

### Logging

Notable log lines and their operational meaning:

| Log line (severity) | Meaning |
|---|---|
| `WARN  L2 read failed for key '...'; degrading to local-only` | Single L2 read exception. If frequent, breaker will open. |
| `WARN  L2 write failed for key '...'; cross-node incoherence until TTL` | A write succeeded locally but didn't reach Redis. Bounded by TTL. |
| `WARN  L2 evict failed for key '...'; cross-node coherence not guaranteed until TTL` | An evict couldn't reach L2. The `NearCache` suppresses the cross-node invalidation message in this case (see Failure behavior, "L2 delete fails"); remote nodes' L1 entries serve their stale value until the TTL expires. |
| `WARN  Generation bump failed; clear visible only locally` | A `clear()` couldn't update Redis. Other nodes won't see the clear until their generation refreshes (which won't help if Redis is down). |
| `INFO  Polymorphic type validator restricted to packages: [...]` | At startup, confirms the security validator is configured. |

---

## Configuration reference

| Property | Default | Purpose |
|---|---|---|
| `cache.server.mode` | `SINGLE` | Connection topology: `SINGLE`, `CLUSTER`, or `SENTINEL`. Determines which other `cache.server.*` fields are required. |
| `cache.server.address` | (required for `SINGLE`) | Single-server address, e.g., `redis://localhost:6379`. Used only when `mode=SINGLE`. |
| `cache.server.addresses` | (required for `CLUSTER` and `SENTINEL`) | List of node addresses. For `CLUSTER`, list master node addresses (Redisson discovers replicas). For `SENTINEL`, list Sentinel addresses (not data nodes). |
| `cache.server.master-name` | (required for `SENTINEL`) | Sentinel `master-name` matching the `sentinel monitor` configuration. Used only when `mode=SENTINEL`. |
| `cache.server.scan-interval` | `2000` | Milliseconds between Redisson cluster slot-map refreshes. Used only when `mode=CLUSTER`. |
| `cache.server.password` | empty | Redis password. Applies to all modes. |
| `cache.node-id` | random UUID | Identifies this JVM in invalidation messages. Set explicitly to correlate with logs. |
| `cache.allowed-packages` | (required for default JSON codec) | Package prefixes allowed for Jackson polymorphic deserialization. Each entry must end with `.` or be a fully-qualified class name. Empty value fails startup. See Security. |
| `cache.kryo.registered-classes` | `[]` | Fully-qualified class names registered with the Kryo codec. Required (non-empty) when any cache uses `codec: KRYO`. Order matters — appending is safe, reordering breaks the wire format. See Security. |
| `cache.health.ping-timeout` | `500ms` | Bounds the Redis ping issued by the Actuator health indicator. `/actuator/health` returns within this window even if Redis is unreachable. |
| `cache.startup-probe.enabled` | `false` | Optional fail-fast Redis reachability probe at application startup. Default behaviour stays "boot lazily and surface failures via the breaker" — the probe is for environments where fail-to-boot is preferred over boot-and-then-500. Skipped automatically when every configured cache is `tier=LOCAL_ONLY` (Redis isn't on the request path). |
| `cache.startup-probe.timeout` | `5s` | Per-attempt timeout. Total bound on probe time is `(retries + 1) * timeout + retries * retry-delay`. |
| `cache.startup-probe.retries` | `1` | Additional attempts after the first failure (so two attempts total by default). `0` is valid — single attempt. |
| `cache.startup-probe.retry-delay` | `1s` | Sleep between attempts. `0` is valid — retry immediately; the per-attempt timeout still paces the loop. |
| `cache.invalidation.sharded-pubsub` | `false` | Cluster-only opt-in to Redis 7.0+ sharded pub/sub (`SPUBLISH`/`SSUBSCRIBE`, Redisson `RShardedTopic`). All nodes still receive every invalidation, but the channel is pinned to one shard so the cluster bus stays out of the path — useful at high invalidation rates. Rejected at startup on `mode=SINGLE` and `mode=SENTINEL`. Heterogeneous deployments (some nodes sharded, some not) are unsupported and will silently fail to deliver between groups. |
| `cache.default-spec.tier` | `NEAR_CACHE` | Default tier for caches not explicitly configured. |
| `cache.default-spec.ttl` | `1h` | Default TTL. |
| `cache.default-spec.maximum-size` | `10000` | Default L1 maximum entries. |
| `cache.default-spec.lock-wait` | `5s` | How long to wait for the cross-node lock before falling through. |
| `cache.default-spec.lock-lease` | `30s` | Lock holder's lease before automatic release. Must exceed worst-case loader latency. |
| `cache.default-spec.codec` | `JSON` | L2 wire format. `JSON` (debuggable) or `KRYO` (compact, faster). |
| `cache.default-spec.ttl-jitter-ratio` | `0.0` | Per-entry random spread on the L1 (Caffeine) TTL. Each entry's effective expiry is `ttl ± uniform(ttl·ratio)`, sampled at insert/update. Spreads coordinated reload spikes when many entries expire together. Range `[0.0, 0.5]`; `0.0` disables jitter. Applies only to L1 — L2 TTLs are unchanged. Rejected on `tier=DISTRIBUTED_ONLY` (no L1 to apply it to). |
| `cache.default-spec.max-idle` | unset | Optional L1 idle-eviction window. When set, an entry not accessed for this duration is evicted regardless of remaining TTL; reads reset the idle timer, with TTL still acting as a hard ceiling. Useful for evicting cold entries before the absolute TTL on caches with hot/cold key distributions. Must be `<= ttl`. Rejected on `tier=DISTRIBUTED_ONLY` (no L1). |
| `cache.caches.<name>.*` | inherits default-spec | Per-cache overrides. |

---

## Cache keys

Keys are stringified at every cache boundary (`put`, `get`, `evict`) before they reach Caffeine, Redis, or the cross-node invalidation pipeline. The library never stores `Object` keys internally and never serializes a key into a pub/sub payload. This avoids cross-type mismatches like a `Long` key that arrives on a remote node as `Integer` and fails to match the original entry.

The stringification has three constraints:

- **Reserved character `':'`** — colons are used as path separators inside Redis keys. A user-supplied key whose `toString()` contains `':'` is rejected at `put`/`get`/`evict` with `IllegalArgumentException`. If your keys naturally contain colons (tenant prefixes, namespaced ids), apply a custom `KeyGenerator` that escapes them.
- **256-byte length limit** — measured in UTF-8 bytes after stringification. Long keys waste Redis memory and CPU; if you have keys this large, hash them in a `KeyGenerator` (UUID, SHA-1, etc.) before they reach the cache.
- **Null keys** — map to the sentinel string `_null`. A `null` key is valid (you can cache and evict by null), but only one entry per cache shares that key.

Cache names share the `':'` prohibition (same path-separator reason) and additionally reject `'{'` and `'}'` — these delimit Redis Cluster hash tags, and a name containing them would shift the tag boundary and route every key in the cache to a single shard. Names listed under `cache.caches.*` are validated at startup; names that fall through to `default-spec` are validated on first access.

### Redis key layout (0.3.0+)

The library produces three kinds of Redis key per logical cache entry:

| | Format | Example |
|---|---|---|
| value | `{<cache>:<key>}:v:<generation>` | `{products:42}:v:0` |
| lock | `{<cache>:<key>}:lock` | `{products:42}:lock` |
| gen | `<cache>:generation` | `products:generation` |

The `{...}` is a Redis Cluster hash tag — Cluster routes by the substring inside it. So value and lock for the same logical `(cache, key)` collocate on one shard, while different keys distribute across shards normally. The generation counter sits outside the tag (per-cache, intentionally not collocated with each key).

**Upgrading from 0.2.0:** the key format changed. 0.3.0 will not see entries written by 0.2.0; entries lazily reload — acceptable for a cache, but plan for a brief cold-start period after the upgrade. Lock keys also changed, so during a rolling deploy 0.2.0 and 0.3.0 nodes cannot coordinate single-flight on the same key. See [`CHANGELOG.md`](CHANGELOG.md) for full migration guidance.

## Multi-tenancy

The cache implementations are tenant-agnostic. Tenant isolation belongs in the key generation layer:

```java
@Bean
public KeyGenerator tenantAwareKeyGenerator() {
  return (target, method, params) ->
      TenantContext.current() + ":" + SimpleKey.forArguments(params);
}
```

```java
@Cacheable(cacheNames = "products", keyGenerator = "tenantAwareKeyGenerator")
public Product findById(Long id) { ... }
```

Two operational notes:

- `@CacheEvict(allEntries = true)` is cache-wide, not tenant-scoped. With shared caches and per-tenant keys, clearing the `products` cache evicts every tenant's data. If you need per-tenant clears, use a separate cache name per tenant.
- Tenant context must be available at key-generation time. If your tenant context lives in a `ThreadLocal`, validate that `@Async` methods, scheduled jobs, and background threads have it populated correctly — or those caches will produce wrong-tenant hits.

## Security

The library uses Jackson's polymorphic typing for L2 serialization (so cached objects deserialize to their concrete types, not just `LinkedHashMap`). The same threat model applies to the optional Kryo codec. Both surfaces are **fail-closed by default**: misconfiguration prevents the application from starting rather than shipping a permissive deserializer to production.

### Jackson polymorphic typing (default codec)

```yaml
cache:
  allowed-packages:
    - com.example.domain.     # MUST end with '.'
    - com.example.dto.
```

`cache.allowed-packages` is required — startup fails with `IllegalStateException` if it is empty. Jackson's permissive default-typing pattern has been the basis of 50+ deserialization CVEs (canonical example: CVE-2017-7525), and a library that ships with that pattern as a default would invite RCE in any deployment that forgot to set the property.

**Each entry must end with `.`** (package prefix) or be a fully-qualified class name. Jackson matches via `startsWith`, so `com.example` (no trailing dot) would also accept `com.examplerogue.evil`. The trailing dot enforces the package boundary; the validator rejects entries that don't with a clear error message.

`java.util.`, `java.time.`, and `java.lang.` are always added regardless of user configuration — collections, dates, and primitive wrappers appear in every realistic value graph.

Three remediation paths if the default codec doesn't fit:

1. Set `cache.allowed-packages` to your domain package prefixes (each ending with `.`).
2. Switch to the Kryo codec (`cache.default-spec.codec=KRYO` plus `cache.kryo.registered-classes=[...]`). See below.
3. Provide a custom `RedissonClient` bean with a different codec (see Customization).

### Kryo codec (optional, more compact)

Kryo without class registration accepts arbitrary class names from the payload — the same threat model as Jackson's permissive default typing. The library enforces registration:

```yaml
cache:
  default-spec:
    codec: KRYO
  kryo:
    registered-classes:
      - com.example.domain.Product
      - com.example.domain.User
```

If any cache (default-spec or per-cache) is configured to use KRYO, `cache.kryo.registered-classes` must be non-empty. Each class is loaded via `Class.forName` at startup — a missing class fails the context with the offending name. The resulting `Kryo5Codec` runs with class registration mandatory; payloads referencing an unregistered class cannot be deserialized.

**Order matters.** Kryo assigns numeric registration ids in declaration order. Changing the order of an existing entry changes the wire format and breaks deserialization of payloads written by older deployments. Append new entries to the end; do not reorder.

---

## Customization

All beans are `@ConditionalOnMissingBean`. Override any of them to customize:

### Custom Jackson codec for JPA entities

JPA entities with lazy collections fail to serialize cleanly with default Jackson. Use `Hibernate6Module`:

```java
@Bean
public RedissonClient redissonClient(CacheProperties properties) {
  ObjectMapper mapper = new ObjectMapper()
      .registerModule(new JavaTimeModule())
      .registerModule(new Hibernate6Module()
          .configure(Hibernate6Module.Feature.FORCE_LAZY_LOADING, false))
      .activateDefaultTyping(...);  // configure validator as in CacheConfig
  Config config = new Config();
  config.setCodec(new JsonJacksonCodec(mapper));
  config.useSingleServer().setAddress(properties.server().address());
  return Redisson.create(config);
}
```

The same custom bean is the right place to opt into replica reads if you want them. The library defaults to `ReadMode.MASTER` on cluster and sentinel topologies (see Failure behavior, "Cluster/Sentinel read routing"); override by calling `.setReadMode(ReadMode.SLAVE)` on the `useClusterServers()` or `useSentinelServers()` builder. You give up read-your-writes coherence for higher read throughput and load distribution across replicas — appropriate for caches whose values tolerate brief replication lag (seconds at most under normal conditions, longer during replica catch-up after a network event).

### Custom circuit breaker thresholds

```yaml
resilience4j.circuitbreaker:
  instances:
    redis-cache:
      sliding-window-size: 50
      slow-call-duration-threshold: 200ms
      wait-duration-in-open-state: 60s
```

### Custom `KeyGenerator`

See Multi-tenancy above.

---

## Architecture

The library is a thin set of `org.springframework.cache.Cache` implementations dispatched by a custom `CacheResolver`. Spring's `CacheInterceptor` does the heavy lifting of `@Cacheable` orchestration; this library plugs in below it via Spring's standard extension points. No annotation processing, no aspects, no parallel cache machinery.

### Why Spring's cache abstraction (and not a custom annotation)

Spring's `@Cacheable` already handles SpEL keys, conditions, `unless`, sync, multi-cache resolution, and reactive return types. Implementing the `Cache` interface inherits all of that behavior; introducing a custom annotation would mean reimplementing it. The library's value is in the cache implementation, not the orchestration layer.

### Why Redisson (and not Spring Data Redis directly)?
Spring Boot uses Lettuce by default, but this library intentionally relies on Redisson for four critical architectural reasons:

1. **Safe Distributed Locking (Watchdog):** We rely on Redisson's `RLock`, which includes a background watchdog that automatically extends lock leases while the owning thread is alive. This prevents locks from expiring prematurely during unusually slow database loads. Replicating this with Lettuce requires risky, hardcoded worst-case timeouts and custom Lua scripts.
2. **Zero-Latency Lock Wake-ups:** During high contention (cache stampedes), Redisson uses Redis Pub/Sub to instantly wake up waiting threads the moment a lock is released. A hand-rolled Lettuce lock forces you to use polling intervals (e.g., checking every 50ms), adding unnecessary latency to cache misses.
3. **Seamless Cluster Support:** Redisson's high-level objects transparently handle Redis Cluster complexities, such as `MOVED` and `ASK` redirects. Building custom distributed locks and counters on top of raw Lettuce commands would require writing and maintaining that cluster-handling logic ourselves.
4. **Reliable High-Level Primitives:** Direct access to components like `RBucket`, `RAtomicLong` (for O(1) generation clearing), and `RTopic` keeps the library's codebase clean, focused, and free of the boilerplate required to map raw Redis commands to complex distributed patterns.


### Package layout

```
io.github.nwwarm
  CacheConfig                   — Spring auto-configuration
  CacheProperties               — @ConfigurationProperties + Tier, Codec, Kryo, Health
  CacheKeys                     — key stringification + cache-name and key validation
  HybridCacheManager            — tier-aware Spring CacheManager; per-name dispatch
  InvalidationDispatcher        — single RTopic subscription; self-skip + name routing
  InvalidationMessage           — wire format (key is String — see CacheKeys)
  CodecResolver                 — maps codec enum to Redisson codec instance
  NearCache                     — L1 + L2; publishes/handles invalidations via the dispatcher
  LocalOnlyCache                — Caffeine-only wrapper with metrics
  DistributedOnlyCache          — Redis-only with two-tier single-flight, breaker, generation
  HybridCacheHealthIndicator    — Actuator HealthIndicator (breaker + Redis ping + per-cache stats)
```


## License

Apache License 2.0.
