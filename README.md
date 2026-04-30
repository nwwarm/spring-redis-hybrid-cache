# spring-redis-hybrid-cache

**Multi-layer caching for Spring Boot with cross-node invalidation, stampede protection, and graceful Redis degradation.**

Drop-in replacement for the typical `@Cacheable` + Redis setup that addresses the failure modes most home-grown solutions miss: stale local copies after remote writes, thundering herds on cold keys, and cascading outages when Redis is slow.

```xml
<dependency>
  <groupId>io.github.nwwarm</groupId>
  <artifactId>hybrid-cache-spring-boot-starter</artifactId>
  <version>0.1.0</version>
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
- **Strict TTL semantics.** Generation-counter clear leaves orphaned entries in Redis until they age out via TTL. If your auditing requires "after `clear()`, no entry exists in Redis," this library is the wrong shape — you'd want eager `SCAN` + `DEL` semantics.
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

### Write or evict

```
@CacheEvict / @CachePut / Cache.put method invoked
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
  <version>0.1.0</version>
</dependency>
```

### 2. Configure caches in `application.yml`

```yaml
cache:
  server:
    address: redis://localhost:6379
  allowed-packages:
    - com.example.domain      # security: narrow polymorphic deserialization
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

The library registers a Spring Boot Actuator `HealthIndicator`. `/actuator/health` reports:

- Redis connectivity status
- Circuit breaker state
- Per-cache Caffeine size and hit rate

Useful for Kubernetes readiness probes and monitoring tool health checks.

### Logging

Notable log lines and their operational meaning:

| Log line (severity) | Meaning |
|---|---|
| `WARN  L2 read failed for key '...'; degrading to local-only` | Single L2 read exception. If frequent, breaker will open. |
| `WARN  L2 write failed for key '...'; cross-node incoherence until TTL` | A write succeeded locally but didn't reach Redis. Bounded by TTL. |
| `WARN  Generation bump failed; clear visible only locally` | A `clear()` couldn't update Redis. Other nodes won't see the clear until their generation refreshes (which won't help if Redis is down). |
| `INFO  Polymorphic type validator restricted to packages: [...]` | At startup, confirms the security validator is configured. |
| `WARN  cache.allowed-packages is not configured` | Permissive deserialization is in effect. Configure for production. |

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
| `cache.allowed-packages` | empty (permissive) | Package prefixes allowed for polymorphic deserialization. Set for production. |
| `cache.default-spec.tier` | `NEAR_CACHE` | Default tier for caches not explicitly configured. |
| `cache.default-spec.ttl` | `1h` | Default TTL. |
| `cache.default-spec.maximum-size` | `10000` | Default L1 maximum entries. |
| `cache.default-spec.lock-wait` | `5s` | How long to wait for the cross-node lock before falling through. |
| `cache.default-spec.lock-lease` | `30s` | Lock holder's lease before automatic release. Must exceed worst-case loader latency. |
| `cache.default-spec.codec` | `JSON` | L2 wire format. `JSON` (debuggable) or `KRYO` (compact, faster). |
| `cache.caches.<name>.*` | inherits default-spec | Per-cache overrides. |

---

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

The library uses Jackson's polymorphic typing for L2 serialization (so cached objects deserialize to their concrete types, not just `LinkedHashMap`). The polymorphic type validator is narrowed via `cache.allowed-packages`:

```yaml
cache:
  allowed-packages:
    - com.example.domain
    - com.example.dto
```

Only types whose fully-qualified name starts with one of these prefixes can be deserialized. `java.util.`, `java.time.`, and `java.lang.` are always allowed. Without this configuration, a permissive validator is used and a startup warning is logged. **Configure this for production deployments** — unrestricted polymorphic deserialization has been a vector for past CVEs in the Jackson ecosystem.

For applications with stricter requirements, override the `RedissonClient` bean with a Kryo codec or a custom Jackson configuration. See Customization.

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
  CacheConfig                  — Spring auto-configuration
  CacheProperties              — @ConfigurationProperties + Tier and Codec enums
  PerNameCaffeineCacheManager  — per-name TTL and size for L1
  LocalCacheResolver           — tier dispatch; injects dispatcher into NearCache                                                                                                                                                   
  InvalidationDispatcher       — single RTopic subscription; self-skip + name routing                                                                                                                                               
  InvalidationMessage          — wire format
  CodecResolver                — maps codec enum to Redisson codec instance
  NearCache                    — L1 + L2; publishes/handles invalidations via the dispatcher                                                                                                                                        
  LocalOnlyCache               — Caffeine-only wrapper with metrics
  DistributedOnlyCache         — Redis-only with two-tier single-flight, breaker, generation
```

---

## Roadmap

- **0.1.0** — Initial release. Three tiers, single-flight, circuit breaker, generation-counter clear, Micrometer metrics, health indicator.
- **0.2.0** — TTL jitter on L1 (spreads expiration to reduce coordinated reload spikes), `clearImmediate()` for caches needing eager memory reclaim.
- **0.3.0** — Stale-while-revalidate refresh strategy for hot keys, exposed via per-call mode override.
- **1.0.0** — API stable. No breaking changes without major version bump.

---

## License

Apache License 2.0.