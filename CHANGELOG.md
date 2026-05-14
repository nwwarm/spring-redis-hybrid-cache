# Changelog

## 1.0.1

### Fixed
- Sync Redisson API call in `NearCache.writeToL2` could throw
  `IllegalStateException("Sync methods can't be invoked from
  async/rx/reactive listeners")` when invoked from a Reactor pipeline
  whose preceding Redis op completed on a Netty event-loop thread.
  Caught by a 24-hour soak at 1000 req/s. Replaced sync calls with
  async variants and composed them into the surrounding
  `CompletionStage` chain; the chain is awaited on regular worker
  threads (preserving the 1.0.0 sync contract for non-reactive
  callers) and runs fire-and-forget when invoked from a Redisson
  Netty thread (where a sync `.join()` would deadlock).

  Audit pass on all Redisson call sites covered: `RBucket.set`,
  `RBucket.delete`, `RTopic.publish`, `RAtomicLong.incrementAndGet`
  in `NearCache` and `DistributedOnlyCache`. The same fix was
  applied to the mirrored `DistributedOnlyCache` write path.
  `RBucket.get` in the sync `readFromL2` path is left sync — that
  path is only reachable from synchronous Spring Cache callers
  (`@Cacheable` on non-reactive return types), which never run on a
  Netty thread. `clearImmediate`'s sync `incrementAndGet` and
  `unlinkByPattern` and the once-per-second generation refresh
  inside `currentGeneration` are left sync with TODO comments —
  they were not observed to fire in the soak and the existing
  `GenerationRefreshConcurrencyTest` assertions on sync
  `RAtomicLong.get` call counts would require updating.

  Regression coverage: `NearCacheEventLoopWriteIT` schedules
  `put(...)` onto Redisson's `EventLoopGroup` directly and asserts
  that L2 receives the value and no `cache.l2.failures` are
  recorded.

## 1.0.0
- Initial release.
