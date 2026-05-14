# Changelog

## 1.0.1

### Fixed
- Sync Redisson API calls reachable from Reactor pipelines /
  `CompletableFuture` completions running on Netty event-loop threads
  were throwing
  `IllegalStateException("Sync methods can't be invoked from async/rx/
  reactive listeners")` under sustained load. Affected paths:
  `NearCache.writeToL2` and the corresponding hot-path `put`/`evict`
  (surfaced in 3.5h of the initial soak); `NearCache.clearImmediate`,
  `DistributedOnlyCache.clearImmediate`, and the `currentGeneration`
  refresh in both classes (theoretically reachable via Spring reactive
  `@CacheEvict` and async write composition, fixed in the same patch
  to address the class of bug rather than the single observed
  instance).

  All converted call sites use the existing
  `awaitUnlessOnEventLoop(chain)` shape: the chain is awaited on
  regular worker threads (preserving the 1.0.0 sync contract for
  non-reactive callers and existing tests) and runs fire-and-forget
  when invoked from a Redisson Netty thread (where a sync `.join()`
  would deadlock the I/O thread that completes the chain).

  Regression coverage: `NearCacheEventLoopWriteIT` covers the hot
  path; `NearCacheEventLoopClearImmediateIT` covers
  `clearImmediate` for both `NearCache` and `DistributedOnlyCache`.
  Both schedule the call directly onto Redisson's `EventLoopGroup`
  and assert no `IllegalStateException` and no `cache.l2.failures`.

### Changed
- `clearImmediate` is now asynchronous internally. The `void`
  signature is preserved; on regular worker threads the call still
  blocks until the bump + UNLINK chain completes (so the "no orphan
  keys when this returns" contract holds for non-reactive callers).
  Failures continue to surface via `l2Failures` /
  `cache.distributed.failures` and `log.warn`, but from completion
  callbacks rather than a `try`/`catch` on the calling thread —
  which means `clearImmediate` no longer throws the originating
  `RuntimeException`. Callers that relied on the throw to detect
  bump failure should observe the failure metric instead. (No
  internal callers do.)
- `currentGeneration` refresh is now eventually-consistent. The
  CAS-winning thread dispatches `getAsync` and returns the cached
  `localGeneration` immediately; subsequent callers observe the
  refreshed value. Refresh cadence (one round-trip per
  `GENERATION_REFRESH_NANOS` window per cache) and per-cache call
  frequency are unchanged. Worst-case staleness is bounded by the
  same window already accepted by the cached-generation contract —
  in practice each cache observes at most one extra stale read per
  ~1s window.

## 1.0.0
- Initial release.
