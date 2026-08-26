# Changelog

## 1.0.3

### Fixed
- Reconciliation declared false misses under concurrent writes, each
  one clearing L1. Three distinct defects in `reconcile()`, all in the
  same read-ordering family, all invisible to the existing tests:

  1. The Redis seq GET is now bracketed by two `lastObservedSeq`
     samples. Regression is judged against the pre-GET sample — the
     only one that can prove genuine counter loss, since every value
     the watermark holds was durable in Redis before it was written
     locally — and miss against the post-GET sample. A publish landing
     while the GET is in flight used to read as a regression; it is
     now correctly no-miss.

  2. The 1.0.2 recheck GET is bracketed the same way. Previously it
     resolved the suspected regression and then re-classified a *fresh*
     recheck value against a watermark snapshot taken *before* the
     recheck round-trip, so every publish that landed during that
     round-trip counted as missed. This was the dominant cost: nearly
     every suppressed regression came straight back as a false miss.
     Measured on `ReconcilerDetectionIT` at 1.0.2, ~11k false misses
     in 5s against a regression counter reading zero.

  3. The MISS branch advances the watermark with
     `accumulateAndGet(redisSeq, Math::max)` rather than `set`. A
     publisher self-bump landing between the GET and the recovery was
     discarded, walking the watermark backwards and producing a
     spurious miss — and a second, needless L1 clear — on the next
     cycle. This is the cascade the 1.0.2 entry describes; 1.0.2 fixed
     the classification but not the non-monotonic write that caused it.

  No API, metric, config, or wire-format change. The observable effect
  is strictly fewer false warnings and fewer false L1 clears.

  Why 1.0.2 looked clean: `cache.reconciliation.seq.regressions` was
  the only asserted counter, and the recheck had already driven it to
  zero. Nothing looked at `misses.detected`.

### Documentation
- Corrected metric names in README and DESIGN that never matched the
  code, so anything built from them queried series that do not exist:
  `cache.reconciliation.cycles` → `cycles.completed`,
  `cache.reconciliation.misses` → `misses.detected`,
  `cache.swr.refresh.failures` → `cache.swr.refreshes{status=failed}`,
  `cache.refresh.ahead.fires`/`.failures` →
  `cache.refresh_ahead.refreshes{status=started|failed}`, and the
  `cache.reconciliation.skipped` tag value `breaker_open` →
  `breaker-open`. Names are unchanged in code; only the docs were wrong.
- Documented `cache.reconciliation.seq.regression_recheck_resolved`,
  which has been emitted since 1.0.2 but appeared in no table. It reads
  zero whether the mechanism works or has silently stopped being
  exercised, so it needs an explicit interpretation: on a
  master-reading deployment (what the library pins) it should stay at
  zero, and non-zero means either a custom `ReadMode.SLAVE` client or
  that the reconciler's read-ordering invariant does not hold and its
  verdicts cannot be trusted.
- DESIGN §2 now specifies the bracketed-read algorithm rather than the
  superseded two-step compare that `reconcile()`'s javadoc cites.

### Testing
- `ReconciliationWatermarkRaceTest`: deterministic coverage for all
  three fixes, placing a publisher bump at exact instruction boundaries
  inside the cycle via injected hooks. Each test verified to fail
  against the 1.0.2 tree.
- Added a dedicated test for the recheck resolve path. Mutation check
  showed its only prior coverage was incidental — a side effect of the
  false-miss storm this release fixes — so removing the storm would
  have left an unreachable-by-construction branch with a
  zero-either-way metric and no test.
- `ReconcilerDetectionIT` / `DistributedOnlyReconcilerIT` now assert
  the misses counter alongside regressions; asserting only the latter
  is what hid defect 2 for a full release.
- Soak harness snapshots the reconciliation counters (driver
  start/end, workflow 30-minute trajectory). None were captured
  before, and `/actuator/metrics` with no name returns only the metric
  name index, so the existing artefact never contained a value.

## 1.0.2

### Fixed
- Reconciler's two reads (Redis seq, local watermark) were not
  atomic. Under sustained concurrent writes, the local watermark
  could be observed strictly greater than the Redis seq snapshot
  from the same cycle, causing a false REGRESSION classification
  and an unnecessary watermark reset. The watermark reset
  cascaded into a false L1 clear on the following cycle when the
  reset value fell more than `tolerance` below the new Redis
  seq. Surfaced in the 24h soak that validated 1.0.1: four false
  regressions in 16 minutes under 1000 req/s, no genuine state
  loss.

  Fix: on suspected regression, re-read Redis through the same
  circuit breaker. If the recheck catches up to the local
  watermark, re-classify and proceed. If the recheck still
  shows the lower value, fall through to the existing
  counter-deleted handler (warning + watermark reset).
  Same change applied to DistributedOnlyCache.

### Added
- Metric `cache.reconciliation.seq.regression_recheck_resolved`:
  count of suspected regressions that were race artifacts rather
  than genuine counter loss. Operators can compare this against
  `cache.reconciliation.seq.regressions` to see the rate of
  benign races vs. real recovery events.

- Covered the reconciler's breaker-open and Redis-error paths with
  regression tests; no production code change. The 1.0.2 recheck
  added two new catches per cache class (breaker-open and generic
  Exception on the recheck read), and the pre-existing first-read
  catches on DistributedOnlyCache had been uncovered since
  introduction. All seven net-new failure-mode paths are now
  exercised via Mockito (RedisTimeoutException injection,
  `CircuitBreaker.transitionToOpenState()` for the breaker-open
  path) with assertions on the `cache.reconciliation.skipped`
  counter (tag `reason` = `breaker-open` or `exception`), absence
  of state changes, and absence of the cascade "Counter likely
  deleted by operator" warning.

## 1.0.1

- Removed an unreachable defensive catch for CallNotPermittedException
  in clearImmediate; the breaker-open path is handled through the
  async chain's .exceptionally handler (Resilience4j 2.4.0's
  executeCompletionStage never throws synchronously). No behavior
  change.

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
