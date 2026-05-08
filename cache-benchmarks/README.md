# cache-benchmarks

JMH microbenchmarks for `hybrid-cache-spring-boot-starter`.

This module is a **sibling** Maven project (not part of any reactor pom).
JMH benchmarks are slow and we do not want them on the default
`mvn verify` path.

## Run locally

```
# from repo root
mvn -DskipTests install
mvn -f cache-benchmarks/pom.xml verify
java -jar cache-benchmarks/target/benchmarks.jar -rf json -rff results.json
```

## Categories

| Class | Covers |
|---|---|
| `L1HitBenchmark` | L1 (Caffeine) hit latency, single-threaded and 10-thread contention. |
| `L2HitBenchmark` | L2 (Redis) hit latency against Testcontainers Redis on localhost. |
| `SingleFlightBenchmark` | N concurrent threads on a cold key; loader-invocation count and wall-clock to first hit. |
| `SwrBenchmark` | SWR fresh-hit fast path and stale-hit + async refresh dispatch overhead. |
| `RefreshAheadDispatchBenchmark` | XFetch predicate evaluation cost on the hot read path. |
| `AsyncRetrieveBenchmark` | Async path L1 hit, L2 hit, and loader-hop scenarios. |
| `ReconciliationCycleBenchmark` | Cycle comparison cost at 1k / 10k / 100k / 1M caches. |

## Regression baselines

Published in `DESIGN.md` §11 (Production baselines). CI compares each PR's
JMH output against the pinned baseline; a regression past the per-benchmark
threshold posts a non-blocking comment on the PR. The 1.0.0 trigger
criteria in `DESIGN.md` §8 elevate these to blocking gates.
