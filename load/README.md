# Load harness

[k6](https://k6.io) scenario against the WebFlux smoke app under `soak/`.

## Run locally

```
# 1. Boot Redis + smoke app (see soak/README.md for the full sequence)
docker run -d --name redis -p 6379:6379 redis:7-alpine
mvn -DskipTests install
mvn -f soak/pom.xml spring-boot:run &

# 2. Run the load scenario
k6 run load/k6-scenario.js
```

## Pass criteria

Reference `DESIGN.md` §11 (Production baselines) for the per-tier
baseline numbers (P50/P99 latency, throughput). The thresholds inside
`k6-scenario.js` are sanity guards — the published baseline is the
canonical source.

## Why k6 over Gatling

Gatling needs a JVM-based scenario runner (`mvn gatling:test` or the
Gatling CLI), which adds dependency surface. k6 is a single binary,
the scenarios are plain JavaScript, and the metric tags on each
request map cleanly to the per-tier rows in §11. Either is fine
operationally; we picked k6 for the lighter footprint.
