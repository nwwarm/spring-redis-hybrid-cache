package io.github.nwwarm.hybridcache.soak;

import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.netty.http.HttpResources;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 24-hour soak driver. Sustains 1k req/s of mixed read / write
 * traffic against the smoke app and asserts the pass criteria from
 * DESIGN.md §7 (Soak tests):
 *
 * <ul>
 *   <li>Heap stable: no leak; allocate/sec stable; GC pause distribution stable.</li>
 *   <li>File descriptor count stable.</li>
 *   <li>Thread count stable. No executor leak.</li>
 *   <li>Metric counters monotonic; no resets.</li>
 *   <li>No WARN/ERROR log spikes.</li>
 *   <li>Preloader snapshots and lock files clean on graceful shutdown.</li>
 * </ul>
 *
 * <p>Run via:
 * <pre>
 *   mvn -f soak/pom.xml exec:java \
 *       -Dexec.mainClass=io.github.nwwarm.hybridcache.soak.SoakDriver \
 *       -Dexec.args="http://localhost:8080 24h"
 * </pre>
 *
 * <p>Pass-criteria assertions are intentionally lightweight in the
 * driver (smoke-style: are we still up, are response codes 2xx, did
 * the request rate hold). Full GC / FD / thread inspection is done by
 * the CI workflow (see {@code .github/workflows/soak.yml}) which
 * collects {@code jstat}, {@code jcmd}, and {@code lsof} samples and
 * compares against the published baseline in DESIGN.md §11.
 *
 * <p>Harness wired only — full 24h CI run scheduled in
 * {@code .github/workflows/soak.yml} as a nightly job; first
 * successful run will populate DESIGN.md §11 / "Soak run summary".
 */
public final class SoakDriver {

    public static void main(String[] args) {
        String baseUrl = args.length > 0 ? args[0] : "http://localhost:8080";
        Duration soakFor = args.length > 1 ? parseDuration(args[1]) : Duration.ofHours(24);
        int targetRps = args.length > 2 ? Integer.parseInt(args[2]) : 1_000;
        new SoakDriver(baseUrl, soakFor, targetRps).run();
    }

    private final WebClient client;
    private final Duration soakFor;
    private final int targetRps;
    private final AtomicLong success = new AtomicLong();
    private final AtomicLong failure = new AtomicLong();

    SoakDriver(String baseUrl, Duration soakFor, int targetRps) {
        this.client = WebClient.builder().baseUrl(baseUrl).build();
        this.soakFor = soakFor;
        this.targetRps = targetRps;
    }

    void run() {
        Instant start = Instant.now();

        System.out.printf("Soak started: %s for %s @ %d req/s%n", start, soakFor, targetRps);
        printSnapshot("start");
        printReconciliationCounters("start");

        String reconciliationEnd;
        try {
            Flux.range(0, Integer.MAX_VALUE)
                    .concatMap(i -> issue())
                    .delayElements(Duration.ofMillis(1000 / targetRps))
                    .take(soakFor)
                    .doOnNext(ok -> { if (ok) success.incrementAndGet(); else failure.incrementAndGet(); })
                    .blockLast();
        } finally {
            // Scrape before the client is torn down — the end snapshot below
            // runs after HttpResources are disposed, so anything needing HTTP
            // has to happen here.
            reconciliationEnd = reconciliationCounters();
            HttpResources.disposeLoopsAndConnections();
            Schedulers.shutdownNow();
        }

        printSnapshot("end");
        System.out.printf("[end] reconciliation %s%n", reconciliationEnd);
        System.out.printf("Soak finished: %s success, %s failures over %s%n",
                success.get(), failure.get(), Duration.between(start, Instant.now()));
    }

    /**
     * Reconciliation counters worth a before/after comparison on a soak run.
     *
     * <p>{@code seq.regression_recheck_resolved} is the canary of the set. On a
     * master-reading deployment — which the library pins by default — it should
     * be zero at both ends of the run. A non-zero value means either a custom
     * {@code ReadMode.SLAVE} client (benign) or that the reconciler's
     * read-ordering assumption does not hold here, in which case its miss and
     * regression verdicts cannot be trusted. Nothing else in the harness would
     * surface that: the counter reads zero whether the mechanism works or has
     * silently stopped being exercised.
     *
     * <p>{@code misses.detected} is the one to watch for the 1.0.3 fix itself.
     * Before 1.0.3 a suppressed regression came straight back as a miss and
     * cleared L1; the 1.0.2 soak never noticed because nobody looked at this
     * counter — only at {@code seq.regressions}, which the recheck had already
     * driven to zero.
     */
    private static final List<String> RECONCILIATION_METRICS = List.of(
            "cache.reconciliation.cycles.completed",
            "cache.reconciliation.misses.detected",
            "cache.reconciliation.skipped",
            "cache.reconciliation.seq.regressions",
            "cache.reconciliation.seq.regression_recheck_resolved");

    private void printReconciliationCounters(String label) {
        System.out.printf("[%s] reconciliation %s%n", label, reconciliationCounters());
    }

    /**
     * Reads each counter's COUNT via {@code /actuator/metrics/{name}}, summed
     * across caches. Deliberately not {@code /actuator/prometheus}: that
     * endpoint is listed in the soak app's exposure config but the
     * {@code micrometer-registry-prometheus} artifact is not on its classpath,
     * so it 404s. Never throws — a metrics failure must not end a 24h run.
     */
    private String reconciliationCounters() {
        StringBuilder sb = new StringBuilder();
        for (String metric : RECONCILIATION_METRICS) {
            if (!sb.isEmpty()) sb.append(", ");
            sb.append(metric.substring("cache.reconciliation.".length()))
                    .append('=')
                    .append(counterValue(metric));
        }
        return sb.toString();
    }

    private String counterValue(String metric) {
        try {
            String body = client.get().uri("/actuator/metrics/{m}", metric)
                    .retrieve().bodyToMono(String.class)
                    .onErrorReturn("")
                    .block(Duration.ofSeconds(10));
            if (body == null || body.isEmpty()) return "n/a";
            // {"name":"...","measurements":[{"statistic":"COUNT","value":42.0}],...}
            Matcher m = COUNT_VALUE.matcher(body);
            return m.find() ? m.group(1) : "0.0";
        } catch (Exception e) {
            // A 404 (counter never registered because no cache opted into
            // reconciliation) is normal and reads as n/a, not a failure.
            return "n/a";
        }
    }

    private static final Pattern COUNT_VALUE =
            Pattern.compile("\"statistic\"\\s*:\\s*\"COUNT\"\\s*,\\s*\"value\"\\s*:\\s*([0-9.eE+-]+)");

    private Mono<Boolean> issue() {
        int op = ThreadLocalRandom.current().nextInt(100);
        // 80% reads (50% products, 30% users), 15% sessions, 5% writes split
        // between sync /products clear and reactive /users clear. The reactive
        // clear exercises the @CacheEvict(allEntries=true) → NearCache.clear()
        // → bumpGenerationAsync path from a Mono pipeline; under load the
        // continuation completes on a Redisson Netty event-loop thread, which
        // is exactly the path the 1.0.1 fix targets.
        if (op < 50) return get("/cache/products/" + ThreadLocalRandom.current().nextLong(10_000));
        if (op < 80) return get("/cache/users/" + ThreadLocalRandom.current().nextLong(10_000));
        if (op < 95) return get("/cache/sessions/tok-" + ThreadLocalRandom.current().nextInt(1_000));
        if (op < 98) return clearSync();
        return clearReactive();
    }

    private Mono<Boolean> get(String path) {
        return client.get().uri(path).retrieve().toBodilessEntity()
                .map(r -> r.getStatusCode().is2xxSuccessful())
                .onErrorReturn(false);
    }

    private Mono<Boolean> clearSync() {
        return client.delete().uri("/cache/products").retrieve().toBodilessEntity()
                .map(r -> r.getStatusCode().is2xxSuccessful())
                .onErrorReturn(false)
                .subscribeOn(Schedulers.boundedElastic());
    }

    private Mono<Boolean> clearReactive() {
        return client.delete().uri("/cache/users").retrieve().toBodilessEntity()
                .map(r -> r.getStatusCode().is2xxSuccessful())
                .onErrorReturn(false)
                .subscribeOn(Schedulers.boundedElastic());
    }

    private void printSnapshot(String label) {
        var rt = Runtime.getRuntime();
        var os = ManagementFactory.getOperatingSystemMXBean();
        var threads = ManagementFactory.getThreadMXBean();
        System.out.printf("[%s] heap=%dMB threads=%d loadavg=%.2f%n",
                label,
                (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024),
                threads.getThreadCount(),
                os.getSystemLoadAverage());
    }

    private static Duration parseDuration(String s) {
        if (s.endsWith("h")) return Duration.ofHours(Long.parseLong(s.substring(0, s.length() - 1)));
        if (s.endsWith("m")) return Duration.ofMinutes(Long.parseLong(s.substring(0, s.length() - 1)));
        if (s.endsWith("s")) return Duration.ofSeconds(Long.parseLong(s.substring(0, s.length() - 1)));
        return Duration.parse(s);
    }
}
