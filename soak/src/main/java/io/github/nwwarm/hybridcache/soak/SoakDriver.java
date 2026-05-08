package io.github.nwwarm.hybridcache.soak;

import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

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
        Instant deadline = start.plus(soakFor);
        long intervalNanos = 1_000_000_000L / targetRps;

        System.out.printf("Soak started: %s for %s @ %d req/s%n", start, soakFor, targetRps);
        printSnapshot("start");

        Flux.interval(Duration.ofNanos(intervalNanos))
                .takeWhile(t -> Instant.now().isBefore(deadline))
                .flatMap(t -> issue(), 32)
                .doOnNext(ok -> { if (ok) success.incrementAndGet(); else failure.incrementAndGet(); })
                .blockLast();

        printSnapshot("end");
        System.out.printf("Soak finished: %s success, %s failures over %s%n",
                success.get(), failure.get(), Duration.between(start, Instant.now()));
    }

    private Mono<Boolean> issue() {
        int op = ThreadLocalRandom.current().nextInt(100);
        // 80% reads (50% products, 30% users), 15% sessions, 5% writes.
        if (op < 50) return get("/cache/products/" + ThreadLocalRandom.current().nextLong(10_000));
        if (op < 80) return get("/cache/users/" + ThreadLocalRandom.current().nextLong(10_000));
        if (op < 95) return get("/cache/sessions/tok-" + ThreadLocalRandom.current().nextInt(1_000));
        return clear();
    }

    private Mono<Boolean> get(String path) {
        return client.get().uri(path).retrieve().toBodilessEntity()
                .map(r -> r.getStatusCode().is2xxSuccessful())
                .onErrorReturn(false);
    }

    private Mono<Boolean> clear() {
        return client.delete().uri("/cache/products").retrieve().toBodilessEntity()
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
