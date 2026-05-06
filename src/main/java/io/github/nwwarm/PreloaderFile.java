package io.github.nwwarm;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Preloader snapshot file format — atomic writer + lenient parser.
 *
 * <p>See {@code docs/preloader-design.md} §3 for the format. Single header
 * line plus newline-delimited stringified keys, UTF-8.
 *
 * <p>Stateless utility: every method takes its inputs explicitly. The
 * {@link NearCachePreloader} layer owns scheduling, generation comparison,
 * and metric updates; this class is concerned only with bytes-on-disk and
 * the wire format.
 *
 * <p>Forward compatibility: a {@code v1} reader rejects any header that does
 * not start with the exact literal {@link #HEADER_PREFIX}. Future versions
 * bump the literal; mixed-version rolling upgrades therefore lose preloader
 * benefit for the rollout window but never produce corrupt state.
 */
final class PreloaderFile {

    private static final Logger log = LoggerFactory.getLogger(PreloaderFile.class);

    /** The canonical prefix; trailing space is significant. */
    static final String HEADER_PREFIX = "# hybrid-cache preloader v1 ";

    /**
     * Header layout. Cache name is matched as {@code \S+} — names with
     * whitespace are unsupported by the preloader and surface as
     * {@link ParseError} on load. {@code CacheKeys.validateCacheName} does
     * not reject whitespace today; in practice cache names are
     * hyphens/underscores so the constraint costs nothing.
     */
    private static final Pattern HEADER_PATTERN = Pattern.compile(
            "^\\Q" + HEADER_PREFIX + "\\Ecache=(\\S+) generation=(\\d+) writtenAt=(\\S+)$");

    /** Owner-only file permissions — POSIX {@code 0600}. */
    static final Set<PosixFilePermission> FILE_PERMS = PosixFilePermissions.fromString("rw-------");

    /** Result of a {@link #load(Path)} call. */
    sealed interface LoadResult permits Loaded, Missing, ParseError {}

    /**
     * Parsed snapshot. {@link #skippedLines} is the count of body lines that
     * failed validation ({@code :}, over-length, etc.). The caller surfaces
     * a single WARN once if {@code skippedLines > 0} — see design §6.
     */
    record Loaded(String cacheName, long generation, Instant writtenAt,
                  List<String> keys, int skippedLines) implements LoadResult {}

    /** No file at the given path. First-ever boot, or after manual delete. */
    record Missing() implements LoadResult {
        static final Missing INSTANCE = new Missing();
    }

    /** Header missing/wrong-version, IO error, or other malformed input. */
    record ParseError(String reason) implements LoadResult {}

    private PreloaderFile() {}

    /**
     * Atomically writes a snapshot to {@code file}.
     *
     * <p>Writes to {@code <file>.tmp}, fsync's, then {@code Files.move(...,
     * ATOMIC_MOVE, REPLACE_EXISTING)} swaps it into place. A {@code .tmp}
     * left over from a crashed prior run is overwritten — no recovery
     * needed.
     *
     * <p>Per-key validation on store: keys containing {@code \n}, {@code \r},
     * {@code :}, or exceeding {@link CacheKeys#MAX_KEY_BYTES} after UTF-8
     * encoding are dropped silently (debug log). These reflect upstream
     * {@code CacheKeys.stringify} rules; their presence here is operator
     * error or a user-supplied {@code KeyGenerator} that bypasses the
     * stringify path.
     *
     * @param maxKeys hard cap on lines written. {@code null} or {@code <= 0}
     *                means no cap. Truncation order is whatever order the
     *                {@code keys} iterable produces — we make no promise.
     */
    static void writeAtomic(Path file, String cacheName, long generation,
                            Iterable<String> keys, Integer maxKeys) throws IOException {
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        Files.deleteIfExists(tmp);

        int cap = (maxKeys != null && maxKeys > 0) ? maxKeys : Integer.MAX_VALUE;
        try (BufferedWriter writer = Files.newBufferedWriter(tmp,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            writer.write(HEADER_PREFIX);
            writer.write("cache=" + cacheName);
            writer.write(" generation=" + generation);
            writer.write(" writtenAt=" + Instant.now());
            writer.write('\n');

            int written = 0;
            int droppedNewline = 0, droppedColon = 0, droppedLength = 0;
            for (String key : keys) {
                if (written >= cap) break;
                if (key == null) continue;
                if (key.indexOf('\n') >= 0 || key.indexOf('\r') >= 0) {
                    droppedNewline++;
                    continue;
                }
                if (key.indexOf(':') >= 0) {
                    droppedColon++;
                    continue;
                }
                if (key.getBytes(StandardCharsets.UTF_8).length > CacheKeys.MAX_KEY_BYTES) {
                    droppedLength++;
                    continue;
                }
                writer.write(key);
                writer.write('\n');
                written++;
            }
            writer.flush();
            if (droppedNewline + droppedColon + droppedLength > 0) {
                log.debug("Cache '{}' preloader: skipped invalid keys on store"
                                + " — newline:{}, colon:{}, length:{}",
                        cacheName, droppedNewline, droppedColon, droppedLength);
            }
        }

        // fsync the .tmp before the rename — without this, the rename can
        // succeed and a power loss leaves us with a zero-byte destination
        // even though the rename itself was atomic. fsync says "the bytes
        // are durable on disk."
        try (java.nio.channels.FileChannel ch = java.nio.channels.FileChannel.open(tmp,
                StandardOpenOption.WRITE)) {
            ch.force(true);
        }

        // POSIX 0600. On non-POSIX (Windows), the operation throws
        // UnsupportedOperationException — silently swallowed; permissions
        // there fall back to the parent ACL, documented as such on the
        // property javadoc.
        applyOwnerOnlyFilePerms(tmp);

        try {
            Files.move(tmp, file,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (FileSystemException e) {
            // Some filesystems (notably some FUSE mounts) reject
            // ATOMIC_MOVE; fall back to a non-atomic move so we still
            // make progress, with a debug log so this isn't invisible.
            log.debug("Atomic move not supported for {}; falling back to REPLACE_EXISTING",
                    file, e);
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /**
     * Reads a snapshot file and returns either {@link Loaded},
     * {@link Missing}, or {@link ParseError}. Never throws.
     *
     * <p>Tolerance rules (from §6 of the design doc):
     * <ul>
     *   <li>Missing file → {@link Missing}.</li>
     *   <li>Empty file, header malformed/wrong-version, header generation
     *       not a long, header writtenAt not parsable → {@link ParseError}
     *       with a reason naming the failure.</li>
     *   <li>Body lines: empty lines are tolerated and skipped. Lines
     *       containing {@code :} or exceeding the byte limit are skipped
     *       and counted in {@link Loaded#skippedLines}.</li>
     *   <li>File truncated mid-write (no trailing newline) → the partial
     *       last line is dropped; the rest loads.</li>
     * </ul>
     */
    static LoadResult load(Path file) {
        if (!Files.exists(file)) return Missing.INSTANCE;
        String content;
        try {
            content = Files.readString(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            return new ParseError("readString failed: " + e.getClass().getSimpleName()
                    + ": " + e.getMessage());
        }
        if (content.isEmpty()) return new ParseError("file is empty");

        // split with -1 keeps trailing empties so we can spot the
        // clean-vs-truncated termination case.
        String[] rawLines = content.split("\n", -1);
        // Always drop the last element. It is either:
        //   (a) the trailing empty after a clean "...\n" terminator, or
        //   (b) the partial last line of a truncated write.
        // In both cases the element does not represent a complete record.

        String headerLine = rawLines[0];
        Matcher m = HEADER_PATTERN.matcher(headerLine);
        if (!m.matches()) {
            return new ParseError("header missing or wrong version: "
                    + abbreviate(headerLine));
        }
        String cacheName = m.group(1);
        long generation;
        try {
            generation = Long.parseLong(m.group(2));
        } catch (NumberFormatException e) {
            return new ParseError("header generation not a long: " + m.group(2));
        }
        Instant writtenAt;
        try {
            writtenAt = Instant.parse(m.group(3));
        } catch (DateTimeParseException e) {
            return new ParseError("header writtenAt not parsable: " + m.group(3));
        }

        List<String> keys = new ArrayList<>(Math.max(16, rawLines.length - 2));
        int skipped = 0;
        int lastIndex = rawLines.length - 1;
        for (int i = 1; i < lastIndex; i++) {
            String line = rawLines[i];
            if (line.isEmpty()) continue;
            if (line.indexOf(':') >= 0
                    || line.getBytes(StandardCharsets.UTF_8).length > CacheKeys.MAX_KEY_BYTES) {
                skipped++;
                continue;
            }
            keys.add(line);
        }
        return new Loaded(cacheName, generation, writtenAt, keys, skipped);
    }

    private static void applyOwnerOnlyFilePerms(Path path) {
        try {
            Files.setPosixFilePermissions(path, FILE_PERMS);
        } catch (UnsupportedOperationException e) {
            // Non-POSIX filesystem (Windows). Documented as inheriting the
            // parent ACL on those platforms.
        } catch (IOException e) {
            log.debug("Failed to set 0600 on {}; continuing", path, e);
        }
    }

    private static String abbreviate(String s) {
        if (s == null) return "<null>";
        if (s.length() <= 80) return s;
        return s.substring(0, 80) + "...";
    }
}
