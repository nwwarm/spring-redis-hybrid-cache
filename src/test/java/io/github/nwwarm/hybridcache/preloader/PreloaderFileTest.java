package io.github.nwwarm.hybridcache.preloader;

import io.github.nwwarm.hybridcache.core.CacheKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit coverage of the on-disk preloader file format. Pure file-IO; no
 * Spring, no Redis, no `NearCache`.
 *
 * <p>Mirrors the test plan in {@code docs/preloader-design.md} §9 unit
 * section: round-trip, every header-corruption case, body-corruption
 * count, atomic-write crash recovery.
 */
class PreloaderFileTest {

    @Test
    void roundTrip_writesHeaderAndKeys_loadReturnsSame(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("snap");
        List<String> keys = List.of("k1", "k2", "k3-with-dashes", "𝄞-unicode");
        PreloaderFile.writeAtomic(file, "products", 7L, keys, null);

        PreloaderFile.LoadResult result = PreloaderFile.load(file);
        assertThat(result).isInstanceOf(PreloaderFile.Loaded.class);
        PreloaderFile.Loaded loaded = (PreloaderFile.Loaded) result;
        assertThat(loaded.cacheName()).isEqualTo("products");
        assertThat(loaded.generation()).isEqualTo(7L);
        assertThat(loaded.keys()).containsExactlyElementsOf(keys);
        assertThat(loaded.skippedLines()).isZero();
        assertThat(loaded.writtenAt()).isNotNull();
    }

    @Test
    void roundTrip_zeroKeys_loadsEmpty(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("snap");
        PreloaderFile.writeAtomic(file, "empty", 0L, List.of(), null);

        PreloaderFile.LoadResult result = PreloaderFile.load(file);
        assertThat(result).isInstanceOf(PreloaderFile.Loaded.class);
        PreloaderFile.Loaded loaded = (PreloaderFile.Loaded) result;
        assertThat(loaded.keys()).isEmpty();
        assertThat(loaded.skippedLines()).isZero();
    }

    @Test
    void load_missingFile_returnsMissing(@TempDir Path tmp) {
        Path file = tmp.resolve("does-not-exist");
        PreloaderFile.LoadResult result = PreloaderFile.load(file);
        assertThat(result).isInstanceOf(PreloaderFile.Missing.class);
    }

    @Test
    void load_emptyFile_returnsParseError(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("snap");
        Files.writeString(file, "");

        PreloaderFile.LoadResult result = PreloaderFile.load(file);
        assertThat(result).isInstanceOf(PreloaderFile.ParseError.class);
        assertThat(((PreloaderFile.ParseError) result).reason()).contains("empty");
    }

    @Test
    void load_missingHeader_returnsParseError(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("snap");
        Files.writeString(file, "k1\nk2\nk3\n");

        PreloaderFile.LoadResult result = PreloaderFile.load(file);
        assertThat(result).isInstanceOf(PreloaderFile.ParseError.class);
        assertThat(((PreloaderFile.ParseError) result).reason())
                .contains("header missing or wrong version");
    }

    @Test
    void load_wrongVersion_returnsParseError(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("snap");
        Files.writeString(file, "# hybrid-cache preloader v2 cache=foo generation=0 writtenAt=2025-01-01T00:00:00Z\n");

        PreloaderFile.LoadResult result = PreloaderFile.load(file);
        assertThat(result).isInstanceOf(PreloaderFile.ParseError.class);
        assertThat(((PreloaderFile.ParseError) result).reason())
                .contains("header missing or wrong version")
                .contains("v2");
    }

    @Test
    void load_headerWithBadGeneration_returnsParseError(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("snap");
        // Generation isn't a number — the regex requires \d+, so this
        // surfaces as a header-mismatch ParseError (matches the design's
        // "header malformed" bucket).
        Files.writeString(file, "# hybrid-cache preloader v1 cache=foo generation=NOT_A_NUMBER writtenAt=2025-01-01T00:00:00Z\n");

        PreloaderFile.LoadResult result = PreloaderFile.load(file);
        assertThat(result).isInstanceOf(PreloaderFile.ParseError.class);
        assertThat(((PreloaderFile.ParseError) result).reason())
                .contains("header missing or wrong version");
    }

    @Test
    void load_headerWithBadWrittenAt_returnsParseError(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("snap");
        Files.writeString(file, "# hybrid-cache preloader v1 cache=foo generation=5 writtenAt=not-an-instant\n");

        PreloaderFile.LoadResult result = PreloaderFile.load(file);
        assertThat(result).isInstanceOf(PreloaderFile.ParseError.class);
        assertThat(((PreloaderFile.ParseError) result).reason())
                .contains("writtenAt not parsable");
    }

    @Test
    void load_bodyWithColon_skipsAndCounts(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("snap");
        Files.writeString(file,
                "# hybrid-cache preloader v1 cache=foo generation=0 writtenAt=2025-01-01T00:00:00Z\n"
                + "good1\n"
                + "bad:key\n"
                + "good2\n"
                + "another:bad\n"
                + "good3\n");

        PreloaderFile.LoadResult result = PreloaderFile.load(file);
        assertThat(result).isInstanceOf(PreloaderFile.Loaded.class);
        PreloaderFile.Loaded loaded = (PreloaderFile.Loaded) result;
        assertThat(loaded.keys()).containsExactly("good1", "good2", "good3");
        assertThat(loaded.skippedLines()).isEqualTo(2);
    }

    @Test
    void load_bodyWithOverLengthLine_skipsAndCounts(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("snap");
        String tooLong = "a".repeat(CacheKeys.MAX_KEY_BYTES + 1);
        Files.writeString(file,
                "# hybrid-cache preloader v1 cache=foo generation=0 writtenAt=2025-01-01T00:00:00Z\n"
                + "good\n"
                + tooLong + "\n"
                + "good2\n");

        PreloaderFile.LoadResult result = PreloaderFile.load(file);
        PreloaderFile.Loaded loaded = (PreloaderFile.Loaded) result;
        assertThat(loaded.keys()).containsExactly("good", "good2");
        assertThat(loaded.skippedLines()).isEqualTo(1);
    }

    @Test
    void load_emptyLines_areTolerated(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("snap");
        Files.writeString(file,
                "# hybrid-cache preloader v1 cache=foo generation=0 writtenAt=2025-01-01T00:00:00Z\n"
                + "k1\n"
                + "\n"
                + "k2\n"
                + "\n"
                + "\n"
                + "k3\n");

        PreloaderFile.LoadResult result = PreloaderFile.load(file);
        PreloaderFile.Loaded loaded = (PreloaderFile.Loaded) result;
        assertThat(loaded.keys()).containsExactly("k1", "k2", "k3");
        assertThat(loaded.skippedLines()).isZero();
    }

    @Test
    void load_truncatedMidWrite_dropsPartialLastLine(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("snap");
        // No trailing newline → last line is partial; spec says drop only it.
        Files.writeString(file,
                "# hybrid-cache preloader v1 cache=foo generation=0 writtenAt=2025-01-01T00:00:00Z\n"
                + "k1\n"
                + "k2\n"
                + "partial-no-newline");

        PreloaderFile.LoadResult result = PreloaderFile.load(file);
        PreloaderFile.Loaded loaded = (PreloaderFile.Loaded) result;
        assertThat(loaded.keys()).containsExactly("k1", "k2");
    }

    @Test
    void writeAtomic_keysWithNewline_areDropped(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("snap");
        List<String> keys = List.of("good1", "bad\nwith\nnewlines", "good2", "bad\rwith\rcr");
        PreloaderFile.writeAtomic(file, "foo", 0, keys, null);

        PreloaderFile.LoadResult result = PreloaderFile.load(file);
        PreloaderFile.Loaded loaded = (PreloaderFile.Loaded) result;
        assertThat(loaded.keys()).containsExactly("good1", "good2");
    }

    @Test
    void writeAtomic_keysWithColon_areDropped(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("snap");
        List<String> keys = List.of("good", "with:colon", "also-good");
        PreloaderFile.writeAtomic(file, "foo", 0, keys, null);

        PreloaderFile.LoadResult result = PreloaderFile.load(file);
        PreloaderFile.Loaded loaded = (PreloaderFile.Loaded) result;
        assertThat(loaded.keys()).containsExactly("good", "also-good");
    }

    @Test
    void writeAtomic_maxKeysCap_truncatesIteration(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("snap");
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 100; i++) keys.add("k" + i);
        PreloaderFile.writeAtomic(file, "foo", 0, keys, 10);

        PreloaderFile.LoadResult result = PreloaderFile.load(file);
        PreloaderFile.Loaded loaded = (PreloaderFile.Loaded) result;
        assertThat(loaded.keys()).hasSize(10);
        // Order is iteration-order from the source — design says we don't
        // guarantee an order, but for this list we know it's k0..k9.
        assertThat(loaded.keys()).containsExactly(
                "k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8", "k9");
    }

    @Test
    void writeAtomic_overwritesExistingFile(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("snap");
        PreloaderFile.writeAtomic(file, "foo", 0, List.of("old1", "old2"), null);
        PreloaderFile.writeAtomic(file, "foo", 1, List.of("new1"), null);

        PreloaderFile.LoadResult result = PreloaderFile.load(file);
        PreloaderFile.Loaded loaded = (PreloaderFile.Loaded) result;
        assertThat(loaded.generation()).isEqualTo(1L);
        assertThat(loaded.keys()).containsExactly("new1");
    }

    @Test
    void writeAtomic_strayTmpFromCrash_isOverwritten(@TempDir Path tmp) throws IOException {
        // Simulate a crashed prior run that left a half-written .tmp.
        Path file = tmp.resolve("snap");
        Path stray = tmp.resolve("snap.tmp");
        Files.writeString(stray, "stray garbage", StandardCharsets.UTF_8);

        PreloaderFile.writeAtomic(file, "foo", 0, List.of("k1"), null);

        // Destination contains the new content.
        PreloaderFile.LoadResult result = PreloaderFile.load(file);
        assertThat(result).isInstanceOf(PreloaderFile.Loaded.class);
        assertThat(((PreloaderFile.Loaded) result).keys()).containsExactly("k1");
        // .tmp does not exist after a successful move (Files.move with
        // ATOMIC_MOVE removes the source).
        assertThat(Files.exists(stray)).isFalse();
    }

    @Test
    void writeAtomic_isAtomic_destinationNeverPartial(@TempDir Path tmp) throws IOException {
        // We can't easily kill the writer mid-flight in a unit test, but
        // we can verify the contract surface: after writeAtomic returns
        // successfully, the destination contains the *new* content, not
        // a partial mix of old and new. Pre-create a known good file,
        // then overwrite, then assert no old keys leak.
        Path file = tmp.resolve("snap");
        PreloaderFile.writeAtomic(file, "foo", 0, List.of("OLD-1", "OLD-2"), null);
        PreloaderFile.writeAtomic(file, "foo", 1, List.of("NEW-1"), null);

        PreloaderFile.Loaded loaded = (PreloaderFile.Loaded) PreloaderFile.load(file);
        assertThat(loaded.keys()).doesNotContain("OLD-1", "OLD-2");
        assertThat(loaded.keys()).containsExactly("NEW-1");
    }
}
