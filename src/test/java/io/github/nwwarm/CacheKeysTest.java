package io.github.nwwarm;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Boundary validation: cache names cannot contain {@code ':'}, keys cannot
 * contain {@code ':'}, keys cannot exceed 256 bytes after stringification,
 * and {@code null} maps to a sentinel string.
 */
class CacheKeysTest {

    @Test
    void stringify_nullMapsToSentinel() {
        assertThat(CacheKeys.stringify("c", null)).isEqualTo(CacheKeys.NULL_KEY);
    }

    @Test
    void stringify_stringPassesThrough() {
        assertThat(CacheKeys.stringify("c", "abc")).isEqualTo("abc");
    }

    @Test
    void stringify_longUsesToString() {
        assertThat(CacheKeys.stringify("c", 42L)).isEqualTo("42");
    }

    @Test
    void stringify_rejectsColonInKey() {
        assertThatThrownBy(() -> CacheKeys.stringify("products", "tenant:42"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("products")
                .hasMessageContaining("tenant:42")
                .hasMessageContaining("KeyGenerator");
    }

    @Test
    void stringify_rejectsKeyOverByteLimit() {
        String tooLong = "a".repeat(CacheKeys.MAX_KEY_BYTES + 1);
        assertThatThrownBy(() -> CacheKeys.stringify("products", tooLong))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(String.valueOf(CacheKeys.MAX_KEY_BYTES))
                .hasMessageContaining("KeyGenerator");
    }

    @Test
    void stringify_acceptsKeyAtByteLimit() {
        String exactly256 = "a".repeat(CacheKeys.MAX_KEY_BYTES);
        assertThat(CacheKeys.stringify("products", exactly256)).hasSize(CacheKeys.MAX_KEY_BYTES);
    }

    @Test
    void stringify_byteLimitMeasuredInUtf8NotChars() {
        // A 4-byte UTF-8 character (e.g., 𝄞) — 64 of these = 256 bytes (at limit).
        String at = "𝄞".repeat(64);
        assertThat(CacheKeys.stringify("c", at)).isEqualTo(at);

        // 65 of them = 260 bytes (over limit).
        String over = "𝄞".repeat(65);
        assertThatThrownBy(() -> CacheKeys.stringify("c", over))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void validateCacheName_rejectsColon() {
        assertThatThrownBy(() -> CacheKeys.validateCacheName("user:profile"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("user:profile")
                .hasMessageContaining("path separator");
    }

    @Test
    void validateCacheName_acceptsHyphensAndUnderscores() {
        CacheKeys.validateCacheName("user-profile");
        CacheKeys.validateCacheName("user_profile");
        CacheKeys.validateCacheName("products");
    }

    @Test
    void validateCacheName_rejectsNull() {
        assertThatThrownBy(() -> CacheKeys.validateCacheName(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void validateCacheName_rejectsOpenBrace() {
        // A name like "tenant{a}" produces a value key "{tenant{a}:42}:v:0".
        // Redis Cluster matches the first balanced {...} substring, so the
        // slot would be computed from "tenant{a}" alone, routing every key
        // in the cache to a single shard.
        assertThatThrownBy(() -> CacheKeys.validateCacheName("tenant{a}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tenant{a}")
                .hasMessageContaining("hash tag");
    }

    @Test
    void validateCacheName_rejectsCloseBrace() {
        assertThatThrownBy(() -> CacheKeys.validateCacheName("a}b"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("hash tag");
    }

    // ---------- Redis key formatting ----------

    @Test
    void valueKey_producesHashTaggedFormat() {
        assertThat(CacheKeys.valueKey("products", "42", 0))
                .isEqualTo("{products:42}:v:0");
    }

    @Test
    void valueKey_generationGoesOutsideTheTag() {
        // Same logical key, different generations: the {...} substring is
        // identical so both keys hash to the same slot. This is what makes
        // a future atomic evict-and-bump pipeline possible.
        String g0 = CacheKeys.valueKey("products", "42", 0);
        String g7 = CacheKeys.valueKey("products", "42", 7);
        assertThat(g0).isEqualTo("{products:42}:v:0");
        assertThat(g7).isEqualTo("{products:42}:v:7");
        // Hash-tag substrings match exactly.
        assertThat(extractTag(g0)).isEqualTo(extractTag(g7));
    }

    @Test
    void lockKey_producesHashTaggedFormat() {
        assertThat(CacheKeys.lockKey("products", "42"))
                .isEqualTo("{products:42}:lock");
    }

    @Test
    void valueAndLockShareTheSameHashTag() {
        // The collocation property — value and lock for the same logical
        // key live in the same {...} substring, so Cluster routes them to
        // the same slot.
        String v = CacheKeys.valueKey("products", "42", 0);
        String l = CacheKeys.lockKey("products", "42");
        assertThat(extractTag(v)).isEqualTo(extractTag(l)).isEqualTo("products:42");
    }

    @Test
    void generationKey_isPerCacheAndUnchanged() {
        // Intentionally NOT inside a hash tag — the generation counter is
        // per-cache; collocating it with each key would scatter it across
        // every slot.
        assertThat(CacheKeys.generationKey("products"))
                .isEqualTo("products:generation");
    }

    @Test
    void valueKey_keyContainingBraces_doesNotShiftHashTagBoundary() {
        // A user-supplied key containing { or } is permitted (we don't
        // validate keys for braces). Redis Cluster matches the FIRST
        // balanced {...} substring, which is still the outer cache:key
        // tag — so routing remains correct.
        String key = CacheKeys.valueKey("products", "{nested}", 0);
        assertThat(key).isEqualTo("{products:{nested}}:v:0");
        // The first balanced {...} starts at index 0 and closes at the
        // first matching '}' (after "products:{nested"). Redis Cluster's
        // implementation actually closes at the *first* '}', giving the
        // tag "products:{nested" — different from a clean key, but
        // deterministic and not catastrophic. The crucial property is
        // that the cache name is brace-free (validated) so the tag still
        // begins where we expect.
        assertThat(extractTag(key)).isEqualTo("products:{nested");
    }

    /**
     * Mirrors Redis Cluster's hash-tag extraction: the substring between
     * the first '{' and the first subsequent '}'. Empty tag (e.g. {@code "{}"})
     * causes Cluster to hash the whole key — but we never produce that.
     */
    private static String extractTag(String key) {
        int open = key.indexOf('{');
        if (open < 0) return key;
        int close = key.indexOf('}', open + 1);
        if (close < 0 || close == open + 1) return key;
        return key.substring(open + 1, close);
    }
}
