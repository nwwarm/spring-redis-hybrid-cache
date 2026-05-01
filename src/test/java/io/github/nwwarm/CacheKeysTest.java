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
}
