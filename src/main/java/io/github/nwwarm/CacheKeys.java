package io.github.nwwarm;

import java.nio.charset.StandardCharsets;

/**
 * Boundary validation and stringification for cache keys and cache names.
 *
 * <p>Keys are stringified at every cache entry point so the L2 wire format and
 * pub/sub messages see only {@code String} values. This avoids cross-type
 * mismatches where, e.g., a {@code Long} key serializes as a JSON number,
 * deserializes on a remote node as {@code Integer}, and then fails to match
 * the original {@code Long} entry stored in Caffeine because
 * {@code equals(Integer, Long)} is {@code false}.
 *
 * <p>The colon character is reserved as the path separator inside Redis bucket
 * keys ({@code cacheName:generation:key}) and inside the lock key
 * ({@code cacheName:lock:key}). A user-supplied key containing {@code ':'}
 * would corrupt either prefix and pull the entry out of its generation
 * partition. Keys are bounded at 256 bytes after stringification to keep Redis
 * key memory predictable.
 */
final class CacheKeys {

    static final int MAX_KEY_BYTES = 256;
    static final String NULL_KEY = "_null";

    private CacheKeys() {}

    /**
     * Stringifies a user-supplied key and validates the result.
     *
     * @throws IllegalArgumentException if the stringified key contains
     *         {@code ':'} or exceeds {@link #MAX_KEY_BYTES} bytes (UTF-8).
     */
    static String stringify(String cacheName, Object key) {
        String s = key == null ? NULL_KEY : key.toString();
        if (s.indexOf(':') >= 0) {
            throw new IllegalArgumentException(
                    "Cache key for cache '" + cacheName + "' contains reserved character ':' "
                            + "(key=\"" + s + "\"). The colon is used as a path separator inside "
                            + "Redis bucket keys; a key containing ':' would break the cache name "
                            + "and generation prefix. Apply a custom KeyGenerator that escapes ':' "
                            + "before passing the value to the cache.");
        }
        int bytes = s.getBytes(StandardCharsets.UTF_8).length;
        if (bytes > MAX_KEY_BYTES) {
            throw new IllegalArgumentException(
                    "Cache key for cache '" + cacheName + "' exceeds " + MAX_KEY_BYTES
                            + "-byte limit (got " + bytes + " bytes). Apply a custom KeyGenerator "
                            + "that hashes long keys (for example to a UUID or SHA-1 digest) before "
                            + "passing them to the cache.");
        }
        return s;
    }

    /**
     * Cache names sit at the root of every Redis key produced by the library
     * ({@code <cacheName>:<generation>:<key>}). A name containing {@code ':'}
     * would collide with the path separator and silently mis-route lookups.
     */
    static void validateCacheName(String name) {
        if (name == null) {
            throw new IllegalArgumentException("Cache name must not be null");
        }
        if (name.indexOf(':') >= 0) {
            throw new IllegalArgumentException(
                    "Cache name '" + name + "' contains reserved character ':'. "
                            + "Cache names form the root prefix of Redis bucket keys "
                            + "(<cacheName>:<generation>:<key>); a name containing ':' "
                            + "would collide with the path separator and break the "
                            + "generation-based clear semantics.");
        }
    }
}
