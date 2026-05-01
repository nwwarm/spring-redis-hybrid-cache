package io.github.nwwarm;

import org.redisson.client.codec.Codec;

/**
 * Maps {@link CacheProperties.Codec} enum values to Redisson {@link Codec}
 * instances for per-cache codec overrides on bucket operations.
 *
 * <p>{@link CacheProperties.Codec#JSON} returns {@code null} — callers should
 * use the global client codec (configured in {@code CacheConfig}) rather than
 * an explicit override. {@link CacheProperties.Codec#KRYO} returns the
 * configured Kryo codec, which is constructed once at startup with the
 * application's registered classes (see {@link CacheProperties.Kryo}).
 *
 * <p>The KRYO instance is built and supplied by {@code CacheConfig}; the
 * resolver does not construct it. This keeps the registration policy
 * centralized — if a cache is configured with KRYO but the application has
 * not declared registered classes, startup fails in {@code CacheConfig}
 * before this resolver is consulted.
 */
final class CodecResolver {

    private final Codec kryoCodec;

    /**
     * @param kryoCodec the configured Kryo codec, or {@code null} if no cache
     *                  uses KRYO. If a cache asks for KRYO and this is
     *                  {@code null}, {@link #resolve(CacheProperties.Codec)}
     *                  throws — that is a programmer error caught by the
     *                  startup-time validator in {@code CacheConfig}.
     */
    CodecResolver(Codec kryoCodec) {
        this.kryoCodec = kryoCodec;
    }

    /**
     * @return the Codec to pass to Redisson per-operation, or {@code null} to
     *         indicate the client default codec should be used.
     */
    Codec resolve(CacheProperties.Codec choice) {
        return switch (choice) {
            case JSON -> null;     // use global client codec
            case KRYO -> {
                if (kryoCodec == null) {
                    throw new IllegalStateException(
                            "Cache configured with codec=KRYO but no Kryo codec was registered. "
                                    + "Set cache.kryo.registered-classes to a non-empty list of "
                                    + "fully-qualified class names that the cache will store.");
                }
                yield kryoCodec;
            }
        };
    }
}
