package io.github.nwwarm;

import org.redisson.client.codec.Codec;
import org.redisson.codec.Kryo5Codec;

/**
 * Resolves {@link CacheProperties.Codec} enum values to Redisson {@link Codec}
 * instances for per-cache codec overrides on bucket operations.
 *
 * <p>{@link CacheProperties.Codec#JSON} returns {@code null} — the caller should
 * use the global client codec (configured in {@code CacheConfig}) rather than
 * an explicit override. {@link CacheProperties.Codec#KRYO} returns a shared
 * {@link Kryo5Codec} instance.
 *
 * <p>The Kryo codec instance is shared because constructing it is non-trivial
 * (Kryo registration, classloader binding) and it is thread-safe in Redisson's
 * usage. If users need a customized Kryo configuration (e.g., explicit class
 * registration for production performance), override the {@code RedissonClient}
 * bean and configure it at the client level instead.
 */
final class CodecResolver {

    private static final Codec KRYO_INSTANCE = new Kryo5Codec();

    private CodecResolver() {}

    /**
     * @return the Codec to pass to Redisson per-operation, or {@code null} to
     *         indicate the client default codec should be used.
     */
    static Codec resolve(CacheProperties.Codec choice) {
        return switch (choice) {
            case JSON -> null;     // use global client codec
            case KRYO -> KRYO_INSTANCE;
        };
    }
}
