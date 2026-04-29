package io.github.nwwarm;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.Codec;
import org.redisson.codec.Kryo5Codec;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies wire-format codecs round-trip values correctly, and documents the
 * actual schema-evolution behavior of the default {@link Kryo5Codec} as
 * configured by the cache.
 */
class CodecRoundTripIT extends RedisTestBase {

    private RedissonClient redisson;

    @BeforeEach
    void setUp() {
        redisson = newRedisson();
    }

    @AfterEach
    void tearDown() {
        if (redisson != null) redisson.shutdown();
    }

    @Test
    void jsonCodec_roundTripsComplexValue() {
        NearCache cache = newNearCache("codec-json-" + System.nanoTime(),
                redisson, defaultBreaker(), "node-A",
                CacheProperties.Codec.JSON, new SimpleMeterRegistry());
        try {
            User original = new User("Alice", 30, List.of("admin", "ops"));
            cache.put("u", original);

            // Drop L1 to force the value to round-trip through the JSON codec.
            nativeOf(cache).invalidate("u");
            User decoded = cache.get("u", User.class);

            assertThat(decoded).isEqualTo(original);
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void kryoCodec_roundTripsComplexValue() {
        NearCache cache = newNearCache("codec-kryo-" + System.nanoTime(),
                redisson, defaultBreaker(), "node-A",
                CacheProperties.Codec.KRYO, new SimpleMeterRegistry());
        try {
            User original = new User("Bob", 42, List.of("dev"));
            cache.put("u", original);

            nativeOf(cache).invalidate("u");
            User decoded = cache.get("u", User.class);

            assertThat(decoded).isEqualTo(original);
        } finally {
            cache.shutdown();
        }
    }

    /**
     * Schema evolution behavior of the default {@link Kryo5Codec}.
     *
     * <p>The codec writes class identity into the payload (via
     * {@code writeClassAndObject}) and reads it back from the payload (via
     * {@code readClassAndObject}) — the bucket's reader type parameter is NOT
     * consulted at decode time. So pointing a wider bucket at a key written as
     * {@link UserV1} returns a {@code UserV1} instance (the writer's class
     * wins), not a deserialization failure.
     *
     * <p>Practical consequence: the relevant schema-evolution failure mode for
     * the default codec is mutating the writer's class itself between deploys
     * (e.g., adding a field to {@code UserV1}), not aiming a different reader
     * class at the bytes. Kryo's default field serializer requires the writer's
     * and the *currently-loaded* writer-class shape to match. Teams that need
     * tolerant evolution must supply a custom Kryo with
     * {@code CompatibleFieldSerializer} / {@code TaggedFieldSerializer}, or
     * evolve via a TTL-bounded clear (the operational guidance documented on
     * {@code CacheProperties.Codec}).
     */
    @Test
    void kryoCodec_decodeUsesWritersClassFromPayload() {
        Codec kryo = new Kryo5Codec();
        String key = "codec-evolve-" + System.nanoTime();

        UserV1 v1 = new UserV1("Carol", 25);
        RBucket<UserV1> writer = redisson.getBucket(key, kryo);
        writer.set(v1);

        // Reading back as the original class works.
        RBucket<UserV1> readerV1 = redisson.getBucket(key, kryo);
        assertThat(readerV1.get()).isEqualTo(v1);

        // Aiming a "wider" reader bucket at the same key returns the writer's
        // class — Kryo decodes whatever the payload says, ignoring the bucket
        // type parameter. The unchecked cast at the call site would be a latent
        // ClassCastException for callers that actually used the value as a
        // different concrete type.
        RBucket<Object> readerAsObject = redisson.getBucket(key, kryo);
        assertThat(readerAsObject.get())
                .as("default Kryo5Codec.readClassAndObject returns the writer's class regardless of bucket type")
                .isInstanceOf(UserV1.class)
                .isEqualTo(v1);
    }

    // ---------- Test fixtures ----------

    /**
     * Public, non-final, no-arg constructor + setters: needed for both Jackson
     * (default polymorphic typing) and Kryo (default field serializer with
     * reflection-instantiation).
     */
    public static class User implements Serializable {
        private String name;
        private int age;
        private List<String> roles;

        public User() {}

        public User(String name, int age, List<String> roles) {
            this.name = name;
            this.age = age;
            this.roles = roles;
        }

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public int getAge() { return age; }
        public void setAge(int age) { this.age = age; }

        public List<String> getRoles() { return roles; }
        public void setRoles(List<String> roles) { this.roles = roles; }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof User u)) return false;
            return age == u.age
                    && Objects.equals(name, u.name)
                    && Objects.equals(roles, u.roles);
        }

        @Override
        public int hashCode() { return Objects.hash(name, age, roles); }
    }

    public static class UserV1 implements Serializable {
        private String name;
        private int age;

        public UserV1() {}
        public UserV1(String name, int age) { this.name = name; this.age = age; }

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public int getAge() { return age; }
        public void setAge(int age) { this.age = age; }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof UserV1 u)) return false;
            return age == u.age && Objects.equals(name, u.name);
        }

        @Override
        public int hashCode() { return Objects.hash(name, age); }
    }

}
