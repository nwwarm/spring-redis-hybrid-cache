package io.github.nwwarm.hybridcache.core;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.Test;
import org.redisson.codec.JsonJacksonCodec;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the wire format produced by {@link ObjectRootJsonJacksonCodec} and
 * documents the contrast with the parent {@link JsonJacksonCodec}.
 *
 * <p>Without this codec the parent emits no top-level {@code @class} for a
 * record-typed value (records are implicitly final, NON_FINAL skips them).
 * The decoder reads via {@code Object.class} and requires {@code @class},
 * which produces {@code InvalidTypeIdException: missing type id property
 * '@class'}. This test asserts the writer-vs-reader symmetry restored by
 * {@link ObjectRootJsonJacksonCodec} and is the regression-pin: a future
 * refactor that swaps the codec back to bare {@link JsonJacksonCodec}
 * (or otherwise stops pinning the writer's static root type) will fail the
 * {@link #recordRoot_emitsAtClass_andRoundTrips()} assertion.
 */
class ObjectRootJsonJacksonCodecTest {

    public record Product(Long id, String name) {}

    private static ObjectMapper newCacheMapper() {
        BasicPolymorphicTypeValidator ptv = BasicPolymorphicTypeValidator.builder()
                .allowIfBaseType("io.github.nwwarm.")
                .allowIfBaseType("java.util.")
                .allowIfBaseType("java.lang.")
                .allowIfBaseType("java.time.")
                .build();
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .activateDefaultTyping(ptv, ObjectMapper.DefaultTyping.NON_FINAL,
                        JsonTypeInfo.As.PROPERTY);
    }

    @Test
    void recordRoot_emitsAtClass_andRoundTrips() throws Exception {
        ObjectRootJsonJacksonCodec codec = new ObjectRootJsonJacksonCodec(newCacheMapper());
        Product p = new Product(99L, "EU-99");

        ByteBuf buf = codec.getValueEncoder().encode(p);
        try {
            String wire = buf.toString(StandardCharsets.UTF_8);
            assertThat(wire)
                    .as("record root must carry @class on the wire so the Object.class decoder can resolve it")
                    .contains("\"@class\":\"" + Product.class.getName() + "\"");

            Object decoded = codec.getValueDecoder().decode(buf.resetReaderIndex(), null);
            assertThat(decoded).isEqualTo(p);
        } finally {
            buf.release();
        }
    }

    @Test
    void parentCodec_omitsAtClassForRecord_demonstratingTheBug() throws Exception {
        // Anti-test: documents what the unfixed codec produces. If this ever
        // starts emitting @class without our subclass, Redisson upstream
        // changed behavior and ObjectRootJsonJacksonCodec is no longer needed.
        JsonJacksonCodec parent = new JsonJacksonCodec(newCacheMapper());
        ByteBuf buf = parent.getValueEncoder().encode(new Product(99L, "EU-99"));
        try {
            String wire = buf.toString(StandardCharsets.UTF_8);
            assertThat(wire)
                    .as("baseline: parent JsonJacksonCodec dispatches on runtime type "
                            + "and skips @class for final records — this is the bug "
                            + "ObjectRootJsonJacksonCodec exists to fix")
                    .doesNotContain("\"@class\"");
        } finally {
            buf.release();
        }
    }

    @Test
    void stringRoot_roundTrips() throws Exception {
        ObjectRootJsonJacksonCodec codec = new ObjectRootJsonJacksonCodec(newCacheMapper());

        ByteBuf buf = codec.getValueEncoder().encode("widget");
        try {
            Object decoded = codec.getValueDecoder().decode(buf, null);
            assertThat(decoded).isEqualTo("widget");
        } finally {
            buf.release();
        }
    }

    @Test
    void longRoot_roundTrips() throws Exception {
        ObjectRootJsonJacksonCodec codec = new ObjectRootJsonJacksonCodec(newCacheMapper());

        ByteBuf buf = codec.getValueEncoder().encode(42L);
        try {
            Object decoded = codec.getValueDecoder().decode(buf, null);
            assertThat(decoded).isEqualTo(42L);
        } finally {
            buf.release();
        }
    }
}
