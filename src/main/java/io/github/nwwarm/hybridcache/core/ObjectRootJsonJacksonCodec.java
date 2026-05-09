package io.github.nwwarm.hybridcache.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import org.redisson.client.protocol.Encoder;
import org.redisson.codec.JsonJacksonCodec;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Variant of {@link JsonJacksonCodec} that pins the writer's static root type
 * to {@link Object}, so Jackson's polymorphic-typing decision is made on the
 * declared type rather than the runtime type.
 *
 * <h2>Why this exists</h2>
 *
 * <p>Jackson's {@link ObjectMapper.DefaultTyping#NON_FINAL} skips emitting
 * {@code @class} for final runtime types. Java records are implicitly final,
 * so a cached record value serializes <em>without</em> a top-level {@code
 * @class} when the writer dispatches on the runtime type (which is what
 * {@link ObjectMapper#writeValue(java.io.OutputStream, Object)} does). The
 * decoder reads with {@code mapper.readValue(in, Object.class)} — for the
 * declared {@link Object} root, default typing requires {@code @class} on
 * the wire because {@link Object} is non-final. The asymmetry shows up as
 * {@code InvalidTypeIdException: missing type id property '@class'} on every
 * L2 GET that targets a record-typed cache value.
 *
 * <p>{@link ObjectMapper#writerFor(Class)} pins the static type the writer
 * uses to make the inclusion decision. With {@code writerFor(Object.class)},
 * the typer is consulted for {@link Object} — non-final, so {@code @class}
 * is always emitted at the root regardless of the runtime value's finality.
 * The reader's expectation is now satisfied by every payload.
 *
 * <h2>Wire format</h2>
 *
 * <p>For {@code Product(99L, "EU-99")} (a record):
 * {@code {"@class":"...$Product","id":["java.lang.Long",99],"name":"EU-99"}}.
 * The {@code @class} on the root is the only delta from the prior wire
 * format; nested fields follow the codec's existing typing rules. Pinned by
 * {@code ObjectRootJsonJacksonCodecTest}.
 *
 * <h2>Scope</h2>
 *
 * <p>The override is intentionally small: only the value encoder is
 * replaced. The decoder, the map key/value codecs, and the configured
 * {@code ObjectMapper} are inherited unchanged so the polymorphic-type
 * validator behavior matches the parent. Pub/sub on the invalidation topic
 * remains on its dedicated {@code TypedJsonJacksonCodec}, which is
 * unaffected by this change.
 */
public final class ObjectRootJsonJacksonCodec extends JsonJacksonCodec {

    public ObjectRootJsonJacksonCodec(ObjectMapper mapper) {
        super(mapper);
    }

    @Override
    public Encoder getValueEncoder() {
        return value -> {
            ByteBuf out = ByteBufAllocator.DEFAULT.buffer();
            try {
                ByteBufOutputStream stream = new ByteBufOutputStream(out);
                // Cast to disambiguate writeValue(OutputStream, ...) from
                // writeValue(DataOutput, ...) — ByteBufOutputStream implements both.
                getObjectMapper().writerFor(Object.class).writeValue((OutputStream) stream, value);
                return stream.buffer();
            } catch (IOException e) {
                out.release();
                throw e;
            } catch (Exception e) {
                out.release();
                throw new IOException(e);
            }
        };
    }
}
