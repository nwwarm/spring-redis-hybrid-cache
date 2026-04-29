package io.github.nwwarm;

import jakarta.annotation.PreDestroy;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.api.listener.MessageListener;
import org.redisson.client.codec.Codec;
import org.redisson.codec.TypedJsonJacksonCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Single per-node Redis pub/sub subscription that fans invalidation messages
 * out to the right {@link NearCache}. Replaces N listeners across N caches
 * with one listener plus an in-memory routing table.
 *
 * <p>Self-skip and cache-name routing happen here, so individual caches don't
 * need to repeat those checks. The dispatcher is also the single source of
 * truth for {@code nodeId} — caches read it from here when publishing.
 */
class InvalidationDispatcher implements MessageListener<InvalidationMessage> {

    private static final Logger log = LoggerFactory.getLogger(InvalidationDispatcher.class);
    private static final String INVALIDATION_TOPIC = "cache:invalidate";

    // Dedicated codec for the invalidation topic. The global JsonJacksonCodec
    // activates NON_FINAL default typing on its internal mapper, but reads via
    // Object.class — which then requires a top-level @class on every payload.
    // InvalidationMessage is a record (implicitly final), so default typing
    // skips the @class on serialize, and the decoder fails on read. Pinning the
    // topic to a typed codec sidesteps default typing entirely for pubsub.
    private static final Codec INVALIDATION_CODEC = new TypedJsonJacksonCodec(InvalidationMessage.class);

    private final String nodeId;
    private final RTopic topic;
    private final int listenerId;
    private final ConcurrentMap<String, NearCache> caches = new ConcurrentHashMap<>();

    InvalidationDispatcher(RedissonClient redisson, String nodeId) {
        this.nodeId = nodeId;
        this.topic = redisson.getTopic(INVALIDATION_TOPIC, INVALIDATION_CODEC);
        this.listenerId = topic.addListener(InvalidationMessage.class, this);
    }

    String getNodeId() {
        return nodeId;
    }

    void register(NearCache cache) {
        NearCache prev = caches.put(cache.getName(), cache);
        if (prev != null && prev != cache) {
            log.warn("Replacing existing NearCache registration for '{}'", cache.getName());
        }
    }

    void deregister(String cacheName) {
        caches.remove(cacheName);
    }

    void publish(InvalidationMessage message) {
        topic.publish(message);
    }

    @Override
    public void onMessage(CharSequence channel, InvalidationMessage msg) {
        if (nodeId.equals(msg.nodeId())) return;
        NearCache cache = caches.get(msg.cacheName());
        if (cache == null) return;
        cache.handleInvalidation(msg.op(), msg.key());
    }

    @PreDestroy
    void shutdown() {
        try {
            topic.removeListener(listenerId);
        } catch (Exception e) {
            log.warn("Failed to remove invalidation listener", e);
        }
        caches.clear();
    }
}
