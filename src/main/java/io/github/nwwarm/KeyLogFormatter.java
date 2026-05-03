package io.github.nwwarm;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.UUID;

/**
 * Formats cache keys for log output.
 *
 * <p>When {@code cache.log-keys=false} (the default), each key is replaced with
 * {@code key:<sha256_b64url[:16]>} — a 16-character URL-safe Base64 digest of
 * {@code salt || key.toString()}.  The salt is either the configured
 * {@code cache.log-key-salt} value or a random per-process UUID chosen at
 * startup, ensuring the digests cannot be reversed against a pre-built rainbow
 * table for common key values (user IDs, SKUs, etc.).
 *
 * <p>When {@code cache.log-keys=true}, {@link #format(Object)} returns
 * {@link String#valueOf(Object)} — the raw key, suitable for local development
 * or deployments where cache keys are not PII.
 *
 * <p>{@code null} is always rendered as {@code "<null>"} so that callers on
 * error paths never introduce a secondary NPE.
 */
final class KeyLogFormatter {

    private final boolean logKeys;
    private final byte[] salt;

    KeyLogFormatter(boolean logKeys, String salt) {
        this.logKeys = logKeys;
        String effectiveSalt = (salt != null && !salt.isBlank()) ? salt : UUID.randomUUID().toString();
        this.salt = effectiveSalt.getBytes(StandardCharsets.UTF_8);
    }

    String format(Object key) {
        if (key == null) return "<null>";
        if (logKeys) return String.valueOf(key);

        String keyStr = String.valueOf(key);
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(salt);
            byte[] hash = digest.digest(keyStr.getBytes(StandardCharsets.UTF_8));
            String b64 = Base64.getUrlEncoder().withoutPadding().encodeToString(hash);
            return "key:" + b64.substring(0, 16);
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is mandatory in every JDK; this branch is unreachable.
            return "key:<hash-error>";
        }
    }
}
