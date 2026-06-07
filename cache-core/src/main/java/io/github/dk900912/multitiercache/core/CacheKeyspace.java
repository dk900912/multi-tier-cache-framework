package io.github.dk900912.multitiercache.core;

import io.github.dk900912.multitiercache.api.CacheKey;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/**
 * Internal Redis keyspace mapping for cache data and generation metadata.
 * <p>
 * Design goals:
 * <ul>
 *     <li>Keep data keys and generation keys in the same Redis Cluster slot</li>
 *     <li>Avoid leaking raw business keys into Redis key names</li>
 *     <li>Use a collision-resistant full SHA-256 identifier instead of a truncated hash</li>
 *     <li>Encode identifiers with Base64URL to keep Redis keys compact and character-safe</li>
 * </ul>
 *
 * @author dukui
 */
public final class CacheKeyspace {

    private static final String DATA_PREFIX = "mtc:data:";
    private static final String GENERATION_PREFIX = "mtc:gen:";
    private static final Base64.Encoder BASE64_URL_ENCODER = Base64.getUrlEncoder().withoutPadding();
    private static final ThreadLocal<MessageDigest> SHA_256 = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    });

    private CacheKeyspace() {
        throw new UnsupportedOperationException("Utility class");
    }

    public static CacheKey dataKey(CacheKey businessKey) {
        return CacheKey.simple(dataKey(businessKey.toKeyString()));
    }

    public static CacheKey generationKey(CacheKey businessKey) {
        return CacheKey.simple(generationKey(businessKey.toKeyString()));
    }

    public static String dataKey(String businessKey) {
        String identifier = identifier(businessKey);
        return DATA_PREFIX + "{" + identifier + "}";
    }

    public static String generationKey(String businessKey) {
        String identifier = identifier(businessKey);
        return GENERATION_PREFIX + "{" + identifier + "}";
    }

    public static String identifier(String businessKey) {
        MessageDigest digest = SHA_256.get();
        digest.reset();
        byte[] hash = digest.digest(businessKey.getBytes(StandardCharsets.UTF_8));
        return BASE64_URL_ENCODER.encodeToString(hash);
    }
}
