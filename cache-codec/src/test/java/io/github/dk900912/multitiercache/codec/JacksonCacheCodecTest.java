package io.github.dk900912.multitiercache.codec;

import io.github.dk900912.multitiercache.api.exception.CacheCodecException;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.api.model.CacheMessage;
import io.github.dk900912.multitiercache.api.model.CacheMessageType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JacksonCacheCodecTest {

    private JacksonCacheCodec codec;

    @BeforeEach
    void setUp() {
        CacheConfig config = new CacheConfig();
        // Trust our own package for testing
        config.getCodec().setTrustedPackages(List.of("io.github.dk900912"));
        codec = new JacksonCacheCodec();
        codec.initialize(config);
    }

    public static class User {
        private int id;
        private String name;

        public User() {}

        public User(int id, String name) {
            this.id = id;
            this.name = name;
        }

        public int getId() { return id; }
        public void setId(int id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
    }

    @Test
    void testEncodeAndDecodeGenericList() {
        List<User> users = Arrays.asList(new User(1, "Alice"), new User(2, "Bob"));
        CacheMessage<Object> originalMessage = new CacheMessage<>(
                "user:list", users, 1L, CacheMessageType.INSERT, 1000L);

        String json = codec.encode(originalMessage);
        assertNotNull(json);
        System.out.println("Encoded JSON: " + json);

        CacheMessage<Object> decodedMessage = codec.decodeMessage(json, Object.class);
        assertNotNull(decodedMessage);
        assertEquals("user:list", decodedMessage.getKey());
        assertEquals(1L, decodedMessage.getVersion());
        assertEquals(CacheMessageType.INSERT, decodedMessage.getType());

        Object data = decodedMessage.getData();
        assertTrue(data instanceof List);
        
        @SuppressWarnings("unchecked")
        List<Object> decodedList = (List<Object>) data;
        assertEquals(2, decodedList.size());
        
        // This is the core verification: the element should be deserialized as User, not LinkedHashMap
        assertTrue(decodedList.get(0) instanceof User);
        User firstUser = (User) decodedList.get(0);
        assertEquals(1, firstUser.getId());
        assertEquals("Alice", firstUser.getName());
    }

    @Test
    void testEncodeAndDecodeWithNulls() {
        CacheMessage<Object> originalMessage = new CacheMessage<>(
                "user:list", null, -1L, CacheMessageType.PENETRATE, null);

        String json = codec.encode(originalMessage);
        assertNotNull(json);

        CacheMessage<Object> decodedMessage = codec.decodeMessage(json, Object.class);
        assertNotNull(decodedMessage);
        assertEquals("user:list", decodedMessage.getKey());
        assertEquals(-1L, decodedMessage.getVersion());
        assertEquals(CacheMessageType.PENETRATE, decodedMessage.getType());
        assertNull(decodedMessage.getData());
        assertNull(decodedMessage.getTtlMillis());
    }

    @Test
    void testDecodeLegacyMessageWithGenerationField() {
        String legacyJson = "{\"key\":\"user:1\",\"data\":\"value\",\"generation\":3,\"version\":7,\"type\":\"update\",\"ttlMillis\":1000}";

        CacheMessage<Object> decodedMessage = codec.decodeMessage(legacyJson, Object.class);

        assertEquals("user:1", decodedMessage.getKey());
        assertEquals("value", decodedMessage.getData());
        assertEquals(7L, decodedMessage.getVersion());
        assertEquals(CacheMessageType.UPDATE, decodedMessage.getType());
        assertEquals(1000L, decodedMessage.getTtlMillis());
    }

    @Test
    void testSecurityRcePrevention() {
        // We simulate a payload that tries to instantiate an untrusted class.
        // We'll use com.sun.rowset.JdbcRowSetImpl as a classic untrusted class for this test.
        String maliciousJson = "{\"key\":\"hack\",\"data\":[\"com.sun.rowset.JdbcRowSetImpl\", {\"dataSourceName\":\"ldap://localhost/Exploit\"}],\"version\":1,\"type\":\"insert\",\"ttlMillis\":1000}";
        
        CacheCodecException exception = assertThrows(CacheCodecException.class, () -> {
            codec.decodeMessage(maliciousJson, Object.class);
        });

        // Debug output
        System.out.println("Exception message: " + exception.getMessage());
        if (exception.getCause() != null) {
            System.out.println("Cause message: " + exception.getCause().getMessage());
        }

        // The exception should mention that the type is not allowed
        assertTrue(exception.getCause() != null && (
                   exception.getCause().getMessage().contains("prevented") || 
                   exception.getCause().getMessage().contains("not allowed") ||
                   exception.getCause().getMessage().contains("Could not resolve type id")
        ));
    }
}
