package io.github.dk900912.multitiercache.codec;

import io.github.dk900912.multitiercache.api.exception.CacheCodecException;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.api.model.CacheMessage;
import io.github.dk900912.multitiercache.api.model.CacheMessageType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

    public static class TimeEntity {
        private Instant createdAt;
        private LocalDateTime updatedAt;

        public TimeEntity() {}

        public TimeEntity(Instant createdAt, LocalDateTime updatedAt) {
            this.createdAt = createdAt;
            this.updatedAt = updatedAt;
        }

        public Instant getCreatedAt() { return createdAt; }
        public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
        public LocalDateTime getUpdatedAt() { return updatedAt; }
        public void setUpdatedAt(LocalDateTime updatedAt) { this.updatedAt = updatedAt; }
    }

    public static class CollectionEntity {
        private long version;
        private List<String> tags;

        public CollectionEntity() {}

        public CollectionEntity(long version, List<String> tags) {
            this.version = version;
            this.tags = tags;
        }

        public long getVersion() { return version; }
        public void setVersion(long version) { this.version = version; }
        public List<String> getTags() { return tags; }
        public void setTags(List<String> tags) { this.tags = tags; }
    }

    public static class ObjectFieldEntity {
        public Object payload;
        public Map<String, Object> metadata;
    }

    public static class ConstructorGuardEntity {
        static int constructorCalls;

        public Object payload;

        public ConstructorGuardEntity() {
            constructorCalls++;
        }
    }

    public record ObjectFieldRecord(Object payload, Map<String, Object> metadata) {
    }

    public enum ComplexStatus {
        ACTIVE,
        ARCHIVED
    }

    public static class HugeComplexBean {
        public long id;
        public String name;
        public ComplexStatus status;
        public Instant createdAt;
        public LocalDateTime updatedAt;
        public Optional<String> note;
        public List<ComplexChild> children;
        public Map<String, ComplexChild> childIndex;
        public Set<String> tags;
        public Map<ComplexStatus, List<ComplexPrice>> priceMatrix;
        public Map<String, Object> metadata;
        public List<List<Map<String, Object>>> nestedMatrix;
        public int[] counters;
        public byte[] digest;
        public ComplexAudit audit;
    }

    public static class ComplexChild {
        public String id;
        public int rank;
        public boolean enabled;
        public BigDecimal score;
        public List<String> aliases;
        public Map<String, Object> attributes;
        public ComplexAudit audit;
    }

    public static class ComplexPrice {
        public String currency;
        public BigDecimal amount;
        public Optional<BigDecimal> discount;
    }

    public static class ComplexAudit {
        public UUID operatorId;
        public URI source;
        public Instant happenedAt;
        public Map<String, String> labels;
    }

    @Test
    void testEncodeAndDecodeGenericList() {
        List<User> users = Arrays.asList(new User(1, "Alice"), new User(2, "Bob"));
        CacheMessage<Object> originalMessage = new CacheMessage<>(
                "user:list", users, 1L, CacheMessageType.INSERT, 1000L);

        String json = codec.encode(originalMessage);
        assertNotNull(json);

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
    void decodeMessageShouldHonorExplicitDataClassWithoutTypeMetadata() {
        String json = """
                {
                  "key": "user:1",
                  "data": { "id": 1, "name": "Alice" },
                  "version": 1,
                  "type": "insert",
                  "ttlMillis": 1000
                }
                """;

        CacheMessage<User> decodedMessage = codec.decodeMessage(json, User.class);

        assertNotNull(decodedMessage.getData());
        assertEquals(User.class, decodedMessage.getData().getClass());
        assertEquals(1, decodedMessage.getData().getId());
        assertEquals("Alice", decodedMessage.getData().getName());
    }

    @Test
    void decodeShouldHonorExplicitClassWithoutTypeMetadata() {
        User decoded = codec.decode("{\"id\":1,\"name\":\"Alice\"}", User.class);

        assertEquals(1, decoded.getId());
        assertEquals("Alice", decoded.getName());
    }

    @Test
    void shouldRoundTripJavaTimePayload() {
        TimeEntity entity = new TimeEntity(
                Instant.parse("2026-07-07T12:00:00Z"),
                LocalDateTime.of(2026, 7, 7, 20, 0));
        CacheMessage<Object> originalMessage = new CacheMessage<>(
                "time:1", entity, 1L, CacheMessageType.INSERT, 1000L);

        String json = codec.encode(originalMessage);
        CacheMessage<TimeEntity> decodedMessage = codec.decodeMessage(json, TimeEntity.class);

        assertEquals(entity.getCreatedAt(), decodedMessage.getData().getCreatedAt());
        assertEquals(entity.getUpdatedAt(), decodedMessage.getData().getUpdatedAt());
    }

    @Test
    void shouldNotLeakJdkInternalCollectionImplementationTypes() {
        CacheMessage<Object> message = new CacheMessage<>(
                "collection:1",
                new CollectionEntity(1L, List.of("fresh")),
                1L,
                CacheMessageType.INSERT,
                1000L);

        String json = codec.encode(message);

        assertFalse(json.contains("java.util.ImmutableCollections"),
                "wire JSON must not depend on JDK internal immutable collection implementation classes");
        CacheMessage<CollectionEntity> decodedMessage = codec.decodeMessage(json, CollectionEntity.class);
        assertEquals(List.of("fresh"), decodedMessage.getData().getTags());
    }

    @Test
    void shouldNotLeakJdkInternalCollectionImplementationWhenPayloadIsCollection() {
        CacheMessage<Object> message = new CacheMessage<>(
                "collection:root",
                List.of(new User(1, "Alice")),
                1L,
                CacheMessageType.INSERT,
                1000L);

        String json = codec.encode(message);

        assertFalse(json.contains("java.util.ImmutableCollections"),
                "root collection payloads must not expose JDK internal immutable collection classes");
        CacheMessage<Object> decodedMessage = codec.decodeMessage(json, Object.class);
        assertTrue(decodedMessage.getData() instanceof List);
        @SuppressWarnings("unchecked")
        List<Object> decodedUsers = (List<Object>) decodedMessage.getData();
        assertEquals(1, decodedUsers.size());
        assertTrue(decodedUsers.getFirst() instanceof User);
        assertEquals("Alice", ((User) decodedUsers.getFirst()).getName());
    }

    @Test
    void shouldNotLeakJdkInternalCollectionImplementationInsideObjectFields() {
        ObjectFieldEntity entity = new ObjectFieldEntity();
        entity.payload = List.of(new User(1, "Alice"));
        entity.metadata = new LinkedHashMap<>();
        entity.metadata.put("tags", List.of("fresh", "large"));
        entity.metadata.put("attributes", Map.of("level", 7, "name", "metadata"));
        CacheMessage<Object> message = new CacheMessage<>(
                "collection:object-field",
                entity,
                1L,
                CacheMessageType.INSERT,
                1000L);

        String json = codec.encode(message);

        assertFalse(json.contains("java.util.ImmutableCollections"),
                "object-valued bean fields must not expose JDK internal immutable collection classes");
        CacheMessage<ObjectFieldEntity> decodedMessage = codec.decodeMessage(json, ObjectFieldEntity.class);
        assertTrue(decodedMessage.getData().payload instanceof List);
        @SuppressWarnings("unchecked")
        List<Object> payload = (List<Object>) decodedMessage.getData().payload;
        assertTrue(payload.getFirst() instanceof User);
        assertEquals("Alice", ((User) payload.getFirst()).getName());
        assertEquals(List.of("fresh", "large"), decodedMessage.getData().metadata.get("tags"));
    }

    @Test
    void shouldNormalizeJdkInternalCollectionsInsideRecordObjectFields() {
        ObjectFieldRecord record = new ObjectFieldRecord(
                List.of(new User(1, "Alice")),
                Map.of("tags", List.of("fresh", "large")));
        CacheMessage<Object> message = new CacheMessage<>(
                "collection:record-field",
                record,
                1L,
                CacheMessageType.INSERT,
                1000L);

        String json = codec.encode(message);

        assertFalse(json.contains("java.util.ImmutableCollections"),
                "record object-valued fields must not expose JDK internal immutable collection classes");
        CacheMessage<ObjectFieldRecord> decodedMessage = codec.decodeMessage(json, ObjectFieldRecord.class);
        assertTrue(decodedMessage.getData().payload() instanceof List);
        assertEquals(List.of("fresh", "large"), decodedMessage.getData().metadata().get("tags"));
    }

    @Test
    void encodeShouldNotMutateOriginalObjectFieldsWhenNormalizingContainers() {
        ObjectFieldEntity entity = new ObjectFieldEntity();
        List<User> originalPayload = List.of(new User(1, "Alice"));
        Map<String, Object> originalMetadata = Map.of("tags", List.of("fresh", "large"));
        entity.payload = originalPayload;
        entity.metadata = originalMetadata;

        codec.encode(new CacheMessage<>(
                "collection:no-mutation",
                entity,
                1L,
                CacheMessageType.INSERT,
                1000L));

        assertTrue(entity.payload == originalPayload);
        assertTrue(entity.metadata == originalMetadata);
    }

    @Test
    void encodeShouldNotInstantiatePojoWhenNormalizingObjectFields() {
        ConstructorGuardEntity.constructorCalls = 0;
        ConstructorGuardEntity entity = new ConstructorGuardEntity();
        entity.payload = List.of(new User(1, "Alice"));

        String json = codec.encode(new CacheMessage<>(
                "collection:no-pojo-copy",
                entity,
                1L,
                CacheMessageType.INSERT,
                1000L));

        assertFalse(json.contains("java.util.ImmutableCollections"));
        assertEquals(1, ConstructorGuardEntity.constructorCalls);
    }

    @Test
    void shouldRejectUntrustedJdkPayloadTypeEvenWhenDataFieldIsObject() {
        String untrustedJson = """
                {
                  "key": "file:1",
                  "data": ["java.io.File", "C:/secret.txt"],
                  "version": 1,
                  "type": "insert",
                  "ttlMillis": 1000
                }
                """;

        assertThrows(CacheCodecException.class, () -> codec.decodeMessage(untrustedJson, Object.class));
    }

    @Test
    void shouldRejectArbitraryJavaUtilPayloadTypeEvenThoughCollectionsAreSupported() {
        String untrustedJson = """
                {
                  "key": "date:1",
                  "data": ["java.util.Date", 1783430400000],
                  "version": 1,
                  "type": "insert",
                  "ttlMillis": 1000
                }
                """;

        assertThrows(CacheCodecException.class, () -> codec.decodeMessage(untrustedJson, Object.class));
    }

    @Test
    void shouldRejectArbitraryJavaLangPayloadTypeEvenThoughPrimitiveWrappersAreSupported() {
        String untrustedJson = """
                {
                  "key": "class:1",
                  "data": ["java.lang.Class", "java.lang.Runtime"],
                  "version": 1,
                  "type": "insert",
                  "ttlMillis": 1000
                }
                """;

        assertThrows(CacheCodecException.class, () -> codec.decodeMessage(untrustedJson, Object.class));
    }

    @Test
    void shouldRoundTripHugeComplexBeanWithMixedNestedTypes() {
        HugeComplexBean bean = createHugeComplexBean(180);
        CacheMessage<Object> message = new CacheMessage<>(
                "huge:complex:1",
                bean,
                42L,
                CacheMessageType.UPDATE,
                300_000L);

        String json = codec.encode(message);

        assertTrue(json.length() > 80_000, "test payload should be large enough to exercise codec pressure");
        assertFalse(json.contains("java.util.ImmutableCollections"),
                "large payload must not leak JDK internal immutable collection classes");

        CacheMessage<HugeComplexBean> typed = codec.decodeMessage(json, HugeComplexBean.class);
        assertHugeComplexBean(bean, typed.getData());

        CacheMessage<Object> dynamic = codec.decodeMessage(json, Object.class);
        assertTrue(dynamic.getData() instanceof HugeComplexBean);
        assertHugeComplexBean(bean, (HugeComplexBean) dynamic.getData());
    }

    @Test
    void encodeDecodeShouldRoundTripHugeComplexBeanWithObjectFields() {
        HugeComplexBean bean = createHugeComplexBean(64);

        String json = codec.encode(bean);
        HugeComplexBean decoded = codec.decode(json, HugeComplexBean.class);

        assertHugeComplexBean(bean, decoded);
    }

    @Test
    void encodeShouldRejectSelfReferentialListWithoutStackOverflow() {
        List<Object> payload = new ArrayList<>();
        payload.add("root");
        payload.add(payload);
        CacheMessage<Object> message = new CacheMessage<>(
                "cycle:list",
                payload,
                1L,
                CacheMessageType.INSERT,
                1000L);

        CacheCodecException exception = assertThrows(CacheCodecException.class, () -> codec.encode(message));

        assertTrue(exception.getCause() instanceof IllegalArgumentException);
        assertTrue(exception.getCause().getMessage().contains("Circular reference"));
    }

    @Test
    void encodeShouldRejectSelfReferentialMapWithoutStackOverflow() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("name", "root");
        payload.put("self", payload);
        CacheMessage<Object> message = new CacheMessage<>(
                "cycle:map",
                payload,
                1L,
                CacheMessageType.INSERT,
                1000L);

        CacheCodecException exception = assertThrows(CacheCodecException.class, () -> codec.encode(message));

        assertTrue(exception.getCause() instanceof IllegalArgumentException);
        assertTrue(exception.getCause().getMessage().contains("Circular reference"));
    }

    @Test
    void testSecurityRcePrevention() {
        // We simulate a payload that tries to instantiate an untrusted class.
        // We'll use com.sun.rowset.JdbcRowSetImpl as a classic untrusted class for this test.
        String maliciousJson = "{\"key\":\"hack\",\"data\":[\"com.sun.rowset.JdbcRowSetImpl\", {\"dataSourceName\":\"ldap://localhost/Exploit\"}],\"version\":1,\"type\":\"insert\",\"ttlMillis\":1000}";
        
        CacheCodecException exception = assertThrows(CacheCodecException.class, () -> {
            codec.decodeMessage(maliciousJson, Object.class);
        });

        // The exception should mention that the type is not allowed
        assertTrue(exception.getCause() != null && (
                   exception.getCause().getMessage().contains("prevented") || 
                   exception.getCause().getMessage().contains("not allowed") ||
                   exception.getCause().getMessage().contains("Could not resolve type id")
        ));
    }

    private HugeComplexBean createHugeComplexBean(int childCount) {
        HugeComplexBean bean = new HugeComplexBean();
        bean.id = 900_912L;
        bean.name = "huge-bean-user-" + "x".repeat(2048);
        bean.status = ComplexStatus.ACTIVE;
        bean.createdAt = Instant.parse("2026-07-07T12:00:00Z");
        bean.updatedAt = LocalDateTime.of(2026, 7, 7, 20, 30, 15);
        bean.note = Optional.of("note-" + "n".repeat(1024));
        bean.children = new ArrayList<>(childCount);
        bean.childIndex = new LinkedHashMap<>();
        bean.tags = new LinkedHashSet<>(List.of("hot", "large", "complex", "stable"));
        bean.priceMatrix = new LinkedHashMap<>();
        bean.metadata = new LinkedHashMap<>();
        bean.nestedMatrix = new ArrayList<>();
        bean.counters = new int[] {1, 1, 2, 3, 5, 8, 13, 21};
        bean.digest = new byte[] {1, 3, 5, 7, 9, 11, 13};
        bean.audit = createAudit(0);

        for (int i = 0; i < childCount; i++) {
            ComplexChild child = createChild(i);
            bean.children.add(child);
            bean.childIndex.put(child.id, child);
        }

        bean.priceMatrix.put(ComplexStatus.ACTIVE, createPrices("USD", 60));
        bean.priceMatrix.put(ComplexStatus.ARCHIVED, createPrices("CNY", 60));
        bean.metadata.put("decimal", new BigDecimal("12345678901234567890.123456789"));
        bean.metadata.put("createdAt", bean.createdAt);
        bean.metadata.put("operatorId", bean.audit.operatorId);
        bean.metadata.put("source", bean.audit.source);
        bean.metadata.put("enabled", Boolean.TRUE);
        bean.metadata.put("count", Long.valueOf(childCount));
        bean.metadata.put("nestedList", new ArrayList<>(List.of("a", "b", "c")));
        bean.metadata.put("nestedMap", new LinkedHashMap<>(Map.of(
                "level", 7,
                "name", "metadata",
                "amount", new BigDecimal("77.07"))));

        for (int row = 0; row < 12; row++) {
            List<Map<String, Object>> rowData = new ArrayList<>();
            for (int col = 0; col < 8; col++) {
                Map<String, Object> cell = new LinkedHashMap<>();
                cell.put("row", row);
                cell.put("col", col);
                cell.put("value", "cell-" + row + "-" + col + "-" + "z".repeat(64));
                rowData.add(cell);
            }
            bean.nestedMatrix.add(rowData);
        }
        return bean;
    }

    private ComplexChild createChild(int index) {
        ComplexChild child = new ComplexChild();
        child.id = "child-" + index;
        child.rank = index;
        child.enabled = index % 3 != 0;
        child.score = BigDecimal.valueOf(index).multiply(new BigDecimal("1.2345"));
        child.aliases = List.of("alias-" + index, "alias-cn-" + index);
        child.attributes = new LinkedHashMap<>();
        child.attributes.put("bucket", "b-" + index % 11);
        child.attributes.put("weight", BigDecimal.valueOf(index * 1000L + 7, 3));
        child.attributes.put("visibleAt", Instant.parse("2026-07-07T12:00:00Z").plusSeconds(index));
        child.attributes.put("flags", new ArrayList<>(List.of("f1", "f2", "f" + index)));
        child.audit = createAudit(index);
        return child;
    }

    private List<ComplexPrice> createPrices(String currency, int count) {
        List<ComplexPrice> prices = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            ComplexPrice price = new ComplexPrice();
            price.currency = currency;
            price.amount = BigDecimal.valueOf(10_000L + i, 2);
            price.discount = i % 2 == 0
                    ? Optional.of(BigDecimal.valueOf(i, 2))
                    : Optional.empty();
            prices.add(price);
        }
        return prices;
    }

    private ComplexAudit createAudit(int index) {
        ComplexAudit audit = new ComplexAudit();
        audit.operatorId = UUID.nameUUIDFromBytes(("operator-" + index).getBytes(java.nio.charset.StandardCharsets.UTF_8));
        audit.source = URI.create("https://example.test/source/" + index);
        audit.happenedAt = Instant.parse("2026-07-07T12:00:00Z").plusSeconds(index);
        audit.labels = new LinkedHashMap<>();
        audit.labels.put("node", "n-" + index % 5);
        audit.labels.put("region", "ap-east-" + index % 3);
        return audit;
    }

    private void assertHugeComplexBean(HugeComplexBean expected, HugeComplexBean actual) {
        assertNotNull(actual);
        assertEquals(expected.id, actual.id);
        assertEquals(expected.name, actual.name);
        assertEquals(expected.status, actual.status);
        assertEquals(expected.createdAt, actual.createdAt);
        assertEquals(expected.updatedAt, actual.updatedAt);
        assertEquals(expected.note, actual.note);
        assertEquals(expected.tags, actual.tags);
        assertArrayEquals(expected.counters, actual.counters);
        assertArrayEquals(expected.digest, actual.digest);
        assertEquals(expected.children.size(), actual.children.size());
        assertEquals(expected.childIndex.size(), actual.childIndex.size());
        assertEquals(expected.priceMatrix.keySet(), actual.priceMatrix.keySet());
        assertEquals(expected.nestedMatrix.size(), actual.nestedMatrix.size());
        assertEquals(expected.audit.operatorId, actual.audit.operatorId);
        assertEquals(expected.audit.source, actual.audit.source);
        assertEquals(expected.audit.happenedAt, actual.audit.happenedAt);

        ComplexChild expectedChild = expected.children.getLast();
        ComplexChild actualChild = actual.children.getLast();
        assertEquals(expectedChild.id, actualChild.id);
        assertEquals(expectedChild.score, actualChild.score);
        assertEquals(expectedChild.aliases, actualChild.aliases);
        assertEquals(expectedChild.attributes.get("bucket"), actualChild.attributes.get("bucket"));
        assertEquals(expectedChild.attributes.get("weight"), actualChild.attributes.get("weight"));
        assertEquals(expectedChild.attributes.get("visibleAt"), actualChild.attributes.get("visibleAt"));
        assertEquals(expectedChild.attributes.get("flags"), actualChild.attributes.get("flags"));

        assertEquals(expected.metadata.get("decimal"), actual.metadata.get("decimal"));
        assertEquals(expected.metadata.get("createdAt"), actual.metadata.get("createdAt"));
        assertEquals(expected.metadata.get("operatorId"), actual.metadata.get("operatorId"));
        assertEquals(expected.metadata.get("source"), actual.metadata.get("source"));
        assertEquals(expected.metadata.get("nestedList"), actual.metadata.get("nestedList"));
        assertEquals(expected.metadata.get("nestedMap"), actual.metadata.get("nestedMap"));
    }
}
