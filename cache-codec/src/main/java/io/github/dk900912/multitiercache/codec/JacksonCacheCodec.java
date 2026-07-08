package io.github.dk900912.multitiercache.codec;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.blackbird.BlackbirdModule;
import io.github.dk900912.multitiercache.api.exception.CacheCodecException;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.api.model.CacheMessage;
import io.github.dk900912.multitiercache.api.model.CacheMessageType;
import io.github.dk900912.multitiercache.spi.CacheCodec;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Jackson-based codec responsible for serializing and deserializing cache payloads and messages.
 * <p>
 * Uses Jackson's {@link ObjectMapper} with Blackbird optimization and polymorphic type validation.
 * </p>
 *
 * @author dukui
 */
public class JacksonCacheCodec implements CacheCodec {

    private final ObjectMapper objectMapper;

    public JacksonCacheCodec() {
        this.objectMapper = new ObjectMapper();
        registerModules(this.objectMapper);
    }

    public JacksonCacheCodec(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper.copy();
        registerModules(this.objectMapper);
    }

    @Override
    public void initialize(CacheConfig config) {
        List<String> trustedPackages = config.getCodec().getTrustedPackages();
        
        BasicPolymorphicTypeValidator.Builder ptvBuilder = BasicPolymorphicTypeValidator.builder()
                .allowIfBaseType(CacheMessage.class)
                .allowIfSubType(CacheMessage.class)
                .allowIfSubType(CacheMessageType.class)
                .allowIfSubType("java.math.")
                .allowIfSubType("java.time.")
                .allowIfSubType(ArrayList.class)
                .allowIfSubType(LinkedHashMap.class)
                .allowIfSubType(LinkedHashSet.class)
                .allowIfSubType(String.class)
                .allowIfSubType(Boolean.class)
                .allowIfSubType(Byte.class)
                .allowIfSubType(Short.class)
                .allowIfSubType(Integer.class)
                .allowIfSubType(Long.class)
                .allowIfSubType(Float.class)
                .allowIfSubType(Double.class)
                .allowIfSubType(Character.class)
                .allowIfSubType(URI.class)
                .allowIfSubType(UUID.class);

        if (trustedPackages != null && !trustedPackages.isEmpty()) {
            for (String pkg : trustedPackages) {
                ptvBuilder.allowIfSubType(pkg);
            }
        }

        PolymorphicTypeValidator ptv = ptvBuilder.build();
        this.objectMapper.activateDefaultTyping(
                ptv,
                ObjectMapper.DefaultTyping.JAVA_LANG_OBJECT,
                JsonTypeInfo.As.PROPERTY);
    }

    @Override
    public String encode(Object obj) throws CacheCodecException {
        try {
            return objectMapper.writeValueAsString(normalizeForWire(obj));
        } catch (JsonProcessingException | IllegalArgumentException e) {
            throw new CacheCodecException("Failed to encode object", e);
        }
    }

    @Override
    public <T> T decode(String json, Class<T> clazz) throws CacheCodecException {
        try {
            return objectMapper.readValue(json, clazz);
        } catch (JsonProcessingException e) {
            throw new CacheCodecException("Failed to decode object", e);
        }
    }

    @Override
    public <T> CacheMessage<T> decodeMessage(String json, Class<T> dataClass) throws CacheCodecException {
        try {
            JsonNode root = objectMapper.readTree(json);
            CacheMessage<T> message = new CacheMessage<>();
            message.setKey(requiredText(root, "key"));
            message.setVersion(requiredLong(root, "version"));
            message.setType(CacheMessageType.fromWireValue(requiredText(root, "type")));
            JsonNode ttlMillis = root.get("ttlMillis");
            if (ttlMillis != null && !ttlMillis.isNull()) {
                message.setTtlMillis(ttlMillis.longValue());
            }
            JsonNode data = root.get("data");
            if (data != null && !data.isNull()) {
                message.setData(decodeData(data, dataClass));
            }
            return message;
        } catch (JsonProcessingException | IllegalArgumentException e) {
            throw new CacheCodecException("Failed to decode CacheMessage", e);
        }
    }

    private Object normalizeForWire(Object value) {
        return normalizeForWire(value, Collections.newSetFromMap(new IdentityHashMap<>()));
    }

    private Object normalizeForWire(Object value, Set<Object> visiting) {
        if (value instanceof CacheMessage<?> message) {
            return new CacheMessage<>(
                    message.getKey(),
                    normalizeContainerValue(message.getData(), visiting),
                    message.getVersion(),
                    message.getType(),
                    message.getTtlMillis());
        }
        return normalizeContainerValue(value, visiting);
    }

    private Object normalizeContainerValue(Object value, Set<Object> visiting) {
        if (value instanceof List<?> list) {
            enterContainer(value, visiting);
            try {
                List<Object> normalized = new ArrayList<>(list.size());
                for (Object element : list) {
                    normalized.add(normalizeContainerValue(element, visiting));
                }
                return normalized;
            } finally {
                visiting.remove(value);
            }
        }
        if (value instanceof Set<?> set) {
            enterContainer(value, visiting);
            try {
                Set<Object> normalized = new LinkedHashSet<>(set.size());
                for (Object element : set) {
                    normalized.add(normalizeContainerValue(element, visiting));
                }
                return normalized;
            } finally {
                visiting.remove(value);
            }
        }
        if (value instanceof Map<?, ?> map) {
            enterContainer(value, visiting);
            try {
                Map<Object, Object> normalized = new LinkedHashMap<>(map.size());
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    normalized.put(
                            normalizeContainerValue(entry.getKey(), visiting),
                            normalizeContainerValue(entry.getValue(), visiting));
                }
                return normalized;
            } finally {
                visiting.remove(value);
            }
        }
        return value;
    }

    private void enterContainer(Object value, Set<Object> visiting) {
        if (!visiting.add(value)) {
            throw new IllegalArgumentException("Circular reference in cache payload is not supported");
        }
    }

    private <T> T decodeData(JsonNode data, Class<T> dataClass) throws JsonProcessingException {
        if (dataClass == null || dataClass == Object.class) {
            @SuppressWarnings("unchecked")
            T decoded = (T) objectMapper.treeToValue(data, Object.class);
            return decoded;
        }
        return objectMapper.treeToValue(data, dataClass);
    }

    private String requiredText(JsonNode root, String fieldName) {
        JsonNode node = root.get(fieldName);
        if (node == null || node.isNull()) {
            throw new IllegalArgumentException("CacheMessage's " + fieldName + " cannot be null");
        }
        return node.asText();
    }

    private Long requiredLong(JsonNode root, String fieldName) {
        JsonNode node = root.get(fieldName);
        if (node == null || node.isNull()) {
            throw new IllegalArgumentException("CacheMessage's " + fieldName + " cannot be null");
        }
        return node.longValue();
    }

    private void registerModules(ObjectMapper mapper) {
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapper.registerModule(new Jdk8Module());
        mapper.registerModule(new JavaTimeModule());
        mapper.registerModule(new BlackbirdModule());
        SimpleModule module = new SimpleModule();
        module.addSerializer(CacheMessageType.class, new JsonSerializer<>() {
            @Override
            public void serialize(CacheMessageType value, JsonGenerator generator, SerializerProvider serializers)
                    throws IOException {
                generator.writeString(value.getWireValue());
            }
        });
        module.addDeserializer(CacheMessageType.class, new JsonDeserializer<>() {
            @Override
            public CacheMessageType deserialize(JsonParser parser, DeserializationContext context)
                    throws IOException {
                return CacheMessageType.fromWireValue(parser.getValueAsString());
            }
        });
        registerWireSafeContainerSerializers(module);
        mapper.registerModule(module);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void registerWireSafeContainerSerializers(SimpleModule module) {
        JsonSerializer collectionSerializer = new WireSafeCollectionSerializer();
        for (String className : List.of(
                "java.util.Arrays$ArrayList",
                "java.util.Collections$EmptyList",
                "java.util.Collections$SingletonList",
                "java.util.Collections$UnmodifiableCollection",
                "java.util.Collections$UnmodifiableList",
                "java.util.Collections$UnmodifiableRandomAccessList",
                "java.util.ImmutableCollections$List12",
                "java.util.ImmutableCollections$ListN",
                "java.util.Collections$EmptySet",
                "java.util.Collections$SingletonSet",
                "java.util.Collections$UnmodifiableSet",
                "java.util.ImmutableCollections$Set12",
                "java.util.ImmutableCollections$SetN")) {
            registerSerializerIfPresent(module, className, collectionSerializer);
        }

        JsonSerializer mapSerializer = new WireSafeMapSerializer();
        for (String className : List.of(
                "java.util.Collections$EmptyMap",
                "java.util.Collections$SingletonMap",
                "java.util.Collections$UnmodifiableMap",
                "java.util.ImmutableCollections$Map1",
                "java.util.ImmutableCollections$MapN")) {
            registerSerializerIfPresent(module, className, mapSerializer);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void registerSerializerIfPresent(SimpleModule module, String className, JsonSerializer serializer) {
        try {
            module.addSerializer((Class) Class.forName(className), serializer);
        } catch (ClassNotFoundException ignored) {
            // JDK collection implementation names vary by version; absent classes simply do not need handling.
        }
    }

    private static final class WireSafeCollectionSerializer extends JsonSerializer<Collection<?>> {

        @Override
        public void serialize(Collection<?> value, JsonGenerator generator, SerializerProvider serializers)
                throws IOException {
            EncodingStack.enter(value);
            try {
                serializers.defaultSerializeValue(toStandardCollection(value), generator);
            } finally {
                EncodingStack.exit(value);
            }
        }

        @Override
        public void serializeWithType(Collection<?> value, JsonGenerator generator, SerializerProvider serializers,
                                      TypeSerializer typeSerializer) throws IOException {
            EncodingStack.enter(value);
            try {
                Object standard = toStandardCollection(value);
                JsonSerializer<Object> serializer = serializers.findValueSerializer(standard.getClass(), null);
                serializer.serializeWithType(standard, generator, serializers, typeSerializer);
            } finally {
                EncodingStack.exit(value);
            }
        }

        private Object toStandardCollection(Collection<?> value) {
            return value instanceof Set<?> ? new LinkedHashSet<>(value) : new ArrayList<>(value);
        }
    }

    private static final class WireSafeMapSerializer extends JsonSerializer<Map<?, ?>> {

        @Override
        public void serialize(Map<?, ?> value, JsonGenerator generator, SerializerProvider serializers)
                throws IOException {
            EncodingStack.enter(value);
            try {
                serializers.defaultSerializeValue(new LinkedHashMap<>(value), generator);
            } finally {
                EncodingStack.exit(value);
            }
        }

        @Override
        public void serializeWithType(Map<?, ?> value, JsonGenerator generator, SerializerProvider serializers,
                                      TypeSerializer typeSerializer) throws IOException {
            EncodingStack.enter(value);
            try {
                LinkedHashMap<?, ?> standard = new LinkedHashMap<>(value);
                JsonSerializer<Object> serializer = serializers.findValueSerializer(LinkedHashMap.class, null);
                serializer.serializeWithType(standard, generator, serializers, typeSerializer);
            } finally {
                EncodingStack.exit(value);
            }
        }
    }

    private static final class EncodingStack {

        private static final ThreadLocal<Set<Object>> VISITING =
                ThreadLocal.withInitial(() -> Collections.newSetFromMap(new IdentityHashMap<>()));

        private EncodingStack() {
        }

        static void enter(Object value) {
            if (!VISITING.get().add(value)) {
                throw new IllegalArgumentException("Circular reference in cache payload is not supported");
            }
        }

        static void exit(Object value) {
            Set<Object> visiting = VISITING.get();
            visiting.remove(value);
            if (visiting.isEmpty()) {
                VISITING.remove();
            }
        }
    }
}
