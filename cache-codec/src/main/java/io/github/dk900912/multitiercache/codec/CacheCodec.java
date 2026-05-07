package io.github.dk900912.multitiercache.codec;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.module.blackbird.BlackbirdModule;
import io.github.dk900912.multitiercache.api.model.CacheMessage;
import io.github.dk900912.multitiercache.api.model.CacheMessageType;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Codec responsible for serializing and deserializing cache payloads and messages.
 * <p>
 * Uses Jackson's {@link ObjectMapper} with Blackbird optimization and a class cache.
 * </p>
 *
 * @author dukui
 */
public class CacheCodec {

    private final ObjectMapper objectMapper;
    private final Map<String, Class<?>> classCache = new ConcurrentHashMap<>();

    public CacheCodec() {
        this.objectMapper = new ObjectMapper();
        registerModules(this.objectMapper);
    }

    public CacheCodec(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper.copy();
        registerModules(this.objectMapper);
    }

    public String encode(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to encode object", e);
        }
    }

    public <T> T decode(String json, Class<T> clazz) {
        try {
            return objectMapper.readValue(json, clazz);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to decode object", e);
        }
    }

    public <T> CacheMessage<T> decodeMessage(String json, Class<T> dataClass) {
        try {
            JsonNode root = objectMapper.readTree(json);
            return decodeMessageNode(root, dataClass);
        } catch (IOException e) {
            throw new RuntimeException("Failed to decode CacheMessage", e);
        }
    }

    private void registerModules(ObjectMapper mapper) {
        mapper.registerModule(new BlackbirdModule());
        SimpleModule module = new SimpleModule();
        registerCacheMessageCodec(module);
        module.addSerializer(CacheMessageType.class, new JsonSerializer<>() {
            @Override
            public void serialize(CacheMessageType value, JsonGenerator generator, SerializerProvider serializers)
                    throws IOException {
                generator.writeString(value.getWireValue());
            }
        });
        module.addDeserializer(CacheMessageType.class, new JsonDeserializer<>() {
            @Override
            public CacheMessageType deserialize(JsonParser parser, DeserializationContext context) throws IOException {
                return parseMessageType(parser.getValueAsString());
            }
        });
        mapper.registerModule(module);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void registerCacheMessageCodec(SimpleModule module) {
        module.addSerializer((Class) CacheMessage.class, new JsonSerializer<CacheMessage<?>>() {
            @Override
            public void serialize(CacheMessage<?> value, JsonGenerator generator, SerializerProvider serializers)
                    throws IOException {
                generator.writeStartObject();
                generator.writeStringField("key", value.getKey());
                generator.writeFieldName("data");
                writeTypedData(generator, value.getData());
                writeNullableNumber(generator, "version", value.getVersion());
                generator.writeStringField("type", value.getType().getWireValue());
                writeNullableNumber(generator, "ttlMillis", value.getTtlMillis());
                generator.writeEndObject();
            }
        });

        module.addDeserializer((Class) CacheMessage.class, new JsonDeserializer<CacheMessage<?>>() {
            @Override
            public CacheMessage<?> deserialize(JsonParser parser, DeserializationContext context) throws IOException {
                return decodeMessageNode(parser.getCodec().readTree(parser), Object.class);
            }
        });
    }

    private void writeTypedData(JsonGenerator generator, Object data) throws IOException {
        if (data == null) {
            generator.writeNull();
            return;
        }

        generator.writeStartArray();
        generator.writeString(data.getClass().getName());
        generator.writeObject(data);
        generator.writeEndArray();
    }

    private void writeNullableNumber(JsonGenerator generator, String fieldName, Long value) throws IOException {
        generator.writeFieldName(fieldName);
        if (value == null) {
            generator.writeNull();
        } else {
            generator.writeNumber(value);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> CacheMessage<T> decodeMessageNode(JsonNode root, Class<T> dataClass) throws IOException {
        CacheMessage<T> message = new CacheMessage<>();
        message.setKey(readText(root.get("key")));
        message.setData(readData(root.get("data"), dataClass));
        message.setVersion(readLong(root.get("version")));
        message.setType(parseMessageType(readText(root.get("type"))));
        message.setTtlMillis(readLong(root.get("ttlMillis")));
        return message;
    }

    @SuppressWarnings("unchecked")
    private <T> T readData(JsonNode dataNode, Class<T> dataClass) throws IOException {
        if (dataNode == null || dataNode.isNull()) {
            return null;
        }

        if (isTypedDataNode(dataNode)) {
            Class<?> payloadClass = resolveClass(dataNode.get(0).asText());
            Object data = objectMapper.treeToValue(dataNode.get(1), payloadClass);
            return (T) data;
        }

        Class<T> targetClass = dataClass == null ? (Class<T>) Object.class : dataClass;
        return objectMapper.treeToValue(dataNode, targetClass);
    }

    private boolean isTypedDataNode(JsonNode dataNode) {
        return dataNode.isArray()
                && dataNode.size() == 2
                && dataNode.get(0).isTextual();
    }

    private Class<?> resolveClass(String className) throws IOException {
        Class<?> clazz = classCache.get(className);
        if (clazz != null) {
            return clazz;
        }
        try {
            clazz = Class.forName(className);
            classCache.put(className, clazz);
            return clazz;
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to resolve cache payload class: " + className, e);
        }
    }

    private String readText(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        return node.asText();
    }

    private Long readLong(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        return node.asLong();
    }

    private CacheMessageType parseMessageType(String wireValue) {
        if (wireValue == null) {
            throw new IllegalArgumentException("CacheMessageType cannot be null");
        }

        String normalized = wireValue.toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "insert" -> CacheMessageType.INSERT;
            case "update" -> CacheMessageType.UPDATE;
            case "delete" -> CacheMessageType.DELETE;
            case "penetrate" -> CacheMessageType.PENETRATE;
            case "backfill" -> CacheMessageType.BACKFILL;
            default -> throw new IllegalArgumentException("Unsupported CacheMessageType: " + wireValue);
        };
    }
}
