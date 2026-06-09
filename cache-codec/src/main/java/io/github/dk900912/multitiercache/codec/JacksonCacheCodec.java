package io.github.dk900912.multitiercache.codec;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.module.blackbird.BlackbirdModule;
import io.github.dk900912.multitiercache.api.exception.CacheCodecException;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.api.model.CacheMessage;
import io.github.dk900912.multitiercache.api.model.CacheMessageType;
import io.github.dk900912.multitiercache.spi.CacheCodec;

import java.io.IOException;
import java.util.List;

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
                .allowIfBaseType(Object.class)
                .allowIfBaseType(CacheMessage.class)
                .allowIfSubType(CacheMessage.class)
                .allowIfSubType(CacheMessageType.class)
                .allowIfSubType("java.util.")
                .allowIfSubType("java.lang.");

        if (trustedPackages != null && !trustedPackages.isEmpty()) {
            for (String pkg : trustedPackages) {
                ptvBuilder.allowIfSubType(pkg);
            }
        }

        PolymorphicTypeValidator ptv = ptvBuilder.build();
        this.objectMapper.activateDefaultTyping(ptv, ObjectMapper.DefaultTyping.NON_FINAL, JsonTypeInfo.As.PROPERTY);
    }

    @Override
    public String encode(Object obj) throws CacheCodecException {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
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
            return objectMapper.readValue(json, new TypeReference<CacheMessage<T>>() {});
        } catch (JsonProcessingException e) {
            throw new CacheCodecException("Failed to decode CacheMessage", e);
        }
    }

    private void registerModules(ObjectMapper mapper) {
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
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
        mapper.registerModule(module);
    }
}
