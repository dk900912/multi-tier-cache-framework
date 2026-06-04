package io.github.dk900912.multitiercache.core;

import io.github.dk900912.multitiercache.api.model.CacheVersion;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;

/**
 * Utility for extracting the version of a cache payload using the {@link io.github.dk900912.multitiercache.api.model.CacheVersion} annotation.
 *
 * @author dukui
 */
public final class AnnotationVersionExtractor {

    private static final ClassValue<Extractor> EXTRACTOR_CACHE = new ClassValue<>() {
        @Override
        protected Extractor computeValue(Class<?> type) {
            Field field = findCacheVersionField(type);
            boolean isPrimitive = field.getType() == long.class;
            try {
                VarHandle handle = MethodHandles.privateLookupIn(field.getDeclaringClass(), MethodHandles.lookup())
                        .unreflectVarHandle(field);
                return new Extractor(handle, isPrimitive, field.getName(), type.getName());
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Failed to un-reflect @CacheVersion field in class: " + type.getName(), e);
            }
        }
    };

    private static final class Extractor {
        final VarHandle handle;
        final boolean isPrimitive;
        final String fieldName;
        final String className;

        Extractor(VarHandle handle, boolean isPrimitive, String fieldName, String className) {
            this.handle = handle;
            this.isPrimitive = isPrimitive;
            this.fieldName = fieldName;
            this.className = className;
        }

        long extract(Object target) {
            if (isPrimitive) {
                return (long) handle.get(target);
            } else {
                Object value = handle.get(target);
                if (value == null) {
                    throw new IllegalStateException("@CacheVersion field '" + fieldName
                            + "' in class " + className + " cannot be null");
                }
                return (Long) value;
            }
        }
    }

    private AnnotationVersionExtractor() {
        throw new UnsupportedOperationException("Utility class");
    }

    public static long extract(Object target) {
        if (target == null) {
            throw new IllegalArgumentException("Target object cannot be null");
        }

        return EXTRACTOR_CACHE.get(target.getClass()).extract(target);
    }

    private static Field findCacheVersionField(Class<?> type) {
        Field result = getField(type);

        if (result == null) {
            throw new RuntimeException("No field annotated with @CacheVersion found in class hierarchy of: " + type.getName());
        }

        Class<?> fieldType = result.getType();
        if (fieldType != long.class && fieldType != Long.class) {
            throw new IllegalStateException("@CacheVersion can only be applied to long or Long type. Invalid field: " + result.getName());
        }

        return result;
    }

    private static Field getField(Class<?> type) {
        Field result = null;
        Class<?> currentClass = type;
        while (currentClass != null && currentClass != Object.class) {
            for (Field field : currentClass.getDeclaredFields()) {
                if (field.isAnnotationPresent(CacheVersion.class)) {
                    if (result != null) {
                        throw new IllegalStateException("Ambiguous @CacheVersion declaration: "
                                + type.getName() + " has multiple annotated fields.");
                    }
                    result = field;
                }
            }
            currentClass = currentClass.getSuperclass();
        }
        return result;
    }
}
