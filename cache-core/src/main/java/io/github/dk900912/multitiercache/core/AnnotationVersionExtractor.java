package io.github.dk900912.multitiercache.core;

import io.github.dk900912.multitiercache.api.model.CacheVersion;

import java.lang.reflect.Field;

/**
 * Utility for extracting the version of a cache payload using the {@link io.github.dk900912.multitiercache.api.model.CacheVersion} annotation.
 *
 * @author dukui
 */
public final class AnnotationVersionExtractor {

    private static final ClassValue<Field> FIELD_CACHE = new ClassValue<>() {
        @Override
        protected Field computeValue(Class<?> type) {
            return findCacheVersionField(type);
        }
    };

    public static long extract(Object target) {
        if (target == null) {
            throw new IllegalArgumentException("Target object cannot be null");
        }

        Class<?> targetClass = target.getClass();
        Field versionField = FIELD_CACHE.get(targetClass);

        try {
            Object value = versionField.get(target);
            if (value == null) {
                throw new IllegalStateException("@CacheVersion field '" + versionField.getName()
                        + "' in class " + targetClass.getName() + " cannot be null");
            }
            return ((Number) value).longValue();
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to access @CacheVersion field in class: " + targetClass.getName(), e);
        }
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

        result.setAccessible(true);
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
