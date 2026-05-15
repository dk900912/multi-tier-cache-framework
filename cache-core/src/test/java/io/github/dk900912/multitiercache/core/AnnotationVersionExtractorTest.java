package io.github.dk900912.multitiercache.core;

import io.github.dk900912.multitiercache.api.model.CacheVersion;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AnnotationVersionExtractorTest {

    @Test
    void shouldExtractPrimitiveLongVersion() {
        long version = AnnotationVersionExtractor.extract(new PrimitiveVersionEntity(7L));
        assertEquals(7L, version);
    }

    @Test
    void shouldFailFastWhenBoxedVersionFieldIsNull() {
        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> AnnotationVersionExtractor.extract(new BoxedVersionEntity(null))
        );

        assertEquals(
                "@CacheVersion field 'version' in class "
                        + BoxedVersionEntity.class.getName()
                        + " cannot be null",
                exception.getMessage()
        );
    }

    private static final class PrimitiveVersionEntity {
        @CacheVersion
        private final long version;

        private PrimitiveVersionEntity(long version) {
            this.version = version;
        }
    }

    private static final class BoxedVersionEntity {
        @CacheVersion
        private final Long version;

        private BoxedVersionEntity(Long version) {
            this.version = version;
        }
    }
}
