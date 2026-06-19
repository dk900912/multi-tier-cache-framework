package io.github.dk900912.multitiercache.core;

import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.CacheLoader;
import io.github.dk900912.multitiercache.api.CacheManager;
import io.github.dk900912.multitiercache.api.CacheMonitor;
import io.github.dk900912.multitiercache.api.LifecycleManager;
import io.github.dk900912.multitiercache.api.model.CacheMessage;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LifecycleAwareCacheManagerTest {

    @Test
    void shouldBootstrapOnlyOnce() {
        List<String> events = new ArrayList<>();
        RecordingCacheManager delegate = new RecordingCacheManager(events);
        RecordingLifecycleManager first = new RecordingLifecycleManager("first", events);
        RecordingLifecycleManager second = new RecordingLifecycleManager("second", events);
        LifecycleAwareCacheManager manager = new LifecycleAwareCacheManager(delegate, first, second);

        manager.bootstrap();
        manager.bootstrap();

        assertEquals(1, delegate.bootstrapCount);
        assertEquals(1, first.bootstrapCount);
        assertEquals(1, second.bootstrapCount);
    }

    @Test
    void shouldRollbackStartedManagersWhenBootstrapFails() {
        List<String> events = new ArrayList<>();
        RecordingCacheManager delegate = new RecordingCacheManager(events);
        RecordingLifecycleManager first = new RecordingLifecycleManager("first", events);
        RecordingLifecycleManager failing = new RecordingLifecycleManager("failing", events);
        failing.failOnBootstrap = true;
        LifecycleAwareCacheManager manager = new LifecycleAwareCacheManager(delegate, first, failing);

        IllegalStateException exception = assertThrows(IllegalStateException.class, manager::bootstrap);

        assertEquals("failing bootstrap failure", exception.getMessage());
        assertEquals(List.of(
                "delegate.bootstrap",
                "first.bootstrap",
                "failing.bootstrap",
                "failing.shutdown",
                "first.shutdown",
                "delegate.shutdown"
        ), events);
        assertEquals(1, delegate.shutdownCount);
        assertEquals(1, first.shutdownCount);
        assertEquals(1, failing.shutdownCount);
    }

    @Test
    void shouldShutdownDelegateWhenDelegateBootstrapFails() {
        List<String> events = new ArrayList<>();
        RecordingCacheManager delegate = new RecordingCacheManager(events);
        delegate.failOnBootstrap = true;
        LifecycleAwareCacheManager manager = new LifecycleAwareCacheManager(delegate);

        IllegalStateException exception = assertThrows(IllegalStateException.class, manager::bootstrap);

        assertEquals("delegate bootstrap failure", exception.getMessage());
        assertEquals(List.of("delegate.bootstrap", "delegate.shutdown"), events);
        assertEquals(1, delegate.shutdownCount);
    }

    @Test
    void shouldShutdownInReverseOrder() {
        List<String> events = new ArrayList<>();
        RecordingCacheManager delegate = new RecordingCacheManager(events);
        RecordingLifecycleManager first = new RecordingLifecycleManager("first", events);
        RecordingLifecycleManager second = new RecordingLifecycleManager("second", events);
        LifecycleAwareCacheManager manager = new LifecycleAwareCacheManager(delegate, first, second);

        manager.bootstrap();
        events.clear();

        manager.shutdown();

        assertEquals(List.of(
                "second.shutdown",
                "first.shutdown",
                "delegate.shutdown"
        ), events);
    }

    @Test
    void shouldRejectBootstrapAfterShutdown() {
        RecordingCacheManager delegate = new RecordingCacheManager(new ArrayList<>());
        LifecycleAwareCacheManager manager = new LifecycleAwareCacheManager(delegate);

        manager.bootstrap();
        manager.shutdown();

        IllegalStateException exception = assertThrows(IllegalStateException.class, manager::bootstrap);
        assertEquals("CacheManager has already been shut down", exception.getMessage());
    }

    private static final class RecordingCacheManager implements CacheManager {

        private final List<String> events;
        private int bootstrapCount;
        private int shutdownCount;
        private boolean failOnBootstrap;

        private RecordingCacheManager(List<String> events) {
            this.events = events;
        }

        @Override
        public <T> T get(CacheKey key, Supplier<T> loader) {
            return null;
        }

        @Override
        public <T> T get(CacheKey key, Supplier<T> loader, Duration ttl) {
            return null;
        }

        @Override
        public <T> T get(CacheKey key, CacheLoader<T> loader) {
            return null;
        }

        @Override
        public void insert(CacheKey key, Object data, Long version, Duration ttl) {
        }

        @Override
        public void update(CacheKey key, Object data, Long version, Duration ttl) {
        }

        @Override
        public void evict(CacheKey key, Long version, Duration ttl) {
        }

        @Override
        public CacheMonitor getMonitor() {
            return null;
        }

        @Override
        public void apply(CacheMessage<?> message) {
        }

        @Override
        public void bootstrap() {
            bootstrapCount++;
            events.add("delegate.bootstrap");
            if (failOnBootstrap) {
                throw new IllegalStateException("delegate bootstrap failure");
            }
        }

        @Override
        public void shutdown() {
            shutdownCount++;
            events.add("delegate.shutdown");
        }
    }

    private static final class RecordingLifecycleManager implements LifecycleManager {

        private final String name;
        private final List<String> events;
        private int bootstrapCount;
        private int shutdownCount;
        private boolean failOnBootstrap;

        private RecordingLifecycleManager(String name, List<String> events) {
            this.name = name;
            this.events = events;
        }

        @Override
        public void bootstrap() {
            bootstrapCount++;
            events.add(name + ".bootstrap");
            if (failOnBootstrap) {
                throw new IllegalStateException(name + " bootstrap failure");
            }
        }

        @Override
        public void shutdown() {
            shutdownCount++;
            events.add(name + ".shutdown");
        }
    }
}
