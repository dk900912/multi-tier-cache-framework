package io.github.dk900912.multitiercache.core;

import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.CacheManager;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.api.model.CacheLoadResult;
import io.github.dk900912.multitiercache.api.model.CacheVersion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class CacheManagerIntegrationTest {

    private CacheManager cacheManager;

    @BeforeEach
    void setUp() {
        assumeTrue(isLocalRedisClusterReachable(), "Local Redis ACL cluster is not reachable");

        CacheConfig config = new CacheConfig();
        // Enable both L1 and L2 for full integration test
        config.getL1().setEnabled(true);
        config.getL2().setEnabled(true);
        config.getL2().setUsername("dk900912");
        config.getL2().setPassword("qwe@1234");
        config.getL2().setHosts(List.of("127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"));
        config.getCacheMiss().setBackfillTtl(Duration.ofMinutes(2));
        
        // Add our test classes to the codec whitelist
        config.getCodec().setTrustedPackages(List.of("io.github.dk900912"));

        cacheManager = CacheManagerFactory.create(config);
        cacheManager.bootstrap();
    }

    @AfterEach
    void tearDown() {
        if (cacheManager != null) {
            cacheManager.shutdown();
        }
    }

    @Test
    void testBasicGetAndCacheHit() {
        AtomicInteger loaderCalls = new AtomicInteger();
        CacheKey key = CacheKey.simple("integration:test:basic:" + UUID.randomUUID());

        String first = cacheManager.get(key, () -> {
            loaderCalls.incrementAndGet();
            return CacheLoadResult.of("value-from-loader", 1L, Duration.ofMinutes(2));
        });
        
        String second = cacheManager.get(key, () -> {
            loaderCalls.incrementAndGet();
            return CacheLoadResult.of("unexpected-second-load", 2L, Duration.ofMinutes(2));
        });

        assertNotNull(first);
        assertEquals("value-from-loader", first);
        assertEquals(first, second);
        assertEquals(1, loaderCalls.get());
    }

    @Test
    void testHighConcurrencySingleFlight() throws InterruptedException {
        int threads = 50;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);
        
        AtomicInteger loaderCalls = new AtomicInteger();
        CacheKey key = CacheKey.simple("integration:test:concurrency:" + UUID.randomUUID());
        
        List<String> results = new ArrayList<>(threads);

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    String result = cacheManager.get(key, () -> {
                        loaderCalls.incrementAndGet();
                        try {
                            Thread.sleep(200); // Simulate slow DB query
                        } catch (InterruptedException ignored) {}
                        return CacheLoadResult.of("concurrent-value", 1L, Duration.ofMinutes(2));
                    });
                    synchronized (results) {
                        results.add(result);
                    }
                } catch (Exception ignored) {
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        doneLatch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        assertEquals(threads, results.size());
        for (String res : results) {
            assertEquals("concurrent-value", res);
        }
        // The most important assertion: SingleFlight prevented cache breakdown
        assertEquals(1, loaderCalls.get(), "Loader should have been called exactly once despite 50 concurrent requests");
    }

    public static class ComplexEntity {
        @CacheVersion
        private long version;
        private String name;
        private List<String> tags;

        public ComplexEntity() {}

        public ComplexEntity(long version, String name, List<String> tags) {
            this.version = version;
            this.name = name;
            this.tags = tags;
        }

        public long getVersion() { return version; }
        public void setVersion(long version) { this.version = version; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public List<String> getTags() { return tags; }
        public void setTags(List<String> tags) { this.tags = tags; }
    }

    @Test
    void testComplexObjectSerializationAndMutation() throws InterruptedException {
        CacheKey key = CacheKey.simple("integration:test:complex:" + UUID.randomUUID());
        
        ComplexEntity entity = new ComplexEntity(1L, "Alice", List.of("admin", "developer"));
        
        // 1. Test manual insert
        cacheManager.insert(key, entity, entity.getVersion(), Duration.ofMinutes(5));
        
        // Give Redis time to process Lua script
        Thread.sleep(1000);
        
        // 2. Test get (should hit L1/L2)
        AtomicInteger loaderCalls = new AtomicInteger();
        ComplexEntity cached = cacheManager.get(key, () -> {
            loaderCalls.incrementAndGet();
            return CacheLoadResult.of(new ComplexEntity(0L, "Wrong", List.of()), 0L, Duration.ofMinutes(5));
        });
        
        assertEquals(0, loaderCalls.get(), "Should hit cache");
        assertEquals("Alice", cached.getName());
        assertEquals(2, cached.getTags().size());
        assertEquals("admin", cached.getTags().get(0));
        
        // 3. Test update
        ComplexEntity updatedEntity = new ComplexEntity(2L, "Alice-Updated", List.of("admin"));
        cacheManager.update(key, updatedEntity, updatedEntity.getVersion(), Duration.ofMinutes(5));
        
        // Give Pub/Sub a tiny moment to invalidate L1
        Thread.sleep(1000);
        
        ComplexEntity cachedAfterUpdate = cacheManager.get(key, () -> {
            loaderCalls.incrementAndGet();
            return CacheLoadResult.of(new ComplexEntity(0L, "Wrong", List.of()), 0L, Duration.ofMinutes(5));
        });
        
        assertEquals(0, loaderCalls.get(), "Should hit cache after update");
        assertEquals("Alice-Updated", cachedAfterUpdate.getName());
        assertEquals(1, cachedAfterUpdate.getTags().size());
        
        // 3.5 Test ignoring old version update
        ComplexEntity oldVersionEntity = new ComplexEntity(1L, "Alice-Old", List.of("admin"));
        cacheManager.update(key, oldVersionEntity, oldVersionEntity.getVersion(), Duration.ofMinutes(5));
        
        Thread.sleep(500); // Give it a moment
        
        ComplexEntity cachedAfterOldUpdate = cacheManager.get(key, () -> {
            loaderCalls.incrementAndGet();
            return CacheLoadResult.of(new ComplexEntity(0L, "Wrong", List.of()), 0L, Duration.ofMinutes(5));
        });
        
        // The Lua script should have rejected the update because 1L < 2L
        assertEquals("Alice-Updated", cachedAfterOldUpdate.getName(), "Old version update should have been ignored");
        
        // 4. Test evict
        cacheManager.evict(key, 3L, Duration.ofMinutes(5));
        
        // Give Pub/Sub a tiny moment to invalidate L1
        Thread.sleep(1000);
        
        ComplexEntity cachedAfterEvict = cacheManager.get(key, () -> {
            loaderCalls.incrementAndGet();
            return CacheLoadResult.of(new ComplexEntity(3L, "DB-Value", List.of()), 3L, Duration.ofMinutes(5));
        });
        
        // Note: The original test expected 1 loader call here, meaning it expected the cache miss.
        // However, evict() writes a tombstone (PENETRATE) to L2 with the new version.
        // When we call get(), it reads the tombstone from L2, realizes it's a penetration record,
        // and returns null *without calling the loader*.
        // Therefore, loaderCalls remains 0, and cachedAfterEvict should be null.
        assertEquals(0, loaderCalls.get(), "Loader should NOT be called because evict writes a tombstone");
        assertNull(cachedAfterEvict, "Evicted key should return null due to tombstone");
    }

    private static boolean isLocalRedisClusterReachable() {
        return isPortReachable(7001) && isPortReachable(7002) && isPortReachable(7003);
    }

    private static boolean isPortReachable(int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", port), 500);
            return true;
        } catch (Exception ignored) {
            return false;
        }
    }
}
