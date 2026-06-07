package io.github.dk900912.multitiercache.provider.caffeine;

import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.FineGrainedExpiry;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.api.model.CacheMessage;
import io.github.dk900912.multitiercache.api.model.CacheMessageType;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class CaffeineL1ProviderTest {

    @Test
    void testBasicPutAndGet() {
        CaffeineL1Provider provider = new CaffeineL1Provider();
        CacheConfig.L1Config config = new CacheConfig.L1Config();
        config.setMaximumSize(100L);
        config.setExpireAfterWrite(Duration.ofMinutes(1));
        provider.initialize(config);

        CacheKey key = CacheKey.simple("caffeine-key");
        provider.put(key, "caffeine-value");
        assertEquals("caffeine-value", provider.get(key));
        
        provider.invalidate(key);
        assertNull(provider.get(key));
    }

    @Test
    void testFineGrainedExpiry() throws InterruptedException {
        CaffeineL1Provider provider = new CaffeineL1Provider();
        CacheConfig.L1Config config = new CacheConfig.L1Config();
        config.setMaximumSize(100L);
        
        // Define fine-grained expiry: key "short" expires in 10ms, key "long" in 1 day
        config.setFineGrainedExpiry(new FineGrainedExpiry<String, Object>() {
            @Override
            public long expireAfterCreate(String key, Object value, long currentTime) {
                if ("short".equals(key)) {
                    return Duration.ofMillis(10).toNanos();
                }
                return Duration.ofDays(1).toNanos();
            }

            @Override
            public long expireAfterUpdate(String key, Object value, long currentTime, long currentDuration) {
                return currentDuration;
            }

            @Override
            public long expireAfterRead(String key, Object value, long currentTime, long currentDuration) {
                return currentDuration;
            }
        });
        
        provider.initialize(config);

        CacheKey shortKey = CacheKey.simple("short");
        CacheKey longKey = CacheKey.simple("long");
        
        provider.put(shortKey, "v1");
        provider.put(longKey, "v2");
        
        // Give it a tiny bit of time to expire the short key
        Thread.sleep(50);
        
        assertNull(provider.get(shortKey), "Short key should have expired");
        assertEquals("v2", provider.get(longKey), "Long key should still be present");
    }

    @Test
    void testComputeWithNewEntry() {
        CaffeineL1Provider provider = new CaffeineL1Provider();
        CacheConfig.L1Config config = new CacheConfig.L1Config();
        config.setMaximumSize(100L);
        provider.initialize(config);

        CacheKey key = CacheKey.simple("compute-key");
        CacheMessage<String> newMessage = createCacheMessage("compute-key", "data1", 1L);

        // Compute on non-existent key should insert new value
        Object result = provider.compute(key, (k, oldValue) -> {
            assertNull(oldValue, "Old value should be null for new entry");
            return newMessage;
        });

        assertEquals(newMessage, result);
        assertEquals(newMessage, provider.get(key));
    }

    @Test
    void testComputeWithVersionUpgrade() {
        CaffeineL1Provider provider = new CaffeineL1Provider();
        CacheConfig.L1Config config = new CacheConfig.L1Config();
        config.setMaximumSize(100L);
        provider.initialize(config);

        CacheKey key = CacheKey.simple("version-key");
        CacheMessage<String> oldMessage = createCacheMessage("version-key", "data1", 1L);
        CacheMessage<String> newMessage = createCacheMessage("version-key", "data2", 2L);

        // Put old version first
        provider.put(key, oldMessage);

        // Compute with version comparison - should upgrade
        Object result = provider.compute(key, (k, oldValue) -> {
            assertNotNull(oldValue, "Old value should exist");
            CacheMessage<?> oldMsg = (CacheMessage<?>) oldValue;
            assertEquals(1L, oldMsg.getVersion());
            
            // Version comparison: upgrade if newVersion > oldVersion
            if (newMessage.getVersion() > oldMsg.getVersion()) {
                return newMessage;
            }
            return oldValue;
        });

        assertEquals(newMessage, result);
        assertEquals(newMessage, provider.get(key));
    }

    @Test
    void testComputeWithVersionDowngrade() {
        CaffeineL1Provider provider = new CaffeineL1Provider();
        CacheConfig.L1Config config = new CacheConfig.L1Config();
        config.setMaximumSize(100L);
        provider.initialize(config);

        CacheKey key = CacheKey.simple("version-key");
        CacheMessage<String> newerMessage = createCacheMessage("version-key", "data2", 2L);
        CacheMessage<String> olderMessage = createCacheMessage("version-key", "data1", 1L);

        // Put newer version first
        provider.put(key, newerMessage);

        // Compute with version comparison - should keep existing (newer) value
        Object result = provider.compute(key, (k, oldValue) -> {
            assertNotNull(oldValue, "Old value should exist");
            CacheMessage<?> oldMsg = (CacheMessage<?>) oldValue;
            assertEquals(2L, oldMsg.getVersion());
            
            // Version comparison: only upgrade if newVersion > oldVersion
            if (olderMessage.getVersion() > oldMsg.getVersion()) {
                return olderMessage;
            }
            return oldValue; // Keep existing newer value
        });

        assertEquals(newerMessage, result);
        assertEquals(newerMessage, provider.get(key));
    }

    @Test
    void testComputeReturningNull() {
        CaffeineL1Provider provider = new CaffeineL1Provider();
        CacheConfig.L1Config config = new CacheConfig.L1Config();
        config.setMaximumSize(100L);
        provider.initialize(config);

        CacheKey key = CacheKey.simple("remove-key");
        CacheMessage<String> message = createCacheMessage("remove-key", "data", 1L);

        // Put a value first
        provider.put(key, message);
        assertNotNull(provider.get(key));

        // Compute returning null should remove the entry
        Object result = provider.compute(key, (k, oldValue) -> null);

        assertNull(result);
        assertNull(provider.get(key));
    }

    @Test
    void testComputeAtomicity() throws InterruptedException {
        CaffeineL1Provider provider = new CaffeineL1Provider();
        CacheConfig.L1Config config = new CacheConfig.L1Config();
        config.setMaximumSize(100L);
        provider.initialize(config);

        CacheKey key = CacheKey.simple("atomic-key");
        int threadCount = 10;
        int iterationsPerThread = 100;
        
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger functionCallCount = new AtomicInteger(0);

        // Multiple threads incrementing a counter atomically
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    startLatch.await(); // Wait for all threads to be ready
                    for (int j = 0; j < iterationsPerThread; j++) {
                        provider.compute(key, (k, oldValue) -> {
                            functionCallCount.incrementAndGet();
                            if (oldValue == null) {
                                return 1;
                            }
                            return ((Integer) oldValue) + 1;
                        });
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown(); // Start all threads simultaneously
        boolean completed = doneLatch.await(10, TimeUnit.SECONDS);

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        // Verify atomicity: final count should be exactly threadCount * iterationsPerThread
        Object finalValue = provider.get(key);
        assertNotNull(finalValue);
        int expectedValue = threadCount * iterationsPerThread;
        assertEquals(expectedValue, finalValue, 
            "Compute should be atomic - expected " + expectedValue + " increments");
        
        // The function call count should match or exceed the expected value
        // (it may be higher due to retries in concurrent scenarios, but the final value should be correct)
        System.out.println("Function called " + functionCallCount.get() + " times for " + expectedValue + " operations");
    }

    private CacheMessage<String> createCacheMessage(String key, String data, Long version) {
        return new CacheMessage<>(key, data, version, CacheMessageType.INSERT, 60000L);
    }
}
