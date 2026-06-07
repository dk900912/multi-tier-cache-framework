package io.github.dk900912.multitiercache.core;

import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.CacheManager;
import io.github.dk900912.multitiercache.api.model.CacheMessageType;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.api.model.CacheLoadResult;
import io.github.dk900912.multitiercache.api.model.CacheMessage;
import io.github.dk900912.multitiercache.api.model.CacheVersion;
import io.github.dk900912.multitiercache.codec.JacksonCacheCodec;
import io.github.dk900912.multitiercache.provider.jedis.JedisL2Provider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class CacheManagerIntegrationTest {

    private CacheManager cacheManager;
    private final List<CacheManager> cacheManagers = new ArrayList<>();

    @BeforeEach
    void setUp() {
        assumeTrue(isLocalRedisClusterReachable(), "Local Redis ACL cluster is not reachable");
        cacheManager = createManager();
    }

    @AfterEach
    void tearDown() {
        for (int i = cacheManagers.size() - 1; i >= 0; i--) {
            try {
                cacheManagers.get(i).shutdown();
            } catch (Exception ignored) {
            }
        }
        cacheManagers.clear();
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

    @Test
    void testReinsertOverridesOlderLifecycleDeleteTombstone() throws InterruptedException {
        CacheKey key = CacheKey.simple("integration:test:reinsert:" + UUID.randomUUID());

        cacheManager.insert(key, new ComplexEntity(1L, "Alice", List.of("v1")), 1L, Duration.ofMinutes(5));
        Thread.sleep(300);

        cacheManager.evict(key, 100L, Duration.ofMinutes(5));
        Thread.sleep(300);

        ComplexEntity reborn = new ComplexEntity(1L, "Alice-Reborn", List.of("v2"));
        cacheManager.insert(key, reborn, reborn.getVersion(), Duration.ofMinutes(5));
        Thread.sleep(500);

        AtomicInteger loaderCalls = new AtomicInteger();
        ComplexEntity cached = cacheManager.get(key, () -> {
            loaderCalls.incrementAndGet();
            return CacheLoadResult.of(new ComplexEntity(999L, "Unexpected", List.of()), 999L, Duration.ofMinutes(5));
        });

        assertEquals(0, loaderCalls.get(), "Reinsert should override old delete tombstone without hitting DB");
        assertNotNull(cached);
        assertEquals("Alice-Reborn", cached.getName());
    }

    @Test
    void testConcurrentDeleteWinsOverDelayedBackfill() throws Exception {
        CacheKey key = CacheKey.simple("integration:test:delete-race:" + UUID.randomUUID());
        CountDownLatch loaderStarted = new CountDownLatch(1);
        CountDownLatch allowLoaderFinish = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<ComplexEntity> staleRead = executor.submit(() -> cacheManager.get(key, () -> {
                loaderStarted.countDown();
                try {
                    allowLoaderFinish.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
                return CacheLoadResult.of(new ComplexEntity(1L, "stale-before-delete", List.of("old")), 1L, Duration.ofMinutes(5));
            }));

            assertTrue(loaderStarted.await(5, TimeUnit.SECONDS), "Loader should start before delete");
            cacheManager.evict(key, 100L, Duration.ofMinutes(5));
            allowLoaderFinish.countDown();

            staleRead.get(5, TimeUnit.SECONDS);
            Thread.sleep(500);

            AtomicInteger loaderCalls = new AtomicInteger();
            ComplexEntity cachedAfterRace = cacheManager.get(key, () -> {
                loaderCalls.incrementAndGet();
                return CacheLoadResult.of(new ComplexEntity(200L, "unexpected", List.of()), 200L, Duration.ofMinutes(5));
            });

            assertNull(cachedAfterRace, "Delete tombstone should survive delayed stale backfill");
            assertEquals(0, loaderCalls.get(), "Delete tombstone should prevent another DB load");
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testReplicaLagBackfillCannotOverrideNewerMutation() throws Exception {
        CacheKey key = CacheKey.simple("integration:test:replica-lag:" + UUID.randomUUID());
        CountDownLatch loaderStarted = new CountDownLatch(1);
        CountDownLatch allowLoaderFinish = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<ComplexEntity> staleRead = executor.submit(() -> cacheManager.get(key, () -> {
                loaderStarted.countDown();
                try {
                    allowLoaderFinish.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
                return CacheLoadResult.of(new ComplexEntity(4L, "replica-stale", List.of("stale")), 4L, Duration.ofMinutes(5));
            }));

            assertTrue(loaderStarted.await(5, TimeUnit.SECONDS), "Loader should start before the fresh write");
            ComplexEntity fresh = new ComplexEntity(5L, "primary-fresh", List.of("fresh"));
            cacheManager.insert(key, fresh, fresh.getVersion(), Duration.ofMinutes(5));
            allowLoaderFinish.countDown();

            staleRead.get(5, TimeUnit.SECONDS);
            Thread.sleep(500);

            AtomicInteger loaderCalls = new AtomicInteger();
            ComplexEntity cached = cacheManager.get(key, () -> {
                loaderCalls.incrementAndGet();
                return CacheLoadResult.of(new ComplexEntity(999L, "unexpected", List.of()), 999L, Duration.ofMinutes(5));
            });

            assertEquals(0, loaderCalls.get(), "Newer mutation should stay in cache after stale backfill");
            assertNotNull(cached);
            assertEquals("primary-fresh", cached.getName());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testDuplicateAndOutOfOrderMutationMessagesAreIgnored() throws InterruptedException {
        CacheKey key = CacheKey.simple("integration:test:duplicate-out-of-order:" + UUID.randomUUID());
        ComplexEntity initial = new ComplexEntity(1L, "Alice", List.of("v1"));
        cacheManager.insert(key, initial, initial.getVersion(), Duration.ofMinutes(5));
        Thread.sleep(300);

        CacheMessage<ComplexEntity> duplicate = new CacheMessage<>(
                key.toKeyString(),
                new ComplexEntity(1L, "Wrong-Duplicate", List.of("dup")),
                1L,
                1L,
                io.github.dk900912.multitiercache.api.model.CacheMessageType.UPDATE,
                Duration.ofMinutes(5).toMillis()
        );
        cacheManager.apply(duplicate);

        CacheMessage<ComplexEntity> outOfOrderOlder = new CacheMessage<>(
                key.toKeyString(),
                new ComplexEntity(0L, "Wrong-Old", List.of("old")),
                1L,
                0L,
                io.github.dk900912.multitiercache.api.model.CacheMessageType.UPDATE,
                Duration.ofMinutes(5).toMillis()
        );
        cacheManager.apply(outOfOrderOlder);
        Thread.sleep(500);

        AtomicInteger loaderCalls = new AtomicInteger();
        ComplexEntity cached = cacheManager.get(key, () -> {
            loaderCalls.incrementAndGet();
            return CacheLoadResult.of(new ComplexEntity(999L, "unexpected", List.of()), 999L, Duration.ofMinutes(5));
        });

        assertEquals(0, loaderCalls.get());
        assertNotNull(cached);
        assertEquals("Alice", cached.getName(), "Duplicate or out-of-order messages must not overwrite current value");
    }

    @Test
    void testCrossNodeWriteStrategyMatchesDesignUnderPressure() throws Exception {
        CacheManager otherNode = createManager();

        for (int i = 0; i < 20; i++) {
            final int round = i;
            CacheKey key = CacheKey.simple("integration:test:cross-node:" + i + ":" + UUID.randomUUID());

            ComplexEntity inserted = new ComplexEntity(1L, "user-" + round, List.of("insert"));
            cacheManager.insert(key, inserted, inserted.getVersion(), Duration.ofMinutes(5));

            waitUntil(() -> {
                try {
                    CacheMessage<Object> message = readL2Message(key);
                    return message != null
                            && message.getType() == CacheMessageType.INSERT
                            && message.getGeneration() == 1L
                            && message.getVersion() == 1L;
                } catch (Exception e) {
                    return false;
                }
            }, "insert should materialize in L2 with generation=1");

            AtomicInteger nodeBLoaderCalls = new AtomicInteger();
            ComplexEntity fromNodeB = otherNode.get(key, () -> {
                nodeBLoaderCalls.incrementAndGet();
                return CacheLoadResult.of(new ComplexEntity(999L, "unexpected", List.of()), 999L, Duration.ofMinutes(5));
            });
            assertEquals(0, nodeBLoaderCalls.get(), "Node B should backfill from L2 rather than hit DB");
            assertEquals("user-" + round, fromNodeB.getName());

            ComplexEntity updated = new ComplexEntity(2L, "user-" + round + "-updated", List.of("update"));
            cacheManager.update(key, updated, updated.getVersion(), Duration.ofMinutes(5));

            waitUntil(() -> {
                try {
                    ComplexEntity latest = otherNode.get(key, () -> {
                        throw new AssertionError("L2 hit should satisfy update propagation");
                    });
                    return latest != null && ("user-" + round + "-updated").equals(latest.getName());
                } catch (Exception e) {
                    return false;
                }
            }, "remote L1 should eventually converge to updated value via invalidation + L2 refill");

            CacheMessage<Object> updatedMessage = readL2Message(key);
            assertNotNull(updatedMessage);
            assertEquals(CacheMessageType.UPDATE, updatedMessage.getType());
            assertEquals(1L, updatedMessage.getGeneration());
            assertEquals(2L, updatedMessage.getVersion());

            cacheManager.evict(key, 100L, Duration.ofMinutes(5));
            waitUntil(() -> {
                try {
                    CacheMessage<Object> message = readL2Message(key);
                    return message != null
                            && message.getType() == CacheMessageType.DELETE
                            && message.getGeneration() == 1L
                            && message.getVersion() == 100L;
                } catch (Exception e) {
                    return false;
                }
            }, "delete should materialize as tombstone in L2");

            AtomicInteger deleteLoaderCalls = new AtomicInteger();
            waitUntil(() -> {
                try {
                    ComplexEntity deleted = otherNode.get(key, () -> {
                        deleteLoaderCalls.incrementAndGet();
                        return CacheLoadResult.of(new ComplexEntity(888L, "unexpected", List.of()), 888L, Duration.ofMinutes(5));
                    });
                    return deleted == null;
                } catch (Exception e) {
                    return false;
                }
            }, "remote node should eventually observe delete tombstone");
            assertEquals(0, deleteLoaderCalls.get(), "Delete tombstone should block DB reloads");

            ComplexEntity reborn = new ComplexEntity(1L, "user-" + round + "-reborn", List.of("reinsert"));
            cacheManager.insert(key, reborn, reborn.getVersion(), Duration.ofMinutes(5));

            waitUntil(() -> {
                try {
                    CacheMessage<Object> message = readL2Message(key);
                    return message != null
                            && message.getType() == CacheMessageType.INSERT
                            && message.getGeneration() == 2L
                            && message.getVersion() == 1L;
                } catch (Exception e) {
                    return false;
                }
            }, "reinsert should bump generation and override prior delete lifecycle");

            AtomicInteger rebornLoaderCalls = new AtomicInteger();
            waitUntil(() -> {
                try {
                    ComplexEntity rebornFromNodeB = otherNode.get(key, () -> {
                        rebornLoaderCalls.incrementAndGet();
                        return CacheLoadResult.of(new ComplexEntity(777L, "unexpected", List.of()), 777L, Duration.ofMinutes(5));
                    });
                    return rebornFromNodeB != null && ("user-" + round + "-reborn").equals(rebornFromNodeB.getName());
                } catch (Exception e) {
                    return false;
                }
            }, "remote node should eventually converge to reborn value after reinsert");
            assertEquals(0, rebornLoaderCalls.get(), "Reinsert should still be served from cache");
        }
    }

    @Test
    void testPenetrateReadStrategyMatchesDesignUnderPressure() throws Exception {
        CacheManager otherNode = createManager();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            for (int i = 0; i < 20; i++) {
                final int round = i;
                CacheKey insertRaceKey = CacheKey.simple("integration:test:penetrate-insert:" + i + ":" + UUID.randomUUID());
                runDelayedReadRace(
                        executor,
                        () -> cacheManager.get(insertRaceKey, () -> {
                            return CacheLoadResult.penetration(Duration.ofSeconds(5));
                        }),
                        () -> otherNode.insert(insertRaceKey, new ComplexEntity(1L, "value-" + round, List.of("fresh")), 1L, Duration.ofMinutes(5))
                );

                waitUntil(() -> {
                    try {
                        CacheMessage<Object> message = readL2Message(insertRaceKey);
                        return message != null
                                && message.getType() == CacheMessageType.INSERT
                                && message.getGeneration() == 1L
                                && message.getVersion() == 1L;
                    } catch (Exception e) {
                        return false;
                    }
                }, "penetrate must not override real value");

                AtomicInteger valueLoaderCalls = new AtomicInteger();
                ComplexEntity afterInsertRace = cacheManager.get(insertRaceKey, () -> {
                    valueLoaderCalls.incrementAndGet();
                    return CacheLoadResult.of(new ComplexEntity(999L, "unexpected", List.of()), 999L, Duration.ofMinutes(5));
                });
                assertNotNull(afterInsertRace);
                assertEquals("value-" + round, afterInsertRace.getName());
                assertEquals(0, valueLoaderCalls.get());

                CacheKey deleteRaceKey = CacheKey.simple("integration:test:penetrate-delete:" + i + ":" + UUID.randomUUID());
                runDelayedReadRace(
                        executor,
                        () -> cacheManager.get(deleteRaceKey, () -> CacheLoadResult.penetration(Duration.ofSeconds(5))),
                        () -> otherNode.evict(deleteRaceKey, 100L + round, Duration.ofMinutes(5))
                );

                waitUntil(() -> {
                    try {
                        CacheMessage<Object> message = readL2Message(deleteRaceKey);
                        return message != null
                                && message.getType() == CacheMessageType.DELETE
                                && message.getGeneration() == 1L
                                && message.getVersion() == 100L + round;
                    } catch (Exception e) {
                        return false;
                    }
                }, "penetrate must not override delete tombstone");

                AtomicInteger tombstoneLoaderCalls = new AtomicInteger();
                ComplexEntity afterDeleteRace = cacheManager.get(deleteRaceKey, () -> {
                    tombstoneLoaderCalls.incrementAndGet();
                    return CacheLoadResult.of(new ComplexEntity(555L, "unexpected", List.of()), 555L, Duration.ofMinutes(5));
                });
                assertNull(afterDeleteRace);
                assertEquals(0, tombstoneLoaderCalls.get());
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testBackfillReadStrategyMatchesDesignAcrossNodes() throws Exception {
        CacheManager otherNode = createManager();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            for (int i = 0; i < 20; i++) {
                final int round = i;
                CacheKey key = CacheKey.simple("integration:test:backfill-race:" + i + ":" + UUID.randomUUID());
                CountDownLatch loaderStarted = new CountDownLatch(1);
                CountDownLatch allowLoaderFinish = new CountDownLatch(1);

                Future<ComplexEntity> staleRead = executor.submit(() -> otherNode.get(key, () -> {
                    loaderStarted.countDown();
                    try {
                        assertTrue(allowLoaderFinish.await(5, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    }
                    return CacheLoadResult.of(new ComplexEntity(4L, "stale-" + round, List.of("stale")), 4L, Duration.ofMinutes(5));
                }));

                assertTrue(loaderStarted.await(5, TimeUnit.SECONDS));
                ComplexEntity fresh = new ComplexEntity(5L, "fresh-" + round, List.of("fresh"));
                cacheManager.insert(key, fresh, fresh.getVersion(), Duration.ofMinutes(5));
                allowLoaderFinish.countDown();

                staleRead.get(5, TimeUnit.SECONDS);

                waitUntil(() -> {
                    try {
                        CacheMessage<Object> message = readL2Message(key);
                        return message != null
                                && message.getType() == CacheMessageType.INSERT
                                && message.getGeneration() == 1L
                                && message.getVersion() == 5L;
                    } catch (Exception e) {
                        return false;
                    }
                }, "stale backfill must not override fresher mutation in L2");

                AtomicInteger loaderCalls = new AtomicInteger();
                ComplexEntity cached = otherNode.get(key, () -> {
                    loaderCalls.incrementAndGet();
                    return CacheLoadResult.of(new ComplexEntity(999L, "unexpected", List.of()), 999L, Duration.ofMinutes(5));
                });

                assertNotNull(cached);
                assertEquals("fresh-" + round, cached.getName());
                assertEquals(0, loaderCalls.get(), "After failed stale backfill, node should refill from authoritative L2");
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testDuplicateAndOutOfOrderWritePressureLoop() throws Exception {
        for (int i = 0; i < 50; i++) {
            CacheKey key = CacheKey.simple("integration:test:order-loop:" + i + ":" + UUID.randomUUID());

            cacheManager.insert(key, new ComplexEntity(1L, "v1", List.of()), 1L, Duration.ofMinutes(5));
            cacheManager.update(key, new ComplexEntity(2L, "v2", List.of()), 2L, Duration.ofMinutes(5));
            cacheManager.update(key, new ComplexEntity(1L, "stale-v1", List.of()), 1L, Duration.ofMinutes(5));
            cacheManager.apply(new CacheMessage<>(
                    key.toKeyString(),
                    new ComplexEntity(2L, "duplicate-v2", List.of()),
                    1L,
                    2L,
                    CacheMessageType.UPDATE,
                    Duration.ofMinutes(5).toMillis()
            ));
            cacheManager.evict(key, 3L, Duration.ofMinutes(5));
            cacheManager.apply(new CacheMessage<>(
                    key.toKeyString(),
                    new ComplexEntity(2L, "late-v2", List.of()),
                    1L,
                    2L,
                    CacheMessageType.UPDATE,
                    Duration.ofMinutes(5).toMillis()
            ));

            waitUntil(() -> {
                try {
                    CacheMessage<Object> message = readL2Message(key);
                    return message != null
                            && message.getType() == CacheMessageType.DELETE
                            && message.getGeneration() == 1L
                            && message.getVersion() == 3L;
                } catch (Exception e) {
                    return false;
                }
            }, "duplicate and out-of-order writes must converge to delete tombstone");

            AtomicInteger loaderCalls = new AtomicInteger();
            ComplexEntity cached = cacheManager.get(key, () -> {
                loaderCalls.incrementAndGet();
                return CacheLoadResult.of(new ComplexEntity(999L, "unexpected", List.of()), 999L, Duration.ofMinutes(5));
            });
            assertNull(cached);
            assertEquals(0, loaderCalls.get());
        }
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

    private CacheManager createManager() {
        CacheManager manager = CacheManagerFactory.create(newCacheConfig());
        manager.bootstrap();
        cacheManagers.add(manager);
        return manager;
    }

    private CacheConfig newCacheConfig() {
        CacheConfig config = new CacheConfig();
        config.getL1().setEnabled(true);
        config.getL2().setEnabled(true);
        config.getL2().setUsername("dk900912");
        config.getL2().setPassword("qwe@1234");
        config.getL2().setHosts(List.of("127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"));
        config.getCacheMiss().setBackfillTtl(Duration.ofMinutes(2));
        config.getCodec().setTrustedPackages(List.of("io.github.dk900912"));
        return config;
    }

    private CacheMessage<Object> readL2Message(CacheKey key) throws Exception {
        JedisL2Provider provider = new JedisL2Provider();
        provider.initialize(newCacheConfig().getL2());
        try {
            String payload = provider.get(CacheKeyspace.dataKey(key));
            if (payload == null) {
                return null;
            }
            JacksonCacheCodec codec = new JacksonCacheCodec();
            codec.initialize(newCacheConfig());
            return codec.decodeMessage(payload, Object.class);
        } finally {
            provider.close();
        }
    }

    private void runDelayedReadRace(ExecutorService executor, Callable<Object> readTask, Runnable writeTask) throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Future<Object> future = executor.submit(() -> {
            started.countDown();
            assertTrue(release.await(5, TimeUnit.SECONDS));
            return readTask.call();
        });
        assertTrue(started.await(5, TimeUnit.SECONDS));
        writeTask.run();
        release.countDown();
        future.get(5, TimeUnit.SECONDS);
    }

    private void waitUntil(BooleanSupplier condition, String message) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(50);
        }
        fail(message);
    }
}
