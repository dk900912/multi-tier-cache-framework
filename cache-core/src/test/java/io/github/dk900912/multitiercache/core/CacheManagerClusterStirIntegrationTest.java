package io.github.dk900912.multitiercache.core;

import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.CacheManager;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.api.model.CacheLoadResult;
import io.github.dk900912.multitiercache.api.model.CacheMessage;
import io.github.dk900912.multitiercache.api.model.CacheMessageType;
import io.github.dk900912.multitiercache.api.model.CacheRuntimeStats;
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
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class CacheManagerClusterStirIntegrationTest {

    private static final Duration TTL = Duration.ofMinutes(5);

    private final List<CacheManager> cacheManagers = new ArrayList<>();

    @BeforeEach
    void setUp() {
        assumeTrue(isLocalRedisClusterReachable(), "Local Redis ACL cluster is not reachable");
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
    void duplicateDeletePubSubShouldNotEvictWarmedRemoteTombstoneFromL1() throws Exception {
        CacheManager writer = createManager();
        CacheManager reader = createManager();
        CacheKey key = CacheKey.simple("integration:stir:duplicate-delete:" + UUID.randomUUID());

        writer.insert(key, new StirEntity(1L, "v1"), 1L, TTL);
        waitUntil(() -> l2MessageMatches(key, CacheMessageType.INSERT, 1L), "insert should reach L2");

        StirEntity value = reader.get(key, () -> {
            throw new AssertionError("reader should warm from L2");
        });
        assertNotNull(value);
        assertEquals("v1", value.getName());

        writer.evict(key, 2L, TTL);
        waitUntil(() -> l2MessageMatches(key, CacheMessageType.DELETE, 2L), "delete should reach L2");

        AtomicInteger loaderCalls = new AtomicInteger();
        waitUntil(() -> reader.get(key, () -> {
            loaderCalls.incrementAndGet();
            return CacheLoadResult.of(new StirEntity(999L, "unexpected"), 999L, TTL);
        }) == null, "reader should warm the delete tombstone into L1");
        assertEquals(0, loaderCalls.get(), "delete tombstone should block DB reload");

        CacheRuntimeStats beforeDuplicate = reader.getMonitor().getRuntimeStats();
        writer.evict(key, 2L, TTL);
        Thread.sleep(500);

        StirEntity deleted = reader.get(key, () -> {
            loaderCalls.incrementAndGet();
            return CacheLoadResult.of(new StirEntity(1000L, "unexpected"), 1000L, TTL);
        });
        CacheRuntimeStats afterDuplicate = reader.getMonitor().getRuntimeStats();

        assertNull(deleted);
        assertEquals(0, loaderCalls.get(), "duplicate delete must still block DB reload");
        assertEquals(
                beforeDuplicate.getL1HitCount() + 1,
                afterDuplicate.getL1HitCount(),
                "duplicate delete Pub/Sub must not evict an already-current local delete tombstone");
        assertEquals(
                beforeDuplicate.getL2HitCount(),
                afterDuplicate.getL2HitCount(),
                "duplicate delete should be served from warmed L1 tombstone, not L2");
    }

    @Test
    void luaShouldRejectDuplicateDeleteButAllowSameVersionDeleteOverValue() {
        CacheConfig config = newCacheConfig();
        JedisL2Provider provider = new JedisL2Provider();
        provider.initialize(config.getL2());
        JacksonCacheCodec codec = new JacksonCacheCodec();
        codec.initialize(config);
        CacheKey key = CacheKey.simple("integration:stir:lua-duplicate-delete:" + UUID.randomUUID());
        CacheKey redisKey = CacheKeyspace.dataKey(key);

        try {
            provider.delete(redisKey);
            CacheMessage<StirEntity> value = new CacheMessage<>(
                    key.toKeyString(),
                    new StirEntity(8L, "v8"),
                    8L,
                    CacheMessageType.UPDATE,
                    TTL.toMillis());
            assertEquals(1L, evalApply(provider, redisKey, codec.encode(value), CacheMessageType.UPDATE, 8L));

            CacheMessage<StirEntity> delete = new CacheMessage<>(
                    key.toKeyString(),
                    null,
                    8L,
                    CacheMessageType.DELETE,
                    TTL.toMillis());
            String deletePayload = codec.encode(delete);
            assertEquals(
                    1L,
                    evalApply(provider, redisKey, deletePayload, CacheMessageType.DELETE, 8L),
                    "delete should be able to override same-version value state");
            assertEquals(
                    0L,
                    evalApply(provider, redisKey, deletePayload, CacheMessageType.DELETE, 8L),
                    "duplicate delete tombstone should be rejected");
        } finally {
            provider.delete(redisKey);
            provider.close();
        }
    }

    @Test
    void randomMutationStormShouldConvergeEveryNodeToL2Authority() throws Exception {
        List<CacheManager> managers = List.of(createManager(), createManager(), createManager());
        int keyCount = 8;
        int operationCount = 160;
        List<CacheKey> keys = new ArrayList<>(keyCount);
        List<AtomicLong> versions = new ArrayList<>(keyCount);
        List<AtomicReference<ExpectedState>> expectedStates = new ArrayList<>(keyCount);
        List<Object> keyLocks = new ArrayList<>(keyCount);

        for (int i = 0; i < keyCount; i++) {
            CacheKey key = CacheKey.simple("integration:stir:storm:" + i + ":" + UUID.randomUUID());
            StirEntity seed = new StirEntity(1L, "seed-" + i);
            managers.getFirst().insert(key, seed, seed.getVersion(), TTL);
            keys.add(key);
            versions.add(new AtomicLong(seed.getVersion()));
            expectedStates.add(new AtomicReference<>(
                    new ExpectedState(CacheMessageType.INSERT, seed.getVersion(), seed.getName())));
            keyLocks.add(new Object());
        }

        ExecutorService executor = Executors.newFixedThreadPool(12);
        Random random = new Random(20260707L);
        List<CompletableFuture<Void>> tasks = new ArrayList<>(operationCount);
        for (int i = 0; i < operationCount; i++) {
            final int operation = i;
            final int keyIndex = random.nextInt(keyCount);
            final int managerIndex = random.nextInt(managers.size());
            final int action = random.nextInt(10);
            tasks.add(CompletableFuture.runAsync(
                    () -> applyRandomOperation(
                            managers.get(managerIndex),
                            keys.get(keyIndex),
                            versions.get(keyIndex),
                            expectedStates.get(keyIndex),
                            keyLocks.get(keyIndex),
                            action,
                            operation),
                    executor));
        }
        CompletableFuture.allOf(tasks.toArray(CompletableFuture[]::new)).get(30, TimeUnit.SECONDS);
        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));

        for (int i = 0; i < keyCount; i++) {
            CacheKey key = keys.get(i);
            ExpectedState expected = expectedStates.get(i).get();
            if (expected.type() == CacheMessageType.DELETE) {
                waitUntil(
                        () -> l2MessageMatches(key, CacheMessageType.DELETE, expected.version()),
                        "delete should be authoritative in L2 for " + key.toKeyString());
            } else if (expected.type() != null) {
                waitUntil(
                        () -> l2MessageMatches(key, expected.type(), expected.version()),
                        "value should be authoritative in L2 for " + key.toKeyString());
            }

            for (CacheManager manager : managers) {
                AtomicInteger loaderCalls = new AtomicInteger();
                StirEntity cached = manager.get(key, () -> {
                    loaderCalls.incrementAndGet();
                    return CacheLoadResult.of(new StirEntity(999_999L, "unexpected"), 999_999L, TTL);
                });

                if (expected.type() == CacheMessageType.DELETE) {
                    assertNull(cached, "deleted key should read as tombstone: " + key.toKeyString());
                    assertEquals(0, loaderCalls.get(), "tombstone must block DB reload: " + key.toKeyString());
                } else {
                    assertNotNull(cached, "value key should be present: " + key.toKeyString());
                    assertEquals(expected.version(), cached.getVersion());
                    assertEquals(expected.name(), cached.getName());
                    assertEquals(0, loaderCalls.get(), "value should come from cache: " + key.toKeyString());
                }
            }
        }
    }

    private void applyRandomOperation(
            CacheManager manager,
            CacheKey key,
            AtomicLong versionCounter,
            AtomicReference<ExpectedState> expectedState,
            Object keyLock,
            int action,
            int operation) {
        synchronized (keyLock) {
            long version = versionCounter.incrementAndGet();
            if (action < 4) {
                StirEntity entity = new StirEntity(version, "insert-" + operation);
                manager.insert(key, entity, version, TTL);
                recordExpectedValue(expectedState, CacheMessageType.INSERT, version, entity.getName());
            } else if (action < 8) {
                StirEntity entity = new StirEntity(version, "update-" + operation);
                manager.update(key, entity, version, TTL);
                recordExpectedValue(expectedState, CacheMessageType.UPDATE, version, entity.getName());
            } else {
                manager.evict(key, version, TTL);
                recordExpectedDelete(expectedState, version);
            }
        }
    }

    private void recordExpectedValue(
            AtomicReference<ExpectedState> expectedState,
            CacheMessageType type,
            long version,
            String name) {
        expectedState.updateAndGet(current -> {
            if (current.type() == CacheMessageType.DELETE) {
                return current;
            }
            if (version > current.version()) {
                return new ExpectedState(type, version, name);
            }
            return current;
        });
    }

    private void recordExpectedDelete(AtomicReference<ExpectedState> expectedState, long version) {
        expectedState.updateAndGet(current -> {
            if (version >= current.version()) {
                return new ExpectedState(CacheMessageType.DELETE, version, null);
            }
            return current;
        });
    }

    private CacheManager createManager() {
        CacheManager manager = CacheManagerFactory.create(newCacheConfig());
        manager.bootstrap();
        cacheManagers.add(manager);
        return manager;
    }

    private Object evalApply(
            JedisL2Provider provider,
            CacheKey redisKey,
            String payload,
            CacheMessageType type,
            long version) {
        return provider.eval(
                CacheLuaScripts.APPLY_MESSAGE_LUA_SCRIPT,
                List.of(redisKey.toKeyString()),
                List.of(
                        payload,
                        String.valueOf(TTL.toMillis()),
                        type.getWireValue(),
                        String.valueOf(version),
                        "unused",
                        "0"));
    }

    private CacheConfig newCacheConfig() {
        CacheConfig config = new CacheConfig();
        config.getL1().setEnabled(true);
        config.getL2().setEnabled(true);
        config.getL2().setUsername("dk900912");
        config.getL2().setPassword("qwe@1234");
        config.getL2().setHosts(List.of("127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"));
        config.getLoadPolicy().setBackfillTtl(Duration.ofMinutes(2));
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

    private boolean l2MessageMatches(CacheKey key, CacheMessageType type, long version) {
        try {
            CacheMessage<Object> message = readL2Message(key);
            return message != null
                    && message.getType() == type
                    && message.getVersion() == version;
        } catch (Exception e) {
            return false;
        }
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

    public static class StirEntity {
        @CacheVersion
        private long version;
        private String name;

        public StirEntity() {
        }

        public StirEntity(long version, String name) {
            this.version = version;
            this.name = name;
        }

        public long getVersion() {
            return version;
        }

        public void setVersion(long version) {
            this.version = version;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    private record ExpectedState(CacheMessageType type, long version, String name) {
    }
}
