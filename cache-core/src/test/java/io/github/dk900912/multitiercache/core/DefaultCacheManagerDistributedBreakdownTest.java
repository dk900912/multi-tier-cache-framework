package io.github.dk900912.multitiercache.core;

import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.CacheMessageListener;
import io.github.dk900912.multitiercache.api.CacheMessageSubscription;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.api.model.CacheLoadResult;
import io.github.dk900912.multitiercache.api.model.CacheMessage;
import io.github.dk900912.multitiercache.api.model.CacheRuntimeStats;
import io.github.dk900912.multitiercache.codec.JacksonCacheCodec;
import io.github.dk900912.multitiercache.spi.CacheCodec;
import io.github.dk900912.multitiercache.spi.L1Provider;
import io.github.dk900912.multitiercache.spi.L2PubSubMode;
import io.github.dk900912.multitiercache.spi.L2Provider;
import io.github.dk900912.multitiercache.spi.L2ReentrantLock;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultCacheManagerDistributedBreakdownTest {

    @Test
    void localModeShouldNotTouchDistributedLock() {
        CacheConfig config = config(CacheConfig.OriginLoadLimitMode.LOCAL_ONLY);
        TestL2Provider provider = new TestL2Provider(config, new JavaLock());
        DefaultCacheManager manager = manager(config, provider, new SingleFlight());

        try {
            String value = manager.get(CacheKey.simple("local:user:1"),
                    () -> CacheLoadResult.of("value", 1L, Duration.ofMinutes(1)));

            assertEquals("value", value);
            assertEquals(0, provider.getLockCalls.get());
            assertEquals(0L, manager.getMonitor().getRuntimeStats().getDistributedLockAttemptCount());
        } finally {
            manager.shutdown();
        }
    }

    @Test
    void twoManagersShouldInvokeLoaderOnlyOnceGlobally() throws Exception {
        CacheConfig firstConfig = config(CacheConfig.OriginLoadLimitMode.GLOBAL);
        CacheConfig secondConfig = config(CacheConfig.OriginLoadLimitMode.GLOBAL);
        JavaLock sharedLock = new JavaLock(2);
        TestL2Provider provider = new TestL2Provider(firstConfig, sharedLock);

        try (ExecutorService firstFlights = Executors.newVirtualThreadPerTaskExecutor();
             ExecutorService secondFlights = Executors.newVirtualThreadPerTaskExecutor();
             ExecutorService callers = Executors.newFixedThreadPool(20)) {
            DefaultCacheManager first = manager(firstConfig, provider, new SingleFlight(firstFlights));
            DefaultCacheManager second = manager(secondConfig, provider, new SingleFlight(secondFlights));
            AtomicInteger loaderCalls = new AtomicInteger();
            CountDownLatch start = new CountDownLatch(1);
            CacheKey key = CacheKey.simple("global:user:1");
            List<Future<String>> results = new ArrayList<>();

            try {
                for (int i = 0; i < 20; i++) {
                    DefaultCacheManager selected = i % 2 == 0 ? first : second;
                    results.add(callers.submit(() -> {
                        start.await();
                        return selected.get(key, () -> {
                            loaderCalls.incrementAndGet();
                            sleep(Duration.ofMillis(150));
                            return CacheLoadResult.of("value", 1L, Duration.ofMinutes(1));
                        });
                    }));
                }

                start.countDown();
                for (Future<String> result : results) {
                    assertEquals("value", result.get(5, TimeUnit.SECONDS));
                }

                assertEquals(1, loaderCalls.get());
                assertEquals(2, provider.getLockCalls.get());
                assertEquals(CacheKeyspace.loadLockKey(key), provider.lastLockName);
                assertFalse(provider.lastLockName.contains(key.toKeyString()));
                assertFalse(sharedLock.fixedLeaseOverloadCalled);

                long attempts = first.getMonitor().getRuntimeStats().getDistributedLockAttemptCount()
                        + second.getMonitor().getRuntimeStats().getDistributedLockAttemptCount();
                assertEquals(2L, attempts);
            } finally {
                first.shutdown();
                second.shutdown();
            }
        }
    }

    @Test
    void lockTimeoutShouldFailOpenByDefault() {
        CacheConfig config = config(CacheConfig.OriginLoadLimitMode.GLOBAL);
        JavaLock lock = new JavaLock();
        lock.timeout = true;
        TestL2Provider provider = new TestL2Provider(config, lock);
        DefaultCacheManager manager = manager(config, provider, new SingleFlight());
        AtomicInteger loaderCalls = new AtomicInteger();

        try {
            String value = manager.get(CacheKey.simple("timeout:open"), () -> {
                loaderCalls.incrementAndGet();
                return CacheLoadResult.of("value", 1L, Duration.ofMinutes(1));
            });

            CacheRuntimeStats stats = manager.getMonitor().getRuntimeStats();
            assertEquals("value", value);
            assertEquals(1, loaderCalls.get());
            assertEquals(1L, stats.getDistributedLockAttemptCount());
            assertEquals(1L, stats.getDistributedLockTimeoutCount());
            assertEquals(1L, stats.getDistributedLockFailOpenLoadCount());
        } finally {
            manager.shutdown();
        }
    }

    @Test
    void lockTimeoutShouldFailClosedWithoutCallingLoader() {
        CacheConfig config = config(CacheConfig.OriginLoadLimitMode.GLOBAL);
        config.getOriginLoadLimiter().setGlobalLoadFailurePolicy(
                CacheConfig.GlobalLoadFailurePolicy.FAIL_CLOSED);
        JavaLock lock = new JavaLock();
        lock.timeout = true;
        TestL2Provider provider = new TestL2Provider(config, lock);
        DefaultCacheManager manager = manager(config, provider, new SingleFlight());
        AtomicInteger loaderCalls = new AtomicInteger();

        try {
            IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> manager.get(CacheKey.simple("timeout:closed"), () -> {
                        loaderCalls.incrementAndGet();
                        return CacheLoadResult.of("unexpected", 1L, Duration.ofMinutes(1));
                    }));

            assertTrue(failure.getMessage().contains("Timed out"));
            assertEquals(0, loaderCalls.get());
            assertEquals(1L, manager.getMonitor().getRuntimeStats().getDistributedLockTimeoutCount());
            assertEquals(0L, manager.getMonitor().getRuntimeStats().getDistributedLockFailOpenLoadCount());
        } finally {
            manager.shutdown();
        }
    }

    @Test
    void acquisitionFailureShouldFailOpenAndRemainObservable() {
        CacheConfig config = config(CacheConfig.OriginLoadLimitMode.GLOBAL);
        JavaLock lock = new JavaLock();
        lock.acquisitionFailure = new IllegalStateException("redis unavailable");
        TestL2Provider provider = new TestL2Provider(config, lock);
        DefaultCacheManager manager = manager(config, provider, new SingleFlight());

        try {
            String value = manager.get(CacheKey.simple("failure:open"),
                    () -> CacheLoadResult.of("value", 1L, Duration.ofMinutes(1)));

            CacheRuntimeStats stats = manager.getMonitor().getRuntimeStats();
            assertEquals("value", value);
            assertEquals(1L, stats.getDistributedLockFailureCount());
            assertEquals(1L, stats.getDistributedLockFailOpenLoadCount());
        } finally {
            manager.shutdown();
        }
    }

    @Test
    void acquisitionFailureShouldFailClosedWithoutCallingLoader() {
        CacheConfig config = config(CacheConfig.OriginLoadLimitMode.GLOBAL);
        config.getOriginLoadLimiter().setGlobalLoadFailurePolicy(
                CacheConfig.GlobalLoadFailurePolicy.FAIL_CLOSED);
        JavaLock lock = new JavaLock();
        lock.acquisitionFailure = new IllegalStateException("redis unavailable");
        TestL2Provider provider = new TestL2Provider(config, lock);
        DefaultCacheManager manager = manager(config, provider, new SingleFlight());
        AtomicInteger loaderCalls = new AtomicInteger();

        try {
            IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> manager.get(CacheKey.simple("failure:closed"), () -> {
                        loaderCalls.incrementAndGet();
                        return CacheLoadResult.of("unexpected", 1L, Duration.ofMinutes(1));
                    }));

            assertTrue(failure.getMessage().contains("Failed to acquire"));
            assertEquals(0, loaderCalls.get());
            assertEquals(1L, manager.getMonitor().getRuntimeStats().getDistributedLockFailureCount());
        } finally {
            manager.shutdown();
        }
    }

    @Test
    void interruptedLockWaitShouldAbortWithoutFailOpenLoad() {
        CacheConfig config = config(CacheConfig.OriginLoadLimitMode.GLOBAL);
        JavaLock lock = new JavaLock();
        lock.interruptWait = true;
        TestL2Provider provider = new TestL2Provider(config, lock);
        DefaultCacheManager manager = manager(config, provider, new SingleFlight());
        AtomicInteger loaderCalls = new AtomicInteger();

        try {
            IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> manager.get(CacheKey.simple("interrupt"), () -> {
                        loaderCalls.incrementAndGet();
                        return CacheLoadResult.of("unexpected", 1L, Duration.ofMinutes(1));
                    }));

            assertTrue(failure.getMessage().contains("Interrupted"));
            assertEquals(0, loaderCalls.get());
            assertEquals(0L, manager.getMonitor().getRuntimeStats().getDistributedLockFailOpenLoadCount());
        } finally {
            manager.shutdown();
        }
    }

    @Test
    void releaseFailureShouldNotMaskSuccessfulLoad() {
        CacheConfig config = config(CacheConfig.OriginLoadLimitMode.GLOBAL);
        JavaLock lock = new JavaLock();
        lock.releaseFailure = true;
        TestL2Provider provider = new TestL2Provider(config, lock);
        DefaultCacheManager manager = manager(config, provider, new SingleFlight());

        try {
            String value = manager.get(CacheKey.simple("release:failure"),
                    () -> CacheLoadResult.of("value", 1L, Duration.ofMinutes(1)));

            assertEquals("value", value);
            assertEquals(1L, manager.getMonitor().getRuntimeStats().getDistributedLockFailureCount());
        } finally {
            manager.shutdown();
        }
    }

    @Test
    void loaderFailureShouldStillReleaseDistributedLock() {
        CacheConfig config = config(CacheConfig.OriginLoadLimitMode.GLOBAL);
        JavaLock lock = new JavaLock();
        TestL2Provider provider = new TestL2Provider(config, lock);
        DefaultCacheManager manager = manager(config, provider, new SingleFlight());

        try {
            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                    () -> manager.get(CacheKey.simple("loader:failure"), () -> {
                        throw new IllegalArgumentException("loader failed");
                    }));

            assertEquals("loader failed", failure.getMessage());
            assertFalse(lock.isLocked());
        } finally {
            manager.shutdown();
        }
    }

    @Test
    void distributedLockShouldAlsoCollapsePenetrationLoads() throws Exception {
        CacheConfig firstConfig = config(CacheConfig.OriginLoadLimitMode.GLOBAL);
        CacheConfig secondConfig = config(CacheConfig.OriginLoadLimitMode.GLOBAL);
        TestL2Provider provider = new TestL2Provider(firstConfig, new JavaLock());
        AtomicInteger loaderCalls = new AtomicInteger();
        CacheKey key = CacheKey.simple("missing:user");

        try (ExecutorService flights1 = Executors.newVirtualThreadPerTaskExecutor();
             ExecutorService flights2 = Executors.newVirtualThreadPerTaskExecutor();
             ExecutorService callers = Executors.newFixedThreadPool(2)) {
            DefaultCacheManager first = manager(firstConfig, provider, new SingleFlight(flights1));
            DefaultCacheManager second = manager(secondConfig, provider, new SingleFlight(flights2));
            CountDownLatch start = new CountDownLatch(1);
            try {
                Future<Object> a = callers.submit(() -> {
                    start.await();
                    return first.get(key, () -> penetration(loaderCalls));
                });
                Future<Object> b = callers.submit(() -> {
                    start.await();
                    return second.get(key, () -> penetration(loaderCalls));
                });
                start.countDown();

                assertNull(a.get(5, TimeUnit.SECONDS));
                assertNull(b.get(5, TimeUnit.SECONDS));
                assertEquals(1, loaderCalls.get());
            } finally {
                first.shutdown();
                second.shutdown();
            }
        }
    }

    private static CacheLoadResult<Object> penetration(AtomicInteger loaderCalls) {
        loaderCalls.incrementAndGet();
        sleep(Duration.ofMillis(100));
        return CacheLoadResult.penetration(Duration.ofMinutes(1));
    }

    private static CacheConfig config(CacheConfig.OriginLoadLimitMode mode) {
        CacheConfig config = new CacheConfig();
        config.getL1().setEnabled(false);
        config.getCodec().setTrustedPackages(List.of("java.lang", "io.github.dk900912"));
        config.getOriginLoadLimiter().setLocalLoadWaitTimeout(Duration.ofSeconds(5));
        config.getOriginLoadLimiter().setGlobalLoadWaitTimeout(Duration.ofSeconds(2));
        config.getOriginLoadLimiter().setOriginLoadLimitMode(mode);
        return config;
    }

    private static DefaultCacheManager manager(CacheConfig config,
                                               L2Provider provider,
                                               SingleFlight singleFlight) {
        CacheCodec codec = new JacksonCacheCodec();
        codec.initialize(config);
        return new DefaultCacheManager(
                config, new DisabledL1Provider(), provider, codec, singleFlight);
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static final class DisabledL1Provider implements L1Provider {
        @Override
        public Object get(CacheKey key) {
            throw new AssertionError("L1 should be disabled");
        }

        @Override
        public void put(CacheKey key, Object value) {
            throw new AssertionError("L1 should be disabled");
        }

        @Override
        public void invalidate(CacheKey key) {
            throw new AssertionError("L1 should be disabled");
        }

        @Override
        public void clear() {
            throw new AssertionError("L1 should be disabled");
        }
    }

    private static final class TestL2Provider implements L2Provider {
        private final CacheCodec codec;
        private final L2ReentrantLock lock;
        private final ConcurrentHashMap<String, String> values = new ConcurrentHashMap<>();
        private final AtomicInteger getLockCalls = new AtomicInteger();
        private volatile String lastLockName;

        private TestL2Provider(CacheConfig config, L2ReentrantLock lock) {
            this.codec = new JacksonCacheCodec();
            this.codec.initialize(config);
            this.lock = lock;
        }

        @Override
        public void initialize(CacheConfig.L2Config config) {
        }

        @Override
        public boolean supportsDistributedLock() {
            return true;
        }

        @Override
        public String get(CacheKey key) {
            return values.get(key.toKeyString());
        }

        @Override
        public void set(CacheKey key, String value, Duration ttl) {
            values.put(key.toKeyString(), value);
        }

        @Override
        public void delete(CacheKey key) {
            values.remove(key.toKeyString());
        }

        @Override
        public void publish(String channel, String message, L2PubSubMode mode) {
        }

        @Override
        public CacheMessageSubscription subscribe(String channel, CacheMessageListener listener, L2PubSubMode mode) {
            return () -> {
            };
        }

        @Override
        public synchronized Object eval(String script, List<String> keys, List<String> args) {
            if (!CacheLuaScripts.APPLY_MESSAGE_LUA_SCRIPT.equals(script)) {
                throw new IllegalArgumentException("Unexpected script");
            }
            String dataKey = keys.getFirst();
            String incomingPayload = args.getFirst();
            String currentPayload = values.get(dataKey);
            if (currentPayload == null) {
                values.put(dataKey, incomingPayload);
                return 1L;
            }
            CacheMessage<Object> incoming = codec.decodeMessage(incomingPayload, Object.class);
            CacheMessage<Object> current = codec.decodeMessage(currentPayload, Object.class);
            if (CacheMessageVersionComparator.shouldReplace(incoming, current)) {
                values.put(dataKey, incomingPayload);
                return 1L;
            }
            return 0L;
        }

        @Override
        public L2ReentrantLock getLock(String name) {
            getLockCalls.incrementAndGet();
            lastLockName = name;
            return lock;
        }
    }

    private static final class JavaLock implements L2ReentrantLock {
        private final ReentrantLock delegate = new ReentrantLock();
        private final CountDownLatch acquisitionBarrier;
        private volatile RuntimeException acquisitionFailure;
        private volatile boolean timeout;
        private volatile boolean interruptWait;
        private volatile boolean releaseFailure;
        private volatile boolean fixedLeaseOverloadCalled;

        private JavaLock() {
            this(0);
        }

        private JavaLock(int expectedConcurrentAcquisitions) {
            this.acquisitionBarrier = expectedConcurrentAcquisitions > 0
                    ? new CountDownLatch(expectedConcurrentAcquisitions)
                    : null;
        }

        @Override
        public void lock() {
            delegate.lock();
        }

        @Override
        public void lock(Duration leaseTime) {
            fixedLeaseOverloadCalled = true;
            delegate.lock();
        }

        @Override
        public boolean tryLock() {
            return delegate.tryLock();
        }

        @Override
        public boolean tryLock(Duration waitTime) throws InterruptedException {
            if (interruptWait) {
                throw new InterruptedException("test interruption");
            }
            if (acquisitionFailure != null) {
                throw acquisitionFailure;
            }
            if (timeout) {
                return false;
            }
            if (acquisitionBarrier != null) {
                acquisitionBarrier.countDown();
                if (!acquisitionBarrier.await(waitTime.toNanos(), TimeUnit.NANOSECONDS)) {
                    return false;
                }
            }
            return delegate.tryLock(waitTime.toNanos(), TimeUnit.NANOSECONDS);
        }

        @Override
        public boolean tryLock(Duration waitTime, Duration leaseTime) throws InterruptedException {
            fixedLeaseOverloadCalled = true;
            return tryLock(waitTime);
        }

        @Override
        public void unlock() {
            delegate.unlock();
            if (releaseFailure) {
                throw new IllegalStateException("test release failure");
            }
        }

        @Override
        public boolean forceUnlock() {
            if (!delegate.isHeldByCurrentThread()) {
                return false;
            }
            while (delegate.isHeldByCurrentThread()) {
                delegate.unlock();
            }
            return true;
        }

        @Override
        public boolean isLocked() {
            return delegate.isLocked();
        }

        @Override
        public boolean isHeldByCurrentThread() {
            return delegate.isHeldByCurrentThread();
        }

        @Override
        public int getHoldCount() {
            return delegate.getHoldCount();
        }

        @Override
        public Duration remainTimeToLive() {
            return delegate.isLocked() ? Duration.ofSeconds(30) : Duration.ofMillis(-2);
        }
    }
}
