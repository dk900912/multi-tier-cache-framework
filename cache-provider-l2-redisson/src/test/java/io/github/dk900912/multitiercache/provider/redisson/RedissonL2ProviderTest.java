package io.github.dk900912.multitiercache.provider.redisson;

import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.CacheMessageSubscription;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.spi.L2PubSubMode;
import io.github.dk900912.multitiercache.spi.L2ReentrantLock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class RedissonL2ProviderTest {

    private RedissonL2Provider provider;

    @BeforeEach
    void setUp() {
        assumeTrue(isLocalRedisClusterReachable(), "Local Redis ACL cluster is not reachable");

        CacheConfig.L2Config config = new CacheConfig.L2Config();
        config.setHosts(List.of("127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"));
        config.setUsername("dk900912");
        config.setPassword("qwe@1234");
        config.setLockWatchdogTimeout(Duration.ofMillis(1500));

        provider = new RedissonL2Provider();
        provider.initialize(config);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (provider != null) {
            provider.close();
        }
    }

    @Test
    void testBasicSetGetAndDelete() {
        CacheKey key = CacheKey.simple("redisson:test:key");
        provider.set(key, "redisson-value", Duration.ofMinutes(1));
        assertEquals("redisson-value", provider.get(key));

        provider.delete(key);
        assertNull(provider.get(key));
    }

    @Test
    void testPubSub() throws InterruptedException {
        String channel = "redisson:test:channel";
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean messageReceived = new AtomicBoolean(false);

        CacheMessageSubscription subscription = provider.subscribe(channel, (ch, msg) -> {
            if (channel.equals(ch) && "hello".equals(msg)) {
                messageReceived.set(true);
                latch.countDown();
            }
        }, L2PubSubMode.STANDARD);

        // Wait a bit for subscription to be fully active
        Thread.sleep(500);

        provider.publish(channel, "hello", L2PubSubMode.STANDARD);

        assertTrue(latch.await(5, TimeUnit.SECONDS), "Did not receive Pub/Sub message in time");
        assertTrue(messageReceived.get());

        subscription.close();
    }

    @Test
    void testShardedPubSub() throws InterruptedException {
        String channel = "redisson:test:sharded-channel";
        CountDownLatch latch = new CountDownLatch(1);

        CacheMessageSubscription subscription = provider.subscribe(channel, (ch, msg) -> {
            if (channel.equals(ch) && "hello".equals(msg)) {
                latch.countDown();
            }
        }, L2PubSubMode.SHARDED);

        try {
            provider.publish(channel, "hello", L2PubSubMode.SHARDED);
            assertTrue(latch.await(5, TimeUnit.SECONDS), "Sharded Pub/Sub message was not delivered");
        } finally {
            subscription.close();
        }
    }

    @Test
    void testEvalSupportsCommonResults() {
        String slotKey = "{redisson:test:eval}";

        assertEquals(1L, provider.eval("return 1", List.of(slotKey), List.of()));
        assertEquals("hello", provider.eval("return 'hello'", List.of(slotKey), List.of()));
        assertEquals(List.of("a", "b"), provider.eval("return {'a', 'b'}", List.of(slotKey), List.of()));
    }

    @Test
    void testReentrantLockAcquireAndRelease() throws InterruptedException {
        String name = "redisson:test:lock:reentrant";
        L2ReentrantLock lock = provider.getLock(name);
        lock.forceUnlock();

        assertTrue(lock.tryLock(Duration.ofSeconds(1)));
        assertTrue(lock.isHeldByCurrentThread());
        assertEquals(1, lock.getHoldCount());

        lock.lock();
        assertEquals(2, lock.getHoldCount());

        AtomicBoolean acquiredByOtherThread = new AtomicBoolean(true);
        Thread contender = new Thread(() -> {
            try {
                L2ReentrantLock contenderLock = provider.getLock(name);
                acquiredByOtherThread.set(contenderLock.tryLock(Duration.ofMillis(300), Duration.ofSeconds(1)));
                if (acquiredByOtherThread.get()) {
                    contenderLock.unlock();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        contender.start();
        contender.join();

        assertFalse(acquiredByOtherThread.get());

        lock.unlock();
        assertTrue(lock.isHeldByCurrentThread());
        assertEquals(1, lock.getHoldCount());

        lock.unlock();
        assertFalse(lock.isLocked());
    }

    @Test
    void testReentrantLockRejectsNonOwnerUnlock() throws InterruptedException {
        String name = "redisson:test:lock:owner";
        L2ReentrantLock lock = provider.getLock(name);
        lock.forceUnlock();
        lock.lock(Duration.ofSeconds(5));

        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread otherThread = new Thread(() -> {
            try {
                provider.getLock(name).unlock();
            } catch (Throwable e) {
                failure.set(e);
            }
        });
        otherThread.start();
        otherThread.join();

        assertTrue(failure.get() instanceof IllegalMonitorStateException);
        assertTrue(lock.isHeldByCurrentThread());

        lock.unlock();
    }

    @Test
    void testReentrantLockLeaseExpires() throws InterruptedException {
        String name = "redisson:test:lock:lease";
        L2ReentrantLock lock = provider.getLock(name);
        lock.forceUnlock();

        assertTrue(lock.tryLock(Duration.ZERO, Duration.ofMillis(500)));
        Thread.sleep(900);

        assertFalse(lock.isLocked());
        L2ReentrantLock contender = provider.getLock(name);
        assertTrue(contender.tryLock(Duration.ofSeconds(1), Duration.ofSeconds(1)));
        contender.unlock();
    }

    @Test
    void testReentrantLockWatchdogKeepsLockAlive() throws InterruptedException {
        String name = "redisson:test:lock:watchdog";
        L2ReentrantLock lock = provider.getLock(name);
        lock.forceUnlock();

        lock.lock();
        Thread.sleep(2500);

        assertTrue(lock.isHeldByCurrentThread());
        AtomicBoolean acquiredByOtherThread = new AtomicBoolean(true);
        Thread contender = new Thread(() -> {
            try {
                acquiredByOtherThread.set(provider.getLock(name).tryLock(Duration.ofMillis(300), Duration.ofSeconds(1)));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        contender.start();
        contender.join();

        assertFalse(acquiredByOtherThread.get());
        lock.unlock();
    }

    @Test
    void testReentrantLockCrossHandleUnlockCancelsWatchdog() throws InterruptedException {
        String name = "redisson:test:lock:cross-handle";
        L2ReentrantLock watchdogLock = provider.getLock(name);
        watchdogLock.forceUnlock();

        watchdogLock.lock();
        provider.getLock(name).unlock();

        L2ReentrantLock leasedLock = provider.getLock(name);
        assertTrue(leasedLock.tryLock(Duration.ZERO, Duration.ofMillis(500)));
        Thread.sleep(2500);

        assertFalse(leasedLock.isLocked());
    }

    @Test
    void testReentrantLockPartialUnlockKeepsExplicitLease() throws InterruptedException {
        String name = "redisson:test:lock:lease-partial";
        L2ReentrantLock lock = provider.getLock(name);
        lock.forceUnlock();

        lock.lock(Duration.ofMillis(500));
        lock.lock(Duration.ofMillis(500));
        lock.unlock();

        Thread.sleep(900);

        assertFalse(lock.isLocked());
    }

    private static boolean isLocalRedisClusterReachable() {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", 7001), 500);
            return true;
        } catch (Exception ignored) {
            return false;
        }
    }
}
