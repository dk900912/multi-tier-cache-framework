package io.github.dk900912.multitiercache.core;

import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.provider.jedis.JedisL2Provider;
import io.github.dk900912.multitiercache.provider.lettuce.LettuceL2Provider;
import io.github.dk900912.multitiercache.provider.redisson.RedissonL2Provider;
import io.github.dk900912.multitiercache.spi.L2Provider;
import io.github.dk900912.multitiercache.spi.L2ReentrantLock;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class L2ReentrantLockProviderConsistencyTest {

    private final List<L2Provider> providers = new ArrayList<>();

    @BeforeEach
    void setUp() {
        assumeTrue(isLocalRedisClusterReachable(), "Local Redis ACL cluster is not reachable");

        providers.add(initialized(new JedisL2Provider()));
        providers.add(initialized(new LettuceL2Provider()));
        providers.add(initialized(new RedissonL2Provider()));
    }

    @AfterEach
    void tearDown() throws Exception {
        for (L2Provider provider : providers) {
            if (provider instanceof AutoCloseable closeable) {
                closeable.close();
            }
        }
        providers.clear();
    }

    @Test
    void providersShouldContendForSameRedisLock() throws InterruptedException {
        for (L2Provider ownerProvider : providers) {
            for (L2Provider contenderProvider : providers) {
                if (ownerProvider == contenderProvider) {
                    continue;
                }

                String lockName = "provider:consistency:" + UUID.randomUUID();
                L2ReentrantLock owner = ownerProvider.getLock(lockName);
                L2ReentrantLock contender = contenderProvider.getLock(lockName);
                owner.forceUnlock();
                contender.forceUnlock();

                owner.lock(Duration.ofSeconds(5));
                try {
                    assertFalse(contender.tryLock(Duration.ofMillis(300), Duration.ofSeconds(1)),
                            ownerProvider.providerType() + " owner should block " + contenderProvider.providerType());
                } finally {
                    owner.unlock();
                }

                assertTrue(contender.tryLock(Duration.ofSeconds(2), Duration.ofSeconds(1)),
                        contenderProvider.providerType() + " should acquire after " + ownerProvider.providerType() + " unlock");
                contender.unlock();
            }
        }
    }

    @Test
    void unlockFromOneProviderShouldWakeWaitingProvider() throws InterruptedException {
        for (L2Provider ownerProvider : providers) {
            for (L2Provider waiterProvider : providers) {
                if (ownerProvider == waiterProvider) {
                    continue;
                }

                String lockName = "provider:wakeup:" + UUID.randomUUID();
                L2ReentrantLock owner = ownerProvider.getLock(lockName);
                owner.forceUnlock();
                owner.lock(Duration.ofSeconds(5));

                CountDownLatch waiterStarted = new CountDownLatch(1);
                AtomicBoolean acquired = new AtomicBoolean(false);
                Thread waiter = new Thread(() -> {
                    try {
                        waiterStarted.countDown();
                        L2ReentrantLock waiterLock = waiterProvider.getLock(lockName);
                        acquired.set(waiterLock.tryLock(Duration.ofSeconds(3), Duration.ofSeconds(1)));
                        if (acquired.get()) {
                            waiterLock.unlock();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });

                waiter.start();
                assertTrue(waiterStarted.await(1, TimeUnit.SECONDS));
                Thread.sleep(300);
                owner.unlock();
                waiter.join(5000);

                assertFalse(waiter.isAlive());
                assertTrue(acquired.get(),
                        ownerProvider.providerType() + " unlock should wake " + waiterProvider.providerType());
            }
        }
    }

    @Test
    void providersShouldRejectLockNamesWithUnmatchedBraces() {
        for (L2Provider provider : providers) {
            assertTrue(provider.supportsDistributedLock(),
                    provider.providerType() + " should declare distributed lock support");
            assertThrows(IllegalArgumentException.class, () -> provider.getLock("{broken"),
                    provider.providerType() + " should reject lock name with unmatched opening brace");
            assertThrows(IllegalArgumentException.class, () -> provider.getLock("broken}"),
                    provider.providerType() + " should reject lock name with unmatched closing brace");
            assertThrows(IllegalArgumentException.class, () -> provider.getLock("{}"),
                    provider.providerType() + " should reject empty Redis hash tag");
        }
    }

    private static L2Provider initialized(L2Provider provider) {
        CacheConfig.L2Config config = new CacheConfig.L2Config();
        config.setHosts(List.of("127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"));
        config.setUsername("dk900912");
        config.setPassword("qwe@1234");
        config.setLockWatchdogTimeout(Duration.ofMillis(1500));
        provider.initialize(config);
        return provider;
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
