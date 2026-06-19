package io.github.dk900912.multitiercache.core;

import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.provider.jedis.JedisL2Provider;
import io.github.dk900912.multitiercache.provider.lettuce.LettuceL2Provider;
import io.github.dk900912.multitiercache.provider.redisson.RedissonL2Provider;
import io.github.dk900912.multitiercache.spi.L2PubSubMode;
import io.github.dk900912.multitiercache.spi.L2Provider;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class L2ProviderLifecycleConsistencyTest {

    private static final List<Supplier<L2Provider>> PROVIDERS = List.of(
            JedisL2Provider::new,
            LettuceL2Provider::new,
            RedissonL2Provider::new);

    @Test
    void providersShouldEnforceTerminalLifecycleAndPositiveTtl() throws Exception {
        assumeTrue(isLocalRedisClusterReachable(), "Local Redis ACL cluster is not reachable");

        for (Supplier<L2Provider> factory : PROVIDERS) {
            L2Provider provider = factory.get();
            CacheConfig.L2Config config = newConfig();
            provider.initialize(config);
            CacheKey key = CacheKey.simple("lifecycle:" + provider.providerType().name().toLowerCase());

            assertThrows(IllegalStateException.class, () -> provider.initialize(config));
            provider.set(key, "original", Duration.ofSeconds(30));
            assertEquals("original", provider.get(key));

            assertThrows(IllegalArgumentException.class, () -> provider.set(key, "null", null));
            assertThrows(IllegalArgumentException.class, () -> provider.set(key, "zero", Duration.ZERO));
            assertThrows(IllegalArgumentException.class, () -> provider.set(key, "negative", Duration.ofMillis(-1)));
            assertThrows(IllegalArgumentException.class, () -> provider.set(key, "sub-millisecond", Duration.ofNanos(1)));
            assertEquals("original", provider.get(key), "Invalid TTL must not overwrite the existing value");

            close(provider);
            close(provider);

            assertThrows(IllegalStateException.class, () -> provider.get(key));
            assertThrows(IllegalStateException.class, () -> provider.set(key, "value", Duration.ofSeconds(1)));
            assertThrows(IllegalStateException.class, () -> provider.delete(key));
            assertThrows(IllegalStateException.class,
                    () -> provider.publish("closed:test", "message", L2PubSubMode.STANDARD));
            assertThrows(IllegalStateException.class,
                    () -> provider.subscribe("closed:test", (channel, message) -> { }, L2PubSubMode.STANDARD));
            assertThrows(IllegalStateException.class,
                    () -> provider.eval("return 1", List.of("{closed:test}"), List.of()));
            assertThrows(IllegalStateException.class, () -> provider.getLock("closed:test"));
            assertThrows(IllegalStateException.class, () -> provider.initialize(config));
        }
    }

    @Test
    void concurrentInitializationShouldCreateExactlyOneRuntime() throws Exception {
        assumeTrue(isLocalRedisClusterReachable(), "Local Redis ACL cluster is not reachable");

        for (Supplier<L2Provider> factory : PROVIDERS) {
            L2Provider provider = factory.get();
            CacheConfig.L2Config config = newConfig();
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(2);
            AtomicInteger successes = new AtomicInteger();
            AtomicInteger rejected = new AtomicInteger();
            AtomicReference<Throwable> unexpected = new AtomicReference<>();

            Runnable initialize = () -> {
                try {
                    assertTrue(start.await(5, TimeUnit.SECONDS));
                    provider.initialize(config);
                    successes.incrementAndGet();
                } catch (IllegalStateException expected) {
                    rejected.incrementAndGet();
                } catch (Throwable failure) {
                    unexpected.compareAndSet(null, failure);
                } finally {
                    done.countDown();
                }
            };

            Thread.ofPlatform().start(initialize);
            Thread.ofPlatform().start(initialize);
            start.countDown();
            assertTrue(done.await(30, TimeUnit.SECONDS));
            assertNull(unexpected.get());
            assertEquals(1, successes.get());
            assertEquals(1, rejected.get());
            close(provider);
        }
    }

    @Test
    void failedInitializationShouldReturnToNewStateAndAllowRetry() throws Exception {
        assumeTrue(isLocalRedisClusterReachable(), "Local Redis ACL cluster is not reachable");

        for (Supplier<L2Provider> factory : PROVIDERS) {
            L2Provider provider = factory.get();
            CacheConfig.L2Config config = newConfig();
            config.getSubscriber().setCapacity(0);
            assertThrows(IllegalArgumentException.class, () -> provider.initialize(config));

            config.getSubscriber().setCapacity(100);
            provider.initialize(config);
            CacheKey key = CacheKey.simple("retry:" + provider.providerType().name().toLowerCase());
            provider.set(key, "ok", Duration.ofSeconds(30));
            assertEquals("ok", provider.get(key));
            close(provider);
        }
    }

    @Test
    void closeBeforeInitializationShouldBeTerminal() throws Exception {
        for (Supplier<L2Provider> factory : PROVIDERS) {
            L2Provider provider = factory.get();
            close(provider);
            close(provider);
            assertThrows(IllegalStateException.class, () -> provider.initialize(newConfig()));
        }
    }

    private static CacheConfig.L2Config newConfig() {
        CacheConfig.L2Config config = new CacheConfig.L2Config();
        config.setHosts(List.of("127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"));
        config.setUsername("dk900912");
        config.setPassword("qwe@1234");
        config.setLockWatchdogTimeout(Duration.ofMillis(1500));
        return config;
    }

    private static void close(L2Provider provider) throws Exception {
        if (provider instanceof AutoCloseable closeable) {
            closeable.close();
        }
    }

    private static boolean isLocalRedisClusterReachable() {
        for (int port = 7001; port <= 7003; port++) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("127.0.0.1", port), 500);
            } catch (Exception ignored) {
                return false;
            }
        }
        return true;
    }
}
