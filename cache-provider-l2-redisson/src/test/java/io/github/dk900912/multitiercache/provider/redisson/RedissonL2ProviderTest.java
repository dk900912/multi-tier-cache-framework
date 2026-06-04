package io.github.dk900912.multitiercache.provider.redisson;

import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.CacheMessageSubscription;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
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
        });

        // Wait a bit for subscription to be fully active
        Thread.sleep(500);

        provider.publish(channel, "hello");

        assertTrue(latch.await(5, TimeUnit.SECONDS), "Did not receive Pub/Sub message in time");
        assertTrue(messageReceived.get());

        subscription.close();
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
