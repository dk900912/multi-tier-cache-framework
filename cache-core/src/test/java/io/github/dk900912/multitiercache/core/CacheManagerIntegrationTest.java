package io.github.dk900912.multitiercache.core;

import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.CacheManager;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.api.model.CacheLoadResult;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class CacheManagerIntegrationTest {

    @Test
    void shouldUseRealRedisAclClusterConfig() {
        assumeTrue(isLocalRedisClusterReachable(), "Local Redis ACL cluster is not reachable");

        CacheConfig config = new CacheConfig();
        config.getL1().setEnabled(false);
        config.getL2().setEnabled(true);
        config.getL2().setUsername("dk900912");
        config.getL2().setPassword("qwe@1234");
        config.getL2().setHosts(List.of("127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"));
        config.getCacheMiss().setBackfillTtl(Duration.ofMinutes(2));

        CacheManager cacheManager = CacheManagerFactory.create(config);
        AtomicInteger loaderCalls = new AtomicInteger();
        CacheKey key = CacheKey.simple("integration:test:cache-manager:" + UUID.randomUUID());

        try {
            cacheManager.bootstrap();

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
        } finally {
            cacheManager.shutdown();
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
}
