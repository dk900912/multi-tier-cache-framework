package io.github.dk900912.multitiercache.provider.jedis;

import io.github.dk900912.multitiercache.api.model.CacheConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JedisL2ProviderConfigurationTest {

    @Test
    void parsesIpv4HostnamesAndIpv6Endpoints() {
        assertEquals(new JedisL2Provider.Endpoint("localhost", 6379),
                JedisL2Provider.Endpoint.parse("localhost"));
        assertEquals(new JedisL2Provider.Endpoint("127.0.0.1", 7001),
                JedisL2Provider.Endpoint.parse("127.0.0.1:7001"));
        assertEquals(new JedisL2Provider.Endpoint("2001:db8::1", 6379),
                JedisL2Provider.Endpoint.parse("2001:db8::1"));
        assertEquals(new JedisL2Provider.Endpoint("2001:db8::1", 7001),
                JedisL2Provider.Endpoint.parse("[2001:db8::1]:7001"));
        assertEquals(new JedisL2Provider.Endpoint("2001:db8::1", 6379),
                JedisL2Provider.Endpoint.parse("[2001:db8::1]"));

        assertThrows(IllegalArgumentException.class, () -> JedisL2Provider.Endpoint.parse("localhost:0"));
        assertThrows(IllegalArgumentException.class, () -> JedisL2Provider.Endpoint.parse("localhost:65536"));
        assertThrows(IllegalArgumentException.class, () -> JedisL2Provider.Endpoint.parse("[2001:db8::1"));
    }

    @Test
    void pubSubExecutorDoesNotRunBlockingSubscriptionsOnCallerWhenSaturated() throws Exception {
        CacheConfig.Subscriber subscriber = new CacheConfig.Subscriber();
        subscriber.setCorePoolSize(0);
        subscriber.setMaximumPoolSize(1);
        subscriber.setKeepAliveTime(Duration.ofMillis(100));
        subscriber.setCapacity(1);

        Thread caller = Thread.currentThread();
        AtomicBoolean ranOnCaller = new AtomicBoolean(false);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        ThreadPoolExecutor executor = JedisL2Provider.newPubSubExecutor(subscriber);
        try {
            executor.execute(() -> {
                if (Thread.currentThread() == caller) {
                    ranOnCaller.set(true);
                }
                started.countDown();
                try {
                    release.await(2, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            assertTrue(started.await(2, TimeUnit.SECONDS));
            assertThrows(RejectedExecutionException.class, () -> executor.execute(() -> ranOnCaller.set(true)));
            assertFalse(ranOnCaller.get());
            release.countDown();
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }
}
