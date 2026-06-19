package io.github.dk900912.multitiercache.spi;

import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.CacheMessageListener;
import io.github.dk900912.multitiercache.api.CacheMessageSubscription;
import io.github.dk900912.multitiercache.api.model.CacheConfig;

import io.github.dk900912.multitiercache.spi.support.RedisL2ReentrantLock;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RedisL2ReentrantLockTest {

    @Test
    void unlockShouldNotCreateLatchKey() {
        FakeL2Provider provider = new FakeL2Provider();
        try (RedisL2ReentrantLock.RenewalRegistry registry = new RedisL2ReentrantLock.RenewalRegistry(2)) {
            L2ReentrantLock lock = new RedisL2ReentrantLock(
                    provider, "lock:no:latch", "client", Duration.ofMillis(500), registry);

            lock.lock(Duration.ofSeconds(5));
            lock.unlock();

            assertEquals(2, provider.lastUnlockKeys.size());
            assertFalse(provider.lastUnlockScript.contains("KEYS[3]"));
            assertFalse(provider.lastUnlockScript.contains("redis.call('set'"));
        }
    }

    @Test
    void watchdogShouldRetryAfterTransientRenewalFailure() throws InterruptedException {
        FakeL2Provider provider = new FakeL2Provider();
        provider.failFirstRenewal.set(true);
        try (RedisL2ReentrantLock.RenewalRegistry registry = new RedisL2ReentrantLock.RenewalRegistry(2)) {
            L2ReentrantLock lock = new RedisL2ReentrantLock(
                    provider, "lock:renew:retry", "client", Duration.ofMillis(90), registry);

            lock.lock();

            assertTrue(provider.secondRenewal.await(2, TimeUnit.SECONDS));
            assertTrue(lock.isHeldByCurrentThread());
            lock.unlock();
        }
    }

    @Test
    void unlockFailureShouldCancelWatchdogRenewal() throws InterruptedException {
        FakeL2Provider provider = new FakeL2Provider();
        try (RedisL2ReentrantLock.RenewalRegistry registry = new RedisL2ReentrantLock.RenewalRegistry(2)) {
            L2ReentrantLock lock = new RedisL2ReentrantLock(
                    provider, "lock:unlock:failure", "client", Duration.ofMillis(300), registry);

            lock.lock();
            assertTrue(provider.firstRenewal.await(1, TimeUnit.SECONDS));
            provider.throwOnUnlock.set(true);

            assertThrows(IllegalStateException.class, lock::unlock);
            int renewalsAfterFailure = provider.renewals.get();
            Thread.sleep(500);

            assertEquals(renewalsAfterFailure, provider.renewals.get());
        }
    }

    @Test
    void watchdogShouldStopWhenOwnerThreadTerminatesWithoutUnlock() throws InterruptedException {
        FakeL2Provider provider = new FakeL2Provider();
        try (RedisL2ReentrantLock.RenewalRegistry registry = new RedisL2ReentrantLock.RenewalRegistry(2)) {
            L2ReentrantLock lock = new RedisL2ReentrantLock(
                    provider, "lock:owner:terminated", "client", Duration.ofMillis(300), registry);
            CountDownLatch acquired = new CountDownLatch(1);

            Thread owner = new Thread(() -> {
                lock.lock();
                acquired.countDown();
            });
            owner.start();
            assertTrue(acquired.await(1, TimeUnit.SECONDS));
            owner.join(1000);
            assertFalse(owner.isAlive());

            Thread.sleep(450);

            assertEquals(0, provider.renewals.get());
        }
    }

    @Test
    void convenienceConstructorsShouldShareProviderDefaultRegistry() throws InterruptedException {
        FakeL2Provider provider = new FakeL2Provider();
        String name = "lock:default:registry";
        L2ReentrantLock owner = new RedisL2ReentrantLock(provider, name, Duration.ofMillis(500));
        owner.lock(Duration.ofSeconds(5));

        CountDownLatch started = new CountDownLatch(2);
        AtomicBoolean firstAcquired = new AtomicBoolean(false);
        AtomicBoolean secondAcquired = new AtomicBoolean(false);
        Thread first = convenienceWaiter(provider, name, started, firstAcquired);
        Thread second = convenienceWaiter(provider, name, started, secondAcquired);

        first.start();
        second.start();
        assertTrue(started.await(1, TimeUnit.SECONDS));
        Thread.sleep(150);

        assertEquals(1, provider.subscribeCount.get());

        owner.unlock();
        first.join(2000);
        second.join(2000);

        assertTrue(firstAcquired.get());
        assertTrue(secondAcquired.get());
    }

    @Test
    void waitingLocksShouldShareOneUnderlyingSubscriptionPerChannel() throws InterruptedException {
        FakeL2Provider provider = new FakeL2Provider();
        try (RedisL2ReentrantLock.RenewalRegistry registry = new RedisL2ReentrantLock.RenewalRegistry(2)) {
            String name = "lock:shared:subscription";
            L2ReentrantLock owner = new RedisL2ReentrantLock(
                    provider, name, "client", Duration.ofMillis(500), registry);
            owner.lock(Duration.ofSeconds(5));

            CountDownLatch started = new CountDownLatch(2);
            AtomicBoolean firstAcquired = new AtomicBoolean(false);
            AtomicBoolean secondAcquired = new AtomicBoolean(false);
            Thread first = waiter(provider, registry, name, started, firstAcquired);
            Thread second = waiter(provider, registry, name, started, secondAcquired);

            first.start();
            second.start();
            assertTrue(started.await(1, TimeUnit.SECONDS));
            Thread.sleep(150);

            assertTrue(first.isAlive());
            assertTrue(second.isAlive());
            assertEquals(1, provider.subscribeCount.get());

            owner.unlock();
            first.join(2000);
            second.join(2000);

            assertFalse(first.isAlive());
            assertFalse(second.isAlive());
            assertTrue(firstAcquired.get());
            assertTrue(secondAcquired.get());
        }
    }

    @Test
    void lockNameShouldRejectBracesWithoutNonEmptyHashTag() {
        FakeL2Provider provider = new FakeL2Provider();
        try (RedisL2ReentrantLock.RenewalRegistry registry = new RedisL2ReentrantLock.RenewalRegistry(2)) {
            assertThrows(IllegalArgumentException.class,
                    () -> new RedisL2ReentrantLock(provider, "{broken", "client", Duration.ofMillis(500), registry));
            assertThrows(IllegalArgumentException.class,
                    () -> new RedisL2ReentrantLock(provider, "broken}", "client", Duration.ofMillis(500), registry));
            assertThrows(IllegalArgumentException.class,
                    () -> new RedisL2ReentrantLock(provider, "{}", "client", Duration.ofMillis(500), registry));

            new RedisL2ReentrantLock(provider, "plain", "client", Duration.ofMillis(500), registry);
            new RedisL2ReentrantLock(provider, "prefix:{tag}:suffix", "client", Duration.ofMillis(500), registry);
        }
    }

    @Test
    void convenienceConstructorShouldUseStableProviderClientId() {
        FakeL2Provider provider = new FakeL2Provider();
        L2ReentrantLock first = new RedisL2ReentrantLock(provider, "lock:stable:client", Duration.ofMillis(500));
        L2ReentrantLock second = new RedisL2ReentrantLock(provider, "lock:stable:client", Duration.ofMillis(500));

        first.lock(Duration.ofSeconds(5));
        assertTrue(second.isHeldByCurrentThread());

        second.unlock();
        assertFalse(first.isLocked());
    }

    @Test
    void noArgumentReentryShouldInheritExplicitOuterLease() {
        FakeL2Provider provider = new FakeL2Provider();
        long outerLeaseMillis = Duration.ofMinutes(5).toMillis();
        try (RedisL2ReentrantLock.RenewalRegistry registry = new RedisL2ReentrantLock.RenewalRegistry(2)) {
            L2ReentrantLock lock = new RedisL2ReentrantLock(
                    provider, "lock:lease:inherit", "client", Duration.ofMillis(300), registry);

            lock.lock(Duration.ofMinutes(5));
            assertEquals(outerLeaseMillis, provider.ttlMillis);

            lock.lock();
            assertEquals(outerLeaseMillis, provider.ttlMillis);

            lock.unlock();
            assertEquals(outerLeaseMillis, provider.ttlMillis);

            lock.unlock();
            assertFalse(lock.isLocked());
        }
    }

    @Test
    void shorterExplicitReentryShouldNotShortenExplicitOuterLease() {
        FakeL2Provider provider = new FakeL2Provider();
        long outerLeaseMillis = Duration.ofMinutes(5).toMillis();
        try (RedisL2ReentrantLock.RenewalRegistry registry = new RedisL2ReentrantLock.RenewalRegistry(2)) {
            L2ReentrantLock lock = new RedisL2ReentrantLock(
                    provider, "lock:lease:restore", "client", Duration.ofMillis(300), registry);

            lock.lock(Duration.ofMinutes(5));
            assertEquals(outerLeaseMillis, provider.ttlMillis);

            lock.lock(Duration.ofSeconds(1));
            assertEquals(outerLeaseMillis, provider.ttlMillis);

            lock.unlock();
            assertEquals(outerLeaseMillis, provider.ttlMillis);

            lock.unlock();
            assertFalse(lock.isLocked());
        }
    }

    @Test
    void explicitReentryShouldNotShortenOuterWatchdogLease() {
        FakeL2Provider provider = new FakeL2Provider();
        long watchdogMillis = 300;
        try (RedisL2ReentrantLock.RenewalRegistry registry = new RedisL2ReentrantLock.RenewalRegistry(2)) {
            L2ReentrantLock lock = new RedisL2ReentrantLock(
                    provider, "lock:lease:watchdog", "client", Duration.ofMillis(watchdogMillis), registry);

            lock.lock();
            assertEquals(watchdogMillis, provider.ttlMillis);

            lock.lock(Duration.ofMillis(50));
            assertEquals(watchdogMillis, provider.ttlMillis);

            lock.unlock();
            assertEquals(watchdogMillis, provider.ttlMillis);

            lock.unlock();
            assertFalse(lock.isLocked());
        }
    }

    private static Thread waiter(
            FakeL2Provider provider,
            RedisL2ReentrantLock.RenewalRegistry registry,
            String name,
            CountDownLatch started,
            AtomicBoolean acquired) {
        return new Thread(() -> {
            started.countDown();
            try {
                L2ReentrantLock lock = new RedisL2ReentrantLock(
                        provider, name, "client", Duration.ofMillis(500), registry);
                acquired.set(lock.tryLock(Duration.ofSeconds(1), Duration.ofSeconds(5)));
                if (acquired.get()) {
                    lock.unlock();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    private static Thread convenienceWaiter(
            FakeL2Provider provider,
            String name,
            CountDownLatch started,
            AtomicBoolean acquired) {
        return new Thread(() -> {
            started.countDown();
            try {
                L2ReentrantLock lock = new RedisL2ReentrantLock(provider, name, Duration.ofMillis(500));
                acquired.set(lock.tryLock(Duration.ofSeconds(1), Duration.ofSeconds(5)));
                if (acquired.get()) {
                    lock.unlock();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    private static final class FakeL2Provider implements L2Provider {
        private final Map<String, CopyOnWriteArrayList<CacheMessageListener>> listeners = new ConcurrentHashMap<>();
        private final AtomicInteger subscribeCount = new AtomicInteger();
        private final AtomicBoolean failFirstRenewal = new AtomicBoolean();
        private final AtomicBoolean throwOnUnlock = new AtomicBoolean();
        private final CountDownLatch firstRenewal = new CountDownLatch(1);
        private final CountDownLatch secondRenewal = new CountDownLatch(1);
        private final AtomicInteger renewals = new AtomicInteger();
        private String owner;
        private int holdCount;
        private volatile long ttlMillis = -2;
        private volatile String lastUnlockScript = "";
        private volatile List<String> lastUnlockKeys = List.of();

        @Override
        public void initialize(CacheConfig.L2Config config) {
        }

        @Override
        public String get(CacheKey key) {
            return null;
        }

        @Override
        public void set(CacheKey key, String value, Duration ttl) {
        }

        @Override
        public void delete(CacheKey key) {
        }

        @Override
        public void publish(String channel, String message, L2PubSubMode mode) {
            publishToListeners(channel, message);
        }

        @Override
        public CacheMessageSubscription subscribe(String channel, CacheMessageListener listener, L2PubSubMode mode) {
            subscribeCount.incrementAndGet();
            listeners.computeIfAbsent(channel, ignored -> new CopyOnWriteArrayList<>()).add(listener);
            return () -> listeners.getOrDefault(channel, new CopyOnWriteArrayList<>()).remove(listener);
        }

        @Override
        public synchronized Object eval(String script, List<String> keys, List<String> args) {
            if (script.contains("hincrby', KEYS[1], ARGV[2], 1")) {
                return tryAcquire(args.get(1), Long.parseLong(args.get(0)), "1".equals(args.get(2)));
            }
            if (script.contains("hincrby', KEYS[1], ARGV[2], -1")) {
                return unlock(script, keys, args);
            }
            if (script.contains("redis.call('del', KEYS[1]) == 1")) {
                boolean removed = owner != null;
                owner = null;
                holdCount = 0;
                ttlMillis = -2;
                if (removed) {
                    publishToListeners(keys.get(1), args.get(0));
                }
                return removed ? 1 : 0;
            }
            if (script.contains("redis.call('pexpire', KEYS[1], ARGV[1])")
                    && !script.contains("hincrby")) {
                return renew(args.get(1), Long.parseLong(args.get(0)));
            }
            if (script.contains("return redis.call('exists'")) {
                return owner == null ? 0 : 1;
            }
            if (script.contains("return redis.call('hexists'")) {
                return args.get(0).equals(owner) ? 1 : 0;
            }
            if (script.contains("return tonumber(count)")) {
                return args.get(0).equals(owner) ? holdCount : 0;
            }
            if (script.contains("return redis.call('pttl'")) {
                return owner == null ? -2 : ttlMillis;
            }
            throw new IllegalArgumentException("Unsupported script: " + script);
        }

        private Object tryAcquire(String requestedOwner, long leaseMillis, boolean updateLeaseOnReentry) {
            if (owner == null) {
                owner = requestedOwner;
                holdCount++;
                ttlMillis = leaseMillis;
                return null;
            }
            if (owner.equals(requestedOwner)) {
                holdCount++;
                if (updateLeaseOnReentry) {
                    ttlMillis = leaseMillis;
                }
                return null;
            }
            return ttlMillis > 0 ? ttlMillis : 1000;
        }

        private Object unlock(String script, List<String> keys, List<String> args) {
            if (throwOnUnlock.get()) {
                throw new IllegalStateException("unlock failed");
            }
            lastUnlockScript = script;
            lastUnlockKeys = List.copyOf(keys);
            if (!args.get(1).equals(owner)) {
                return null;
            }
            holdCount--;
            if (holdCount > 0) {
                ttlMillis = Long.parseLong(args.get(0));
                return 0;
            }
            owner = null;
            ttlMillis = -2;
            publishToListeners(keys.get(1), args.get(2));
            return 1;
        }

        private Object renew(String requestedOwner, long leaseMillis) {
            int renewal = renewals.incrementAndGet();
            firstRenewal.countDown();
            if (failFirstRenewal.compareAndSet(true, false)) {
                throw new IllegalStateException("transient renewal failure");
            }
            if (renewal >= 2) {
                secondRenewal.countDown();
            }
            if (requestedOwner.equals(owner)) {
                ttlMillis = leaseMillis;
                return 1;
            }
            return 0;
        }

        private void publishToListeners(String channel, String message) {
            for (CacheMessageListener listener : listeners.getOrDefault(channel, new CopyOnWriteArrayList<>())) {
                listener.onMessage(channel, message);
            }
        }
    }
}
