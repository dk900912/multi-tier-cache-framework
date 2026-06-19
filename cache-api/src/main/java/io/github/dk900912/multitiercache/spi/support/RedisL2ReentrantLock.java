package io.github.dk900912.multitiercache.spi.support;

import io.github.dk900912.multitiercache.api.CacheMessageListener;
import io.github.dk900912.multitiercache.api.CacheMessageSubscription;
import io.github.dk900912.multitiercache.spi.L2Provider;
import io.github.dk900912.multitiercache.spi.L2PubSubMode;
import io.github.dk900912.multitiercache.spi.L2ReentrantLock;

import java.lang.ref.WeakReference;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Redis-backed implementation of Redisson-style reentrant lock semantics.
 * <p>
 * This helper is intended for L2 providers that do not expose Redisson's
 * native {@code RLock} but still need the same hash-owner and hold-count
 * release semantics.
 * </p>
 *
 * @author dukui
 */
public final class RedisL2ReentrantLock implements L2ReentrantLock {

    private static final String UNLOCK_MESSAGE = "0";
    private static final String RENEWAL_THREADS_PROPERTY = "multitiercache.lock.renewal.threads";
    private static final Map<L2Provider, String> DEFAULT_CLIENT_IDS = new WeakHashMap<>();
    private static final Map<L2Provider, RenewalRegistry> DEFAULT_RENEWAL_REGISTRIES = new WeakHashMap<>();
    private static final ReentrantLock DEFAULT_CLIENT_IDS_LOCK = new ReentrantLock();
    private static final ReentrantLock DEFAULT_RENEWAL_REGISTRIES_LOCK = new ReentrantLock();
    private static final AtomicInteger RENEWAL_COUNTER = new AtomicInteger(1);

    private static final String TRY_LOCK_SCRIPT = """
            if (redis.call('exists', KEYS[1]) == 0) then
                redis.call('hincrby', KEYS[1], ARGV[2], 1);
                redis.call('pexpire', KEYS[1], ARGV[1]);
                return nil;
            end;
            if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then
                redis.call('hincrby', KEYS[1], ARGV[2], 1);
                if (ARGV[3] == '1') then
                    redis.call('pexpire', KEYS[1], ARGV[1]);
                end;
                return nil;
            end;
            return redis.call('pttl', KEYS[1]);""";

    private static final String UNLOCK_SCRIPT = """
            if (redis.call('hexists', KEYS[1], ARGV[2]) == 0) then
                return nil;
            end;
            local counter = redis.call('hincrby', KEYS[1], ARGV[2], -1);
            if (counter > 0) then
                redis.call('pexpire', KEYS[1], ARGV[1]);
                return 0;
            else
                redis.call('del', KEYS[1]);
                redis.call(ARGV[4], KEYS[2], ARGV[3]);
                return 1;
            end;""";

    private static final String FORCE_UNLOCK_SCRIPT = """
            if (redis.call('del', KEYS[1]) == 1) then
                redis.call(ARGV[2], KEYS[2], ARGV[1]);
                return 1;
            end;
            return 0;""";

    private static final String RENEW_SCRIPT = """
            if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then
                redis.call('pexpire', KEYS[1], ARGV[1]);
                return 1;
            end;
            return 0;""";

    private static final String IS_LOCKED_SCRIPT = """
            return redis.call('exists', KEYS[1]);""";

    private static final String IS_HELD_SCRIPT = """
            return redis.call('hexists', KEYS[1], ARGV[1]);""";

    private static final String HOLD_COUNT_SCRIPT = """
            local count = redis.call('hget', KEYS[1], ARGV[1]);
            if not count then
                return 0;
            end;
            return tonumber(count);""";

    private static final String PTTL_SCRIPT = """
            return redis.call('pttl', KEYS[1]);""";

    private final L2Provider provider;
    private final String name;
    private final String clientId;
    private final Duration watchdogTimeout;
    private final RenewalRegistry renewalRegistry;
    private final String publishCommand;
    private final L2PubSubMode pubSubMode;

    public RedisL2ReentrantLock(
            L2Provider provider,
            String name,
            String clientId,
            Duration watchdogTimeout,
            RenewalRegistry renewalRegistry,
            String publishCommand) {
        this.provider = Objects.requireNonNull(provider, "L2 provider cannot be null");
        this.name = validateLockName(name);
        this.clientId = Objects.requireNonNull(clientId, "Lock client id cannot be null");
        this.watchdogTimeout = validatePositive(
                Objects.requireNonNull(watchdogTimeout, "Lock watchdog timeout cannot be null"),
                "Lock watchdog timeout");
        this.renewalRegistry = Objects.requireNonNull(renewalRegistry, "Lock renewal registry cannot be null");
        this.publishCommand = validatePublishCommand(publishCommand);
        this.pubSubMode = "SPUBLISH".equals(this.publishCommand)
                ? L2PubSubMode.SHARDED
                : L2PubSubMode.STANDARD;
    }

    public RedisL2ReentrantLock(
            L2Provider provider,
            String name,
            String clientId,
            Duration watchdogTimeout,
            RenewalRegistry renewalRegistry) {
        this(provider, name, clientId, watchdogTimeout, renewalRegistry, "PUBLISH");
    }

    public RedisL2ReentrantLock(L2Provider provider, String name, String clientId, Duration watchdogTimeout) {
        this(provider, name, clientId, watchdogTimeout, defaultRenewalRegistry(provider));
    }

    public RedisL2ReentrantLock(L2Provider provider, String name, Duration watchdogTimeout) {
        this(provider, name, defaultClientId(provider), watchdogTimeout);
    }

    @Override
    public void lock() {
        lockUninterruptibly(null);
    }

    @Override
    public void lock(Duration leaseTime) {
        lockUninterruptibly(validatePositive(leaseTime, "Lock lease time"));
    }

    @Override
    public boolean tryLock() {
        try {
            return tryAcquire(null) == null;
        } catch (RuntimeException e) {
            throw e;
        }
    }

    @Override
    public boolean tryLock(Duration waitTime) throws InterruptedException {
        validateWaitTime(waitTime);
        return tryLockWithOptionalLease(waitTime, null);
    }

    @Override
    public boolean tryLock(Duration waitTime, Duration leaseTime) throws InterruptedException {
        validateWaitTime(waitTime);
        validatePositive(leaseTime, "Lock lease time");
        return tryLockWithOptionalLease(waitTime, leaseTime);
    }

    private boolean tryLockWithOptionalLease(Duration waitTime, Duration leaseTime) throws InterruptedException {
        if (waitTime.isZero()) {
            return tryAcquire(leaseTime) == null;
        }
        return lockWithWait(waitTime.toMillis(), leaseTime);
    }

    @Override
    public void unlock() {
        long threadId = Thread.currentThread().threadId();
        long leaseMillis = renewalRegistry.leaseMillisForUnlock(name, threadId, watchdogTimeout.toMillis());
        Object result;
        try {
            result = provider.eval(UNLOCK_SCRIPT,
                    List.of(name, channelName()),
                    List.of(Long.toString(leaseMillis), ownerName(threadId), UNLOCK_MESSAGE,
                            publishCommand));
        } catch (RuntimeException e) {
            cancelRenewal(threadId, false);
            renewalRegistry.clearLeases(name, threadId);
            throw e;
        }
        Boolean unlocked = toBooleanOrNull(result);
        if (unlocked == null) {
            cancelRenewal(threadId, false);
            renewalRegistry.clearLeases(name, threadId);
            throw new IllegalMonitorStateException("attempt to unlock lock, not locked by current thread by node id: "
                    + clientId + " thread-id: " + threadId);
        }
        if (unlocked) {
            cancelRenewal(threadId, false);
            renewalRegistry.clearLeases(name, threadId);
        } else {
            renewalRegistry.popLease(name, threadId);
        }
    }

    @Override
    public boolean forceUnlock() {
        boolean unlocked = toBoolean(provider.eval(FORCE_UNLOCK_SCRIPT,
                List.of(name, channelName()),
                List.of(UNLOCK_MESSAGE, publishCommand)));
        if (unlocked) {
            cancelAllRenewalsForLock();
            renewalRegistry.clearAllLeases(name);
        }
        return unlocked;
    }

    @Override
    public boolean isLocked() {
        return toBoolean(provider.eval(IS_LOCKED_SCRIPT, List.of(name), List.of()));
    }

    @Override
    public boolean isHeldByCurrentThread() {
        return toBoolean(provider.eval(IS_HELD_SCRIPT, List.of(name), List.of(ownerName(Thread.currentThread().threadId()))));
    }

    @Override
    public int getHoldCount() {
        return Math.toIntExact(toLong(provider.eval(HOLD_COUNT_SCRIPT,
                List.of(name),
                List.of(ownerName(Thread.currentThread().threadId())))));
    }

    @Override
    public Duration remainTimeToLive() {
        return Duration.ofMillis(toLong(provider.eval(PTTL_SCRIPT, List.of(name), List.of())));
    }

    private void lockUninterruptibly(Duration leaseTime) {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    if (lockWithWait(-1, leaseTime)) {
                        return;
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private boolean lockWithWait(long waitTimeMillis, Duration leaseTime) throws InterruptedException {
        long deadline = waitTimeMillis < 0 ? Long.MAX_VALUE : System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(waitTimeMillis);
        Long ttl = tryAcquire(leaseTime);
        if (ttl == null) {
            return true;
        }
        if (waitTimeMillis == 0) {
            return false;
        }

        String channelName = channelName();
        Semaphore unlockSignal = new Semaphore(0);
        CacheMessageSubscription subscription = renewalRegistry.subscribe(
                provider, channelName, (channel, message) -> {
            if (channelName.equals(channel) && UNLOCK_MESSAGE.equals(message)) {
                unlockSignal.release();
            }
        }, pubSubMode);
        try {
            while (true) {
                long remaining = remainingMillis(deadline);
                if (remaining == 0) {
                    return false;
                }

                ttl = tryAcquire(leaseTime);
                if (ttl == null) {
                    return true;
                }

                long waitMillis = waitMillis(ttl, remaining);
                if (waitMillis <= 0) {
                    waitMillis = Math.min(watchdogTimeout.toMillis(), remaining);
                }
                unlockSignal.tryAcquire(waitMillis, TimeUnit.MILLISECONDS);
                unlockSignal.drainPermits();
            }
        } finally {
            subscription.close();
        }
    }

    private Long tryAcquire(Duration leaseTime) {
        long threadId = Thread.currentThread().threadId();
        if (renewalRegistry.hasLeases(name, threadId) && !isHeldByThread(threadId)) {
            renewalRegistry.clearLeases(name, threadId);
        }
        LeaseEntry lease = renewalRegistry.leaseForAcquire(name, threadId, leaseTime, watchdogTimeout);
        Object result = provider.eval(TRY_LOCK_SCRIPT,
                List.of(name),
                List.of(Long.toString(lease.leaseMillis()), ownerName(threadId), lease.updateLeaseOnReentry() ? "1" : "0"));
        Long ttl = toLongOrNull(result);
        if (ttl == null) {
            renewalRegistry.pushLease(name, threadId, lease);
            if (lease.watchdog()) {
                scheduleRenewal(threadId);
            }
        }
        return ttl;
    }

    private boolean isHeldByThread(long threadId) {
        return toBoolean(provider.eval(IS_HELD_SCRIPT, List.of(name), List.of(ownerName(threadId))));
    }

    private void scheduleRenewal(long threadId) {
        renewalRegistry.schedule(provider, name, threadId, ownerName(threadId), Thread.currentThread(), watchdogTimeout);
    }

    private void cancelRenewal(long threadId, boolean mayInterrupt) {
        renewalRegistry.cancel(name, threadId, mayInterrupt);
    }

    private void cancelAllRenewalsForLock() {
        renewalRegistry.cancelAll(name, false);
    }

    private long waitMillis(Long ttl, long remaining) {
        if (remaining == Long.MAX_VALUE) {
            return ttl != null && ttl > 0 ? ttl : watchdogTimeout.toMillis();
        }
        if (ttl != null && ttl > 0) {
            return Math.min(ttl, remaining);
        }
        return remaining;
    }

    private static long remainingMillis(long deadline) {
        if (deadline == Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        }
        long remainingNanos = deadline - System.nanoTime();
        if (remainingNanos <= 0) {
            return 0;
        }
        return Math.max(1, TimeUnit.NANOSECONDS.toMillis(remainingNanos));
    }

    private String ownerName(long threadId) {
        return clientId + ":" + threadId;
    }

    private String channelName() {
        return prefixedName("redisson_lock__channel", name);
    }

    private static String prefixedName(String prefix, String rawName) {
        if (hasValidHashTag(rawName)) {
            return prefix + ":" + rawName;
        }
        return prefix + ":{" + rawName + "}";
    }

    public static String validateLockName(String name) {
        Objects.requireNonNull(name, "Lock name cannot be null");
        if (name.isEmpty()) {
            throw new IllegalArgumentException("Lock name cannot be empty");
        }
        if (containsBrace(name) && !hasValidHashTag(name)) {
            throw new IllegalArgumentException("Lock name with braces must contain a non-empty Redis hash tag");
        }
        return name;
    }

    private static boolean containsBrace(String value) {
        return value.indexOf('{') >= 0 || value.indexOf('}') >= 0;
    }

    private static boolean hasValidHashTag(String value) {
        int start = value.indexOf('{');
        if (start < 0) {
            return false;
        }
        int end = value.indexOf('}', start + 1);
        return end > start + 1;
    }

    private static String defaultClientId(L2Provider provider) {
        Objects.requireNonNull(provider, "L2 provider cannot be null");
        DEFAULT_CLIENT_IDS_LOCK.lock();
        try {
            return DEFAULT_CLIENT_IDS.computeIfAbsent(provider, ignored -> UUID.randomUUID().toString());
        } finally {
            DEFAULT_CLIENT_IDS_LOCK.unlock();
        }
    }

    private static RenewalRegistry defaultRenewalRegistry(L2Provider provider) {
        Objects.requireNonNull(provider, "L2 provider cannot be null");
        DEFAULT_RENEWAL_REGISTRIES_LOCK.lock();
        try {
            RenewalRegistry registry = DEFAULT_RENEWAL_REGISTRIES.get(provider);
            if (registry == null) {
                registry = new RenewalRegistry();
                DEFAULT_RENEWAL_REGISTRIES.put(provider, registry);
            }
            return registry;
        } finally {
            DEFAULT_RENEWAL_REGISTRIES_LOCK.unlock();
        }
    }

    private static Duration validatePositive(Duration duration, String label) {
        Objects.requireNonNull(duration, label + " cannot be null");
        if (duration.toMillis() <= 0) {
            throw new IllegalArgumentException(label + " must be positive");
        }
        return duration;
    }

    private static String validatePublishCommand(String publishCommand) {
        Objects.requireNonNull(publishCommand, "Lock publish command cannot be null");
        if (!"PUBLISH".equals(publishCommand) && !"SPUBLISH".equals(publishCommand)) {
            throw new IllegalArgumentException("Lock publish command must be PUBLISH or SPUBLISH");
        }
        return publishCommand;
    }

    private static void validateWaitTime(Duration waitTime) {
        Objects.requireNonNull(waitTime, "Lock wait time cannot be null");
        if (waitTime.isNegative()) {
            throw new IllegalArgumentException("Lock wait time cannot be negative");
        }
    }

    private static Boolean toBooleanOrNull(Object value) {
        if (value == null) {
            return null;
        }
        return toLong(value) != 0;
    }

    private static boolean toBoolean(Object value) {
        return toLong(value) != 0;
    }

    private static Long toLongOrNull(Object value) {
        if (value == null) {
            return null;
        }
        return toLong(value);
    }

    private static long toLong(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value instanceof Boolean bool) {
            return bool ? 1 : 0;
        }
        return Long.parseLong(value.toString());
    }

    public static final class RenewalRegistry implements AutoCloseable {
        private final ConcurrentMap<RenewalKey, Renewal> renewals = new ConcurrentHashMap<>();
        private final ConcurrentMap<RenewalKey, LeaseState> leases = new ConcurrentHashMap<>();
        private final ConcurrentMap<SubscriptionKey, SharedSubscription> subscriptions = new ConcurrentHashMap<>();
        private final ScheduledExecutorService renewalExecutor;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        public RenewalRegistry() {
            this(defaultRenewalThreadCount());
        }

        public RenewalRegistry(int renewalThreads) {
            if (renewalThreads <= 0) {
                throw new IllegalArgumentException("Lock renewal thread count must be positive");
            }
            ScheduledThreadPoolExecutor executor =
                    new ScheduledThreadPoolExecutor(renewalThreads, new RenewalThreadFactory());
            executor.setKeepAliveTime(10, TimeUnit.SECONDS);
            executor.allowCoreThreadTimeOut(true);
            this.renewalExecutor = executor;
        }

        private void schedule(
                L2Provider provider,
                String name,
                long threadId,
                String ownerName,
                Thread ownerThread,
                Duration watchdogTimeout) {
            ensureOpen();
            RenewalKey key = new RenewalKey(name, threadId);
            Renewal renewal = renewals.get(key);
            if (renewal != null) {
                return;
            }
            Renewal newRenewal = new Renewal(this, key, provider, ownerName, ownerThread, watchdogTimeout);
            Renewal existing = renewals.putIfAbsent(key, newRenewal);
            if (existing == null) {
                newRenewal.schedule();
            }
        }

        private CacheMessageSubscription subscribe(
                L2Provider provider,
                String channel,
                CacheMessageListener listener,
                L2PubSubMode mode) {
            SubscriptionKey key = new SubscriptionKey(channel, mode);
            while (true) {
                ensureOpen();
                SharedSubscription subscription = subscriptions.computeIfAbsent(
                        key, ignored -> new SharedSubscription(this, key));
                if (subscription.isClosed()) {
                    subscriptions.remove(key, subscription);
                    continue;
                }
                try {
                    subscription.connect(provider);
                } catch (SharedSubscription.SubscriptionClosedException e) {
                    subscriptions.remove(key, subscription);
                    continue;
                } catch (RuntimeException e) {
                    subscriptions.remove(key, subscription);
                    subscription.close();
                    throw e;
                }
                try {
                    return subscription.add(listener);
                } catch (IllegalStateException ignored) {
                    subscriptions.remove(key, subscription);
                }
            }
        }

        private void cancel(String name, long threadId, boolean mayInterrupt) {
            Renewal renewal = renewals.remove(new RenewalKey(name, threadId));
            if (renewal != null) {
                renewal.cancel(mayInterrupt);
            }
        }

        private void cancel(RenewalKey key, Renewal renewal, boolean mayInterrupt) {
            if (renewals.remove(key, renewal)) {
                renewal.cancel(mayInterrupt);
            }
        }

        public void cancelAll() {
            for (RenewalKey key : renewals.keySet()) {
                cancel(key.name, key.threadId, true);
            }
        }

        private void cancelAll(String name, boolean mayInterrupt) {
            for (RenewalKey key : renewals.keySet()) {
                if (key.name.equals(name)) {
                    cancel(key.name, key.threadId, mayInterrupt);
                }
            }
        }

        private LeaseEntry leaseForAcquire(String name, long threadId, Duration leaseTime, Duration watchdogTimeout) {
            RenewalKey key = new RenewalKey(name, threadId);
            LeaseEntry current = currentLease(key);
            long watchdogMillis = watchdogTimeout.toMillis();
            if (leaseTime != null) {
                long leaseMillis = leaseTime.toMillis();
                if (current != null) {
                    leaseMillis = Math.max(leaseMillis, current.leaseMillis());
                } else if (hasWatchdogLease(key)) {
                    leaseMillis = Math.max(leaseMillis, watchdogMillis);
                }
                return new LeaseEntry(leaseMillis, false, true);
            }
            if (current != null) {
                return new LeaseEntry(current.leaseMillis(), current.watchdog(), true);
            }
            return new LeaseEntry(watchdogMillis, true, true);
        }

        private void pushLease(String name, long threadId, LeaseEntry lease) {
            RenewalKey key = new RenewalKey(name, threadId);
            leases.computeIfAbsent(key, ignored -> new LeaseState()).push(lease);
        }

        private void popLease(String name, long threadId) {
            RenewalKey key = new RenewalKey(name, threadId);
            LeaseState state = leases.get(key);
            if (state != null && state.pop()) {
                leases.remove(key, state);
            }
        }

        private long leaseMillisForUnlock(String name, long threadId, long defaultLeaseMillis) {
            LeaseState state = leases.get(new RenewalKey(name, threadId));
            if (state == null) {
                return defaultLeaseMillis;
            }
            return state.leaseMillisAfterUnlock(defaultLeaseMillis);
        }

        private long leaseMillisForRenewal(RenewalKey key, long defaultLeaseMillis) {
            LeaseEntry lease = currentLease(key);
            return lease == null ? defaultLeaseMillis : lease.leaseMillis();
        }

        private boolean hasLeases(String name, long threadId) {
            LeaseState state = leases.get(new RenewalKey(name, threadId));
            return state != null && !state.isEmpty();
        }

        private boolean hasWatchdogLease(RenewalKey key) {
            LeaseState state = leases.get(key);
            return state != null && state.containsWatchdog();
        }

        private void clearLeases(String name, long threadId) {
            leases.remove(new RenewalKey(name, threadId));
        }

        private void clearAllLeases(String name) {
            for (RenewalKey key : leases.keySet()) {
                if (key.name.equals(name)) {
                    leases.remove(key);
                }
            }
        }

        private LeaseEntry currentLease(RenewalKey key) {
            LeaseState state = leases.get(key);
            return state == null ? null : state.peek();
        }

        private ScheduledFuture<?> scheduleRenewal(Runnable task, long delayMillis) {
            if (closed.get()) {
                return null;
            }
            try {
                return renewalExecutor.schedule(task, delayMillis, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException e) {
                if (closed.get()) {
                    return null;
                }
                throw e;
            }
        }

        private boolean stillRegistered(RenewalKey key, Renewal renewal) {
            return renewals.get(key) == renewal;
        }

        private void removeSubscription(SubscriptionKey key, SharedSubscription subscription) {
            subscriptions.remove(key, subscription);
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                cancelAll();
                leases.clear();
                for (SharedSubscription subscription : subscriptions.values()) {
                    subscription.close();
                }
                subscriptions.clear();
                renewalExecutor.shutdownNow();
            }
        }

        private void ensureOpen() {
            if (closed.get()) {
                throw new IllegalStateException("Lock renewal registry is closed");
            }
        }
    }

    private static final class SharedSubscription {
        private final RenewalRegistry registry;
        private final SubscriptionKey key;
        private final ConcurrentMap<CacheMessageListener, Boolean> listeners = new ConcurrentHashMap<>();
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final ReentrantLock lock = new ReentrantLock();
        private volatile CacheMessageSubscription delegate;

        private SharedSubscription(RenewalRegistry registry, SubscriptionKey key) {
            this.registry = registry;
            this.key = key;
        }

        private void connect(L2Provider provider) {
            lock.lock();
            try {
                if (closed.get()) {
                    throw new SubscriptionClosedException("Lock subscription is closed concurrently");
                }
                if (delegate == null) {
                    delegate = provider.subscribe(key.channel, this::onMessage, key.mode);
                }
            } finally {
                lock.unlock();
            }
        }

        private CacheMessageSubscription add(CacheMessageListener listener) {
            Objects.requireNonNull(listener, "Lock subscription listener cannot be null");
            lock.lock();
            try {
                if (closed.get()) {
                    throw new IllegalStateException("Lock subscription is closed");
                }
                listeners.put(listener, Boolean.TRUE);
            } finally {
                lock.unlock();
            }
            AtomicBoolean removed = new AtomicBoolean(false);
            return () -> {
                if (removed.compareAndSet(false, true)) {
                    remove(listener);
                }
            };
        }

        private void remove(CacheMessageListener listener) {
            boolean shouldClose;
            lock.lock();
            try {
                listeners.remove(listener);
                shouldClose = listeners.isEmpty() && closed.compareAndSet(false, true);
            } finally {
                lock.unlock();
            }
            if (shouldClose) {
                registry.removeSubscription(key, this);
                closeDelegate();
            }
        }

        private void close() {
            boolean shouldClose;
            lock.lock();
            try {
                shouldClose = closed.compareAndSet(false, true);
                listeners.clear();
            } finally {
                lock.unlock();
            }
            if (shouldClose) {
                registry.removeSubscription(key, this);
                closeDelegate();
            }
        }

        private boolean isClosed() {
            return closed.get();
        }

        private void onMessage(String receivedChannel, String payload) {
            for (CacheMessageListener listener : listeners.keySet()) {
                try {
                    listener.onMessage(receivedChannel, payload);
                } catch (Exception ignored) {
                    // One waiter must not prevent the shared subscription from waking the rest.
                }
            }
        }

        private void closeDelegate() {
            CacheMessageSubscription current = delegate;
            if (current != null) {
                current.close();
            }
        }

        private static final class SubscriptionClosedException extends RuntimeException {
            private SubscriptionClosedException(String message) {
                super(message);
            }
        }
    }

    private record RenewalKey(String name, long threadId) {
    }

    private record SubscriptionKey(String channel, L2PubSubMode mode) {
        private SubscriptionKey {
            Objects.requireNonNull(channel, "Lock subscription channel cannot be null");
            Objects.requireNonNull(mode, "Lock subscription mode cannot be null");
        }
    }

    private record LeaseEntry(long leaseMillis, boolean watchdog, boolean updateLeaseOnReentry) {
    }

    private static final class LeaseState {
        private final ReentrantLock lock = new ReentrantLock();
        private final Deque<LeaseEntry> stack = new ArrayDeque<>();

        private void push(LeaseEntry lease) {
            lock.lock();
            try {
                stack.push(lease);
            } finally {
                lock.unlock();
            }
        }

        private boolean pop() {
            lock.lock();
            try {
                if (!stack.isEmpty()) {
                    stack.pop();
                }
                return stack.isEmpty();
            } finally {
                lock.unlock();
            }
        }

        private LeaseEntry peek() {
            lock.lock();
            try {
                return stack.peek();
            } finally {
                lock.unlock();
            }
        }

        private boolean isEmpty() {
            lock.lock();
            try {
                return stack.isEmpty();
            } finally {
                lock.unlock();
            }
        }

        private long leaseMillisAfterUnlock(long defaultLeaseMillis) {
            lock.lock();
            try {
                if (stack.isEmpty()) {
                    return defaultLeaseMillis;
                }
                Iterator<LeaseEntry> iterator = stack.iterator();
                LeaseEntry current = iterator.next();
                if (iterator.hasNext()) {
                    return iterator.next().leaseMillis();
                }
                return current.leaseMillis();
            } finally {
                lock.unlock();
            }
        }

        private boolean containsWatchdog() {
            lock.lock();
            try {
                for (LeaseEntry lease : stack) {
                    if (lease.watchdog()) {
                        return true;
                    }
                }
                return false;
            } finally {
                lock.unlock();
            }
        }
    }

    private static final class RenewalThreadFactory implements ThreadFactory {
        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable);
            thread.setName("redis-l2-lock-renewal-" + RENEWAL_COUNTER.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        }
    }

    private static final class Renewal implements Runnable {
        private final RenewalRegistry registry;
        private final RenewalKey key;
        private final WeakReference<L2Provider> providerRef;
        private final WeakReference<Thread> ownerThreadRef;
        private final String ownerName;
        private final Duration watchdogTimeout;
        private final long threadId;
        private volatile ScheduledFuture<?> future;
        // A Renewal schedules its successor only after the current run completes, so this
        // counter is confined to that serial execution chain. Cancellation never reads it.
        private int consecutiveFailures;

        private Renewal(
                RenewalRegistry registry,
                RenewalKey key,
                L2Provider provider,
                String ownerName,
                Thread ownerThread,
                Duration watchdogTimeout) {
            this.registry = registry;
            this.key = key;
            this.providerRef = new WeakReference<>(provider);
            this.ownerThreadRef = new WeakReference<>(ownerThread);
            this.ownerName = ownerName;
            this.watchdogTimeout = watchdogTimeout;
            this.threadId = key.threadId;
        }

        private void schedule() {
            schedule(normalDelayMillis());
        }

        private void schedule(long delayMillis) {
            future = registry.scheduleRenewal(this, delayMillis);
            if (future == null) {
                registry.cancel(key, this, false);
            } else if (!registry.stillRegistered(key, this)) {
                future.cancel(false);
            }
        }

        @Override
        public void run() {
            Thread ownerThread = ownerThreadRef.get();
            if (ownerThread == null || !ownerThread.isAlive()) {
                if (registry.stillRegistered(key, this)) {
                    registry.clearLeases(key.name, key.threadId);
                    registry.cancel(key, this, false);
                }
                return;
            }
            L2Provider currentProvider = providerRef.get();
            if (currentProvider == null) {
                if (registry.stillRegistered(key, this)) {
                    registry.cancel(key, this, false);
                }
                return;
            }
            try {
                long leaseMillis = registry.leaseMillisForRenewal(key, watchdogTimeout.toMillis());
                boolean renewed = toBoolean(currentProvider.eval(RENEW_SCRIPT,
                        List.of(key.name),
                        List.of(Long.toString(leaseMillis), ownerName)));
                if (renewed) {
                    if (registry.stillRegistered(key, this)) {
                        consecutiveFailures = 0;
                        schedule();
                    }
                    return;
                }
            } catch (Exception e) {
                if (registry.stillRegistered(key, this)) {
                    consecutiveFailures++;
                    schedule(retryDelayMillis());
                }
                return;
            }
            if (registry.stillRegistered(key, this)) {
                registry.cancel(key, this, false);
            }
        }

        private void cancel(boolean mayInterrupt) {
            if (future != null) {
                future.cancel(mayInterrupt);
            }
        }

        private long normalDelayMillis() {
            return Math.max(1, watchdogTimeout.toMillis() / 3);
        }

        private long retryDelayMillis() {
            long baseDelay = Math.max(1, watchdogTimeout.toMillis() / 10);
            long maxDelay = Math.max(1, watchdogTimeout.toMillis() / 3);
            int cappedFailures = Math.min(consecutiveFailures - 1, 4);
            long delay = baseDelay * (1L << Math.max(0, cappedFailures));
            return Math.min(maxDelay, Math.min(delay, 1000));
        }
    }

    private static int defaultRenewalThreadCount() {
        Integer configured = Integer.getInteger(RENEWAL_THREADS_PROPERTY);
        if (configured != null) {
            if (configured <= 0) {
                throw new IllegalArgumentException(
                        "System property " + RENEWAL_THREADS_PROPERTY + " must be positive");
            }
            return configured;
        }
        return Math.clamp(Runtime.getRuntime().availableProcessors() * 2L, 4, 32);
    }
}
