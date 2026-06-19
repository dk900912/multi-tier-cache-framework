package io.github.dk900912.multitiercache.provider.redisson;

import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.CacheMessageDeliveryEvent;
import io.github.dk900912.multitiercache.api.CacheMessageDeliveryEventType;
import io.github.dk900912.multitiercache.api.CacheMessageListener;
import io.github.dk900912.multitiercache.api.CacheMessageSubscription;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.spi.L2PubSubMode;
import io.github.dk900912.multitiercache.spi.L2Provider;
import io.github.dk900912.multitiercache.spi.L2ReentrantLock;
import io.github.dk900912.multitiercache.spi.support.RedisL2ReentrantLock;
import io.github.dk900912.multitiercache.spi.support.PubSubMessageDispatcher;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RLock;
import org.redisson.api.RScript;
import org.redisson.api.RShardedTopic;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.api.listener.StatusListener;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Level 2 (L2) cache provider implementation using Redisson.
 *
 * @author dukui
 */
public class RedissonL2Provider implements L2Provider, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedissonL2Provider.class);
    private static final int DEFAULT_PORT = 6379;

    private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock(true);
    private LifecycleState lifecycleState = LifecycleState.NEW;
    private RuntimeResources runtime;

    public RedissonL2Provider() {
    }

    @Override
    public CacheConfig.L2ProviderType providerType() {
        return CacheConfig.L2ProviderType.REDISSON;
    }

    @Override
    public boolean supportsDistributedLock() {
        return true;
    }

    @Override
    public void initialize(CacheConfig.L2Config config) {
        ReentrantReadWriteLock.WriteLock writeLock = lifecycleLock.writeLock();
        writeLock.lock();
        try {
            requireNewState();
            lifecycleState = LifecycleState.INITIALIZING;
            try {
                runtime = createRuntime(Objects.requireNonNull(config, "L2 config cannot be null"));
                lifecycleState = LifecycleState.READY;
            } catch (RuntimeException | Error e) {
                runtime = null;
                lifecycleState = LifecycleState.NEW;
                throw e;
            }
        } finally {
            writeLock.unlock();
        }
    }

    private static RuntimeResources createRuntime(CacheConfig.L2Config config) {
        Config redissonConfig = new Config();
        redissonConfig.setLockWatchdogTimeout(toPositiveMillis(
                config.getLockWatchdogTimeout(), "Lock watchdog timeout"));
        ClusterServersConfig clusterConfig = redissonConfig.useClusterServers();
        if (config.getUsername() != null) {
            redissonConfig.setUsername(config.getUsername());
        }
        if (config.getPassword() != null) {
            redissonConfig.setPassword(config.getPassword());
        }
        clusterConfig.addNodeAddress(config.getHosts().stream()
                .map(RedissonL2Provider::normalizeAddress)
                .toArray(String[]::new));
        CacheConfig.Redisson redisson = Objects.requireNonNull(config.getRedisson(), "Redisson config cannot be null");
        if (redisson.getMasterConnectionMinimumIdleSize() != null) {
            clusterConfig.setMasterConnectionMinimumIdleSize(redisson.getMasterConnectionMinimumIdleSize());
        }
        if (redisson.getSlaveConnectionMinimumIdleSize() != null) {
            clusterConfig.setSlaveConnectionMinimumIdleSize(redisson.getSlaveConnectionMinimumIdleSize());
        }
        if (redisson.getMasterConnectionPoolSize() != null) {
            clusterConfig.setMasterConnectionPoolSize(redisson.getMasterConnectionPoolSize());
        }
        if (redisson.getSlaveConnectionPoolSize() != null) {
            clusterConfig.setSlaveConnectionPoolSize(redisson.getSlaveConnectionPoolSize());
        }
        if (config.getConnectionTimeout() != null) {
            clusterConfig.setConnectTimeout(Math.toIntExact(config.getConnectionTimeout().toMillis()));
        }
        if (config.getSocketTimeout() != null) {
            clusterConfig.setTimeout(Math.toIntExact(config.getSocketTimeout().toMillis()));
        }
        CacheConfig.Subscriber subscriber = Objects.requireNonNull(
                config.getSubscriber(), "Subscriber config cannot be null");
        PubSubMessageDispatcher messageProcessor = null;
        RedissonClient redissonClient = null;
        try {
            messageProcessor = new PubSubMessageDispatcher(subscriber, "redisson-pubsub-message");
            redissonClient = Redisson.create(redissonConfig);
            return new RuntimeResources(redissonClient, messageProcessor);
        } catch (RuntimeException | Error e) {
            shutdownRedissonClient(redissonClient);
            closeDispatcher(messageProcessor);
            throw e;
        }
    }

    @Override
    public String get(CacheKey key) {
        return withRuntime(resources ->
                (String) resources.redissonClient.getBucket(key.toKeyString(), StringCodec.INSTANCE).get());
    }

    @Override
    public void set(CacheKey key, String value, Duration ttl) {
        withRuntimeVoid(resources -> {
            long ttlMillis = requirePositiveTtlMillis(ttl);
            RBucket<String> bucket = resources.redissonClient.getBucket(key.toKeyString(), StringCodec.INSTANCE);
            bucket.set(value, Duration.ofMillis(ttlMillis));
        });
    }

    @Override
    public void delete(CacheKey key) {
        withRuntimeVoid(resources ->
                resources.redissonClient.getBucket(key.toKeyString(), StringCodec.INSTANCE).delete());
    }

    @Override
    public void publish(String channel, String message, L2PubSubMode mode) {
        Objects.requireNonNull(mode, "L2 Pub/Sub mode cannot be null");
        withRuntimeVoid(resources -> topic(resources, channel, mode).publish(message));
    }

    @Override
    public CacheMessageSubscription subscribe(
            String channel,
            CacheMessageListener listener,
            L2PubSubMode mode) {
        Objects.requireNonNull(channel, "Redis channel cannot be null");
        Objects.requireNonNull(listener, "Cache message listener cannot be null");
        Objects.requireNonNull(mode, "L2 Pub/Sub mode cannot be null");
        return withRuntime(resources -> subscribe(resources, channel, listener, mode));
    }

    private static CacheMessageSubscription subscribe(
            RuntimeResources resources,
            String channel,
            CacheMessageListener listener,
            L2PubSubMode mode) {
        RTopic topic = topic(resources, channel, mode);

        AtomicBoolean closing = new AtomicBoolean(false);
        AtomicBoolean subscribedOnce = new AtomicBoolean(false);
        AtomicBoolean deliveryInterrupted = new AtomicBoolean(false);
        int statusListenerId = topic.addListener(new StatusListener() {
            @Override
            public void onSubscribe(String subscribedChannel) {
                if (subscribedOnce.getAndSet(true)
                        && deliveryInterrupted.compareAndSet(true, false)) {
                    resources.messageProcessor.notifyDeliveryEvent(listener, new CacheMessageDeliveryEvent(
                            channel,
                            CacheMessageDeliveryEventType.SUBSCRIPTION_RESTORED,
                            0L,
                            null));
                }
            }

            @Override
            public void onUnsubscribe(String unsubscribedChannel) {
                if (!closing.get()
                        && subscribedOnce.get()
                        && deliveryInterrupted.compareAndSet(false, true)) {
                    resources.messageProcessor.notifyDeliveryEvent(listener, new CacheMessageDeliveryEvent(
                            channel,
                            CacheMessageDeliveryEventType.SUBSCRIPTION_INTERRUPTED,
                            0L,
                            new IllegalStateException("Redisson Pub/Sub subscription interrupted")));
                }
            }
        });
        int listenerId;
        try {
            listenerId = topic.addListener(String.class, (receivedChannel, payload) ->
                    resources.messageProcessor.dispatch(receivedChannel.toString(), payload, listener));
        } catch (RuntimeException e) {
            closing.set(true);
            topic.removeListener(statusListenerId);
            throw e;
        }

        LOGGER.info("Successfully subscribed to Redis channel [{}] via Redisson.", channel);

        return () -> {
            closing.set(true);
            try {
                topic.removeListener(listenerId, statusListenerId);
            } catch (Exception e) {
                LOGGER.debug("Error removing listener for channel [{}]", channel, e);
            }
        };
    }

    private static RTopic topic(RuntimeResources resources, String channel, L2PubSubMode mode) {
        if (mode == L2PubSubMode.SHARDED) {
            RShardedTopic topic = resources.redissonClient.getShardedTopic(channel, StringCodec.INSTANCE);
            return topic;
        }
        return resources.redissonClient.getTopic(channel, StringCodec.INSTANCE);
    }

    @Override
    public Object eval(String script, List<String> keys, List<String> args) {
        return withRuntime(resources -> {
            Object result = resources.redissonClient.getScript(StringCodec.INSTANCE).eval(
                    RScript.Mode.READ_WRITE,
                    script,
                    RScript.ReturnType.VALUE,
                    new java.util.ArrayList<Object>(keys),
                    args.toArray()
            );
            return normalizeEvalResult(result);
        });
    }

    @Override
    public L2ReentrantLock getLock(String name) {
        return withRuntime(resources -> new RedissonLockAdapter(
                resources.redissonClient.getLock(RedisL2ReentrantLock.validateLockName(name))));
    }

    @Override
    public void close() {
        ReentrantReadWriteLock.WriteLock writeLock = lifecycleLock.writeLock();
        writeLock.lock();
        try {
            if (lifecycleState == LifecycleState.CLOSED) {
                return;
            }
            RuntimeResources resources = runtime;
            runtime = null;
            lifecycleState = LifecycleState.CLOSING;
            try {
                closeRuntime(resources);
            } finally {
                lifecycleState = LifecycleState.CLOSED;
            }
        } finally {
            writeLock.unlock();
        }
    }

    private static void closeRuntime(RuntimeResources resources) {
        if (resources == null) {
            return;
        }
        // RedissonClient owns native RLock watchdog and topic resources.
        shutdownRedissonClient(resources.redissonClient);
        resources.messageProcessor.close();
    }

    private static void shutdownRedissonClient(RedissonClient redissonClient) {
        if (redissonClient == null || redissonClient.isShutdown()) {
            return;
        }
        try {
            redissonClient.shutdown();
        } catch (Exception e) {
            LOGGER.warn("Failed to shut down Redisson client", e);
        }
    }

    static String normalizeAddress(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Redis host cannot be blank");
        }
        String normalized = value.trim();
        if (normalized.startsWith("redis://") || normalized.startsWith("rediss://")) {
            validateUriAddress(normalized);
            return normalized;
        }
        Endpoint endpoint = Endpoint.parse(normalized);
        String formattedHost = endpoint.host.indexOf(':') >= 0
                ? "[" + endpoint.host + "]"
                : endpoint.host;
        return "redis://" + formattedHost + ":" + endpoint.port;
    }

    private static void validateUriAddress(String value) {
        try {
            java.net.URI uri = java.net.URI.create(value);
            if (uri.getHost() == null || uri.getHost().isBlank()) {
                throw new IllegalArgumentException("Redis URI must contain a valid host: " + value);
            }
            int port = uri.getPort();
            if (port != -1 && (port < 1 || port > 65535)) {
                throw new IllegalArgumentException("Redis port must be between 1 and 65535: " + value);
            }
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid Redis URI: " + value, e);
        }
    }

    private static Object normalizeEvalResult(Object result) {
        if (result instanceof Integer integer) {
            return integer.longValue();
        }
        if (result instanceof List<?> list) {
            return list.stream()
                    .map(RedissonL2Provider::normalizeEvalResult)
                    .toList();
        }
        return result;
    }

    private static void closeDispatcher(PubSubMessageDispatcher dispatcher) {
        if (dispatcher != null) {
            dispatcher.close();
        }
    }

    private <T> T withRuntime(Function<RuntimeResources, T> action) {
        ReentrantReadWriteLock.ReadLock readLock = lifecycleLock.readLock();
        readLock.lock();
        try {
            if (lifecycleState != LifecycleState.READY || runtime == null) {
                throw new IllegalStateException("Redisson L2 provider is not ready; state=" + lifecycleState);
            }
            return action.apply(runtime);
        } finally {
            readLock.unlock();
        }
    }

    private void withRuntimeVoid(Consumer<RuntimeResources> action) {
        withRuntime(resources -> {
            action.accept(resources);
            return null;
        });
    }

    private void requireNewState() {
        if (lifecycleState != LifecycleState.NEW) {
            throw new IllegalStateException("Redisson L2 provider cannot initialize from state " + lifecycleState);
        }
    }

    private static long requirePositiveTtlMillis(Duration ttl) {
        if (ttl == null) {
            throw new IllegalArgumentException("TTL cannot be null");
        }
        long ttlMillis;
        try {
            ttlMillis = ttl.toMillis();
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("TTL is too large to represent in milliseconds", e);
        }
        if (ttlMillis <= 0) {
            throw new IllegalArgumentException("TTL must resolve to at least one millisecond");
        }
        return ttlMillis;
    }

    private enum LifecycleState {
        NEW,
        INITIALIZING,
        READY,
        CLOSING,
        CLOSED
    }

    private record RuntimeResources(RedissonClient redissonClient, PubSubMessageDispatcher messageProcessor) {
    }

    static record Endpoint(String host, int port) {
        static Endpoint parse(String value) {
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException("Redis host cannot be blank");
            }
            String normalized = value.trim();
            if (normalized.startsWith("[")) {
                int closingBracket = normalized.indexOf(']');
                if (closingBracket < 0) {
                    throw new IllegalArgumentException("Invalid bracketed IPv6 Redis host: " + value);
                }
                String host = normalized.substring(1, closingBracket);
                String suffix = normalized.substring(closingBracket + 1);
                if (suffix.isEmpty()) {
                    return new Endpoint(requireHost(host, value), DEFAULT_PORT);
                }
                if (!suffix.startsWith(":") || suffix.length() == 1) {
                    throw new IllegalArgumentException("Invalid bracketed IPv6 Redis endpoint: " + value);
                }
                return new Endpoint(requireHost(host, value), parsePort(suffix.substring(1), value));
            }

            int firstColon = normalized.indexOf(':');
            if (firstColon < 0) {
                return new Endpoint(requireHost(normalized, value), DEFAULT_PORT);
            }
            if (firstColon != normalized.lastIndexOf(':')) {
                return new Endpoint(requireHost(normalized, value), DEFAULT_PORT);
            }
            return new Endpoint(
                    requireHost(normalized.substring(0, firstColon), value),
                    parsePort(normalized.substring(firstColon + 1), value));
        }

        private static String requireHost(String host, String original) {
            if (host.isBlank()) {
                throw new IllegalArgumentException("Redis host cannot be blank: " + original);
            }
            return host;
        }

        private static int parsePort(String value, String original) {
            try {
                int port = Integer.parseInt(value);
                if (port < 1 || port > 65535) {
                    throw new IllegalArgumentException("Redis port must be between 1 and 65535: " + original);
                }
                return port;
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid Redis port: " + original, e);
            }
        }
    }

    private final class RedissonLockAdapter implements L2ReentrantLock {
        private final RLock lock;

        private RedissonLockAdapter(RLock lock) {
            this.lock = lock;
        }

        @Override
        public void lock() {
            withRuntimeVoid(resources -> lock.lock());
        }

        @Override
        public void lock(Duration leaseTime) {
            withRuntimeVoid(resources -> lock.lock(
                    validatePositive(leaseTime, "Lock lease time").toMillis(), TimeUnit.MILLISECONDS));
        }

        @Override
        public boolean tryLock() {
            return withRuntime(resources -> lock.tryLock());
        }

        @Override
        public boolean tryLock(Duration waitTime) throws InterruptedException {
            validateWaitTime(waitTime);
            try {
                return withRuntime(resources -> {
                    try {
                        return lock.tryLock(waitTime.toMillis(), TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        throw new InterruptedLockOperationException(e);
                    }
                });
            } catch (InterruptedLockOperationException e) {
                throw e.interruptedException;
            }
        }

        @Override
        public boolean tryLock(Duration waitTime, Duration leaseTime) throws InterruptedException {
            validateWaitTime(waitTime);
            long leaseMillis = validatePositive(leaseTime, "Lock lease time").toMillis();
            try {
                return withRuntime(resources -> {
                    try {
                        return lock.tryLock(
                                waitTime.toMillis(), leaseMillis, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        throw new InterruptedLockOperationException(e);
                    }
                });
            } catch (InterruptedLockOperationException e) {
                throw e.interruptedException;
            }
        }

        @Override
        public void unlock() {
            withRuntimeVoid(resources -> lock.unlock());
        }

        @Override
        public boolean forceUnlock() {
            return withRuntime(resources -> lock.forceUnlock());
        }

        @Override
        public boolean isLocked() {
            return withRuntime(resources -> lock.isLocked());
        }

        @Override
        public boolean isHeldByCurrentThread() {
            return withRuntime(resources -> lock.isHeldByCurrentThread());
        }

        @Override
        public int getHoldCount() {
            return withRuntime(resources -> lock.getHoldCount());
        }

        @Override
        public Duration remainTimeToLive() {
            return withRuntime(resources -> Duration.ofMillis(lock.remainTimeToLive()));
        }

        private static Duration validatePositive(Duration duration, String label) {
            Objects.requireNonNull(duration, label + " cannot be null");
            if (duration.toMillis() <= 0) {
                throw new IllegalArgumentException(label + " must be positive");
            }
            return duration;
        }

        private static void validateWaitTime(Duration waitTime) {
            Objects.requireNonNull(waitTime, "Lock wait time cannot be null");
            if (waitTime.isNegative()) {
                throw new IllegalArgumentException("Lock wait time cannot be negative");
            }
        }
    }

    private static final class InterruptedLockOperationException extends RuntimeException {
        private final InterruptedException interruptedException;

        private InterruptedLockOperationException(InterruptedException interruptedException) {
            super(interruptedException);
            this.interruptedException = interruptedException;
        }
    }

    private static int toPositiveMillis(Duration duration, String label) {
        Objects.requireNonNull(duration, label + " cannot be null");
        long millis = duration.toMillis();
        if (millis <= 0) {
            throw new IllegalArgumentException(label + " must be positive");
        }
        return Math.toIntExact(millis);
    }
}
