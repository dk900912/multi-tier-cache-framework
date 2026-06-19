package io.github.dk900912.multitiercache.provider.jedis;

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
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Connection;
import redis.clients.jedis.ConnectionPoolConfig;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.JedisShardedPubSub;
import redis.clients.jedis.RedisClusterClient;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.params.SetParams;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Level 2 (L2) cache provider implementation using Jedis.
 *
 * @author dukui
 */
public class JedisL2Provider implements L2Provider, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(JedisL2Provider.class);

    private static final int DEFAULT_PORT = 6379;

    private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock(true);
    private final String lockClientId = UUID.randomUUID().toString();
    private LifecycleState lifecycleState = LifecycleState.NEW;
    private RuntimeResources runtime;

    public JedisL2Provider() {
    }

    @Override
    public CacheConfig.L2ProviderType providerType() {
        return CacheConfig.L2ProviderType.JEDIS;
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
        Set<HostAndPort> nodes = parseNodes(Objects.requireNonNull(config.getHosts(), "Redis hosts cannot be null"));
        DefaultJedisClientConfig.Builder clientConfigBuilder = DefaultJedisClientConfig.builder()
                .connectionTimeoutMillis(toMillis(config.getConnectionTimeout()))
                .socketTimeoutMillis(toMillis(config.getSocketTimeout()));
        if (config.getUsername() != null) {
            clientConfigBuilder.user(config.getUsername());
        }
        if (config.getPassword() != null) {
            clientConfigBuilder.password(config.getPassword());
        }

        GenericObjectPoolConfig<Connection> poolConfig = getObjectPoolConfig(config);
        Duration watchdogTimeout = Objects.requireNonNull(
                config.getLockWatchdogTimeout(), "Lock watchdog timeout cannot be null");
        Duration readyTimeout = Objects.requireNonNull(
                config.getSocketTimeout(), "Redis socket timeout cannot be null");
        CacheConfig.Subscriber subscriber = Objects.requireNonNull(
                config.getSubscriber(), "Subscriber config cannot be null");

        ThreadPoolExecutor pubSubExecutor = null;
        PubSubMessageDispatcher messageProcessor = null;
        RedisClusterClient redisClient = null;
        RedisL2ReentrantLock.RenewalRegistry renewalRegistry = null;
        try {
            pubSubExecutor = newPubSubExecutor(subscriber);
            messageProcessor = new PubSubMessageDispatcher(subscriber, "jedis-pubsub-message");
            redisClient = RedisClusterClient.builder()
                    .nodes(nodes)
                    .clientConfig(clientConfigBuilder.build())
                    .poolConfig(poolConfig)
                    .maxAttempts(config.getMaxRedirects() == null
                            ? RedisClusterClient.DEFAULT_MAX_ATTEMPTS
                            : config.getMaxRedirects())
                    .build();
            renewalRegistry = new RedisL2ReentrantLock.RenewalRegistry();
            return new RuntimeResources(
                    redisClient, pubSubExecutor, messageProcessor, watchdogTimeout, readyTimeout, renewalRegistry);
        } catch (RuntimeException | Error e) {
            if (renewalRegistry != null) {
                renewalRegistry.close();
            }
            closeRedisClient(redisClient);
            closeDispatcher(messageProcessor);
            shutdownExecutor(pubSubExecutor, "Jedis pub/sub executor");
            throw e;
        }
    }

    static ThreadPoolExecutor newPubSubExecutor(CacheConfig.Subscriber subscriber) {
        Objects.requireNonNull(subscriber, "Subscriber config cannot be null");
        int corePoolSize = subscriber.getCorePoolSize();
        int maximumPoolSize = subscriber.getMaximumPoolSize();
        long keepAliveTime = subscriber.getKeepAliveTime().toMillis();
        // Jedis subscribe/ssubscribe is blocking; never let pool saturation run it on the caller thread.
        return new ThreadPoolExecutor(
                corePoolSize, maximumPoolSize,
                keepAliveTime, TimeUnit.MILLISECONDS,
                new SynchronousQueue<>(),
                Thread.ofPlatform().daemon().name("jedis-pubsub-worker-", 1).factory(),
                new ThreadPoolExecutor.AbortPolicy()
        );
    }

    private static GenericObjectPoolConfig<Connection> getObjectPoolConfig(CacheConfig.L2Config config) {
        GenericObjectPoolConfig<Connection> poolConfig = new ConnectionPoolConfig();
        CacheConfig.Jedis jedis = Objects.requireNonNull(config.getJedis(), "Jedis config cannot be null");
        if (jedis.getMaxTotal() != null) {
            poolConfig.setMaxTotal(jedis.getMaxTotal());
        }
        if (jedis.getMaxIdle() != null) {
            poolConfig.setMaxIdle(jedis.getMaxIdle());
        }
        if (jedis.getMinIdle() != null) {
            poolConfig.setMinIdle(jedis.getMinIdle());
        }
        if (jedis.getMaxWait() != null) {
            poolConfig.setMaxWait(jedis.getMaxWait());
        }
        return poolConfig;
    }

    @Override
    public String get(CacheKey key) {
        return withRuntime(resources -> resources.redisClient.get(key.toKeyString()));
    }

    @Override
    public void set(CacheKey key, String value, Duration ttl) {
        withRuntimeVoid(resources -> {
            long ttlMillis = requirePositiveTtlMillis(ttl);
            resources.redisClient.set(key.toKeyString(), value, SetParams.setParams().px(ttlMillis));
        });
    }

    @Override
    public void delete(CacheKey key) {
        withRuntimeVoid(resources -> resources.redisClient.del(key.toKeyString()));
    }

    @Override
    public void publish(String channel, String message, L2PubSubMode mode) {
        Objects.requireNonNull(mode, "L2 Pub/Sub mode cannot be null");
        withRuntimeVoid(resources -> {
            if (mode == L2PubSubMode.SHARDED) {
                resources.redisClient.spublish(channel, message);
            } else {
                resources.redisClient.publish(channel, message);
            }
        });
    }

    @Override
    public CacheMessageSubscription subscribe(
            String channel,
            CacheMessageListener listener,
            L2PubSubMode mode) {
        Objects.requireNonNull(channel, "Redis channel cannot be null");
        Objects.requireNonNull(listener, "Cache message listener cannot be null");
        Objects.requireNonNull(mode, "L2 Pub/Sub mode cannot be null");
        return withRuntime(resources -> mode == L2PubSubMode.SHARDED
                ? subscribeSharded(resources, channel, listener)
                : subscribeStandard(resources, channel, listener));
    }

    private static CacheMessageSubscription subscribeStandard(
            RuntimeResources resources,
            String channel,
            CacheMessageListener listener) {
        AtomicBoolean closing = new AtomicBoolean(false);
        AtomicBoolean subscribedOnce = new AtomicBoolean(false);
        AtomicBoolean deliveryInterrupted = new AtomicBoolean(false);
        CountDownLatch subscribed = new CountDownLatch(1);
        JedisPubSub pubSub = new JedisPubSub() {
            @Override
            public void onSubscribe(String receivedChannel, int subscribedChannels) {
                if (channel.equals(receivedChannel)) {
                    subscribed.countDown();
                    if (subscribedOnce.getAndSet(true)
                            && deliveryInterrupted.compareAndSet(true, false)) {
                        notifyDeliveryEvent(resources.messageProcessor, listener, channel,
                                CacheMessageDeliveryEventType.SUBSCRIPTION_RESTORED, null);
                    }
                }
            }

            @Override
            public void onMessage(String receivedChannel, String payload) {
                dispatchMessage(
                        resources.messageProcessor, receivedChannel, payload, listener, "cache message");
            }
        };

        resources.activePubSubs.add(pubSub);

        try {
            resources.pubSubExecutor.execute(() -> {
                int retryDelay = 1;
                while (!closing.get() && !Thread.currentThread().isInterrupted()) {
                    try {
                        resources.redisClient.subscribe(pubSub, channel);
                        break;
                    } catch (JedisConnectionException e) {
                        if (!closing.get()) {
                            markSubscriptionInterrupted(resources.messageProcessor, listener, channel,
                                    subscribedOnce, deliveryInterrupted, e);
                            LOGGER.warn("Redis Pub/Sub connection lost for channel [{}]. Reconnecting in {} seconds...", channel, retryDelay, e);
                            sleepSilently(retryDelay);
                            retryDelay = Math.min(retryDelay * 2, 10);
                        }
                    } catch (Exception e) {
                        if (!closing.get()) {
                            markSubscriptionInterrupted(resources.messageProcessor, listener, channel,
                                    subscribedOnce, deliveryInterrupted, e);
                            LOGGER.error("Unexpected error in Pub/Sub loop for channel [{}]. Retrying in 5 seconds...", channel, e);
                            sleepSilently(5);
                        }
                    }
                }
            });
        } catch (RejectedExecutionException e) {
            closing.set(true);
            resources.activePubSubs.remove(pubSub);
            throw new IllegalStateException("Redis Pub/Sub executor rejected subscription for channel: " + channel, e);
        }

        CacheMessageSubscription subscription = () -> {
            closing.set(true);
            resources.activePubSubs.remove(pubSub);
            try {
                if (pubSub.isSubscribed()) {
                    pubSub.unsubscribe();
                }
            } catch (Exception e) {
                LOGGER.debug("Exception swallowed while unsubscribing from channel: {}", channel, e);
            }
        };

        try {
            if (!subscribed.await(resources.subscriptionReadyTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
                subscription.close();
                throw new IllegalStateException("Timed out while subscribing to Redis channel: " + channel);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            subscription.close();
            throw new IllegalStateException("Interrupted while subscribing to Redis channel: " + channel, e);
        }

        return subscription;
    }

    private static CacheMessageSubscription subscribeSharded(
            RuntimeResources resources,
            String channel,
            CacheMessageListener listener) {
        AtomicBoolean closing = new AtomicBoolean(false);
        AtomicBoolean subscribedOnce = new AtomicBoolean(false);
        AtomicBoolean deliveryInterrupted = new AtomicBoolean(false);
        CountDownLatch subscribed = new CountDownLatch(1);
        JedisShardedPubSub pubSub = new JedisShardedPubSub() {
            @Override
            public void onSSubscribe(String receivedChannel, int subscribedChannels) {
                if (channel.equals(receivedChannel)) {
                    subscribed.countDown();
                    if (subscribedOnce.getAndSet(true)
                            && deliveryInterrupted.compareAndSet(true, false)) {
                        notifyDeliveryEvent(resources.messageProcessor, listener, channel,
                                CacheMessageDeliveryEventType.SUBSCRIPTION_RESTORED, null);
                    }
                }
            }

            @Override
            public void onSMessage(String receivedChannel, String payload) {
                dispatchMessage(
                        resources.messageProcessor, receivedChannel, payload, listener, "cache sharded message");
            }
        };

        resources.activeShardedPubSubs.add(pubSub);

        try {
            resources.pubSubExecutor.execute(() -> {
                int retryDelay = 1;
                while (!closing.get() && !Thread.currentThread().isInterrupted()) {
                    try {
                        resources.redisClient.ssubscribe(pubSub, channel);
                        break;
                    } catch (JedisConnectionException e) {
                        if (!closing.get()) {
                            markSubscriptionInterrupted(resources.messageProcessor, listener, channel,
                                    subscribedOnce, deliveryInterrupted, e);
                            LOGGER.warn("Redis sharded Pub/Sub connection lost for channel [{}]. Reconnecting in {} seconds...", channel, retryDelay, e);
                            sleepSilently(retryDelay);
                            retryDelay = Math.min(retryDelay * 2, 10);
                        }
                    } catch (Exception e) {
                        if (!closing.get()) {
                            markSubscriptionInterrupted(resources.messageProcessor, listener, channel,
                                    subscribedOnce, deliveryInterrupted, e);
                            LOGGER.error("Unexpected error in sharded Pub/Sub loop for channel [{}]. Retrying in 5 seconds...", channel, e);
                            sleepSilently(5);
                        }
                    }
                }
            });
        } catch (RejectedExecutionException e) {
            closing.set(true);
            resources.activeShardedPubSubs.remove(pubSub);
            throw new IllegalStateException("Redis Pub/Sub executor rejected sharded subscription for channel: " + channel, e);
        }

        CacheMessageSubscription subscription = () -> {
            closing.set(true);
            resources.activeShardedPubSubs.remove(pubSub);
            try {
                if (pubSub.isSubscribed()) {
                    pubSub.sunsubscribe();
                }
            } catch (Exception e) {
                LOGGER.debug("Exception swallowed while unsubscribing from sharded channel: {}", channel, e);
            }
        };

        try {
            if (!subscribed.await(resources.subscriptionReadyTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
                subscription.close();
                throw new IllegalStateException("Timed out while subscribing to Redis sharded channel: " + channel);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            subscription.close();
            throw new IllegalStateException("Interrupted while subscribing to Redis sharded channel: " + channel, e);
        }

        return subscription;
    }

    @Override
    public Object eval(String script, List<String> keys, List<String> args) {
        return withRuntime(resources -> resources.redisClient.eval(script, keys, args));
    }

    @Override
    public L2ReentrantLock getLock(String name) {
        return withRuntime(resources -> new RedisL2ReentrantLock(
                this,
                name,
                lockClientId,
                resources.lockWatchdogTimeout,
                resources.lockRenewalRegistry,
                "SPUBLISH"));
    }

    private static void sleepSilently(int seconds) {
        try {
            TimeUnit.SECONDS.sleep(seconds);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
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
        for (JedisPubSub pubSub : resources.activePubSubs) {
            try {
                if (pubSub.isSubscribed()) {
                    pubSub.unsubscribe();
                }
            } catch (Exception e) {
                LOGGER.trace("Ignored exception during graceful shutdown unsubscribe", e);
            }
        }
        resources.activePubSubs.clear();
        for (JedisShardedPubSub pubSub : resources.activeShardedPubSubs) {
            try {
                if (pubSub.isSubscribed()) {
                    pubSub.sunsubscribe();
                }
            } catch (Exception e) {
                LOGGER.trace("Ignored exception during graceful shutdown sharded unsubscribe", e);
            }
        }
        resources.activeShardedPubSubs.clear();
        resources.lockRenewalRegistry.close();

        shutdownExecutor(resources.pubSubExecutor, "Jedis pub/sub executor");
        resources.messageProcessor.close();
        closeRedisClient(resources.redisClient);
    }

    private static void closeRedisClient(RedisClusterClient redisClient) {
        if (redisClient != null) {
            try {
                redisClient.close();
            } catch (Exception e) {
                LOGGER.warn("Failed to close Jedis redis client", e);
            }
        }
    }

    private static Set<HostAndPort> parseNodes(List<String> hosts) {
        Set<HostAndPort> nodes = new HashSet<>();
        for (String host : hosts) {
            Endpoint endpoint = Endpoint.parse(host);
            nodes.add(new HostAndPort(endpoint.host, endpoint.port));
        }
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("Redis hosts cannot be empty");
        }
        return nodes;
    }

    private static int toMillis(Duration duration) {
        return Math.toIntExact(Objects.requireNonNull(duration, "Redis timeout cannot be null").toMillis());
    }

    private static void dispatchMessage(
            PubSubMessageDispatcher messageProcessor,
            String channel,
            String payload,
            CacheMessageListener listener,
            String description) {
        messageProcessor.dispatch(channel, payload, listener);
    }

    private static void markSubscriptionInterrupted(
            PubSubMessageDispatcher dispatcher,
            CacheMessageListener listener,
            String channel,
            AtomicBoolean subscribedOnce,
            AtomicBoolean deliveryInterrupted,
            Throwable cause) {
        if (subscribedOnce.get() && deliveryInterrupted.compareAndSet(false, true)) {
            notifyDeliveryEvent(dispatcher, listener, channel,
                    CacheMessageDeliveryEventType.SUBSCRIPTION_INTERRUPTED, cause);
        }
    }

    private static void notifyDeliveryEvent(
            PubSubMessageDispatcher dispatcher,
            CacheMessageListener listener,
            String channel,
            CacheMessageDeliveryEventType type,
            Throwable cause) {
        dispatcher.notifyDeliveryEvent(listener, new CacheMessageDeliveryEvent(channel, type, 0L, cause));
    }

    private static void closeDispatcher(PubSubMessageDispatcher dispatcher) {
        if (dispatcher != null) {
            dispatcher.close();
        }
    }

    private static void shutdownExecutor(ExecutorService executor, String name) {
        if (executor == null || executor.isShutdown()) {
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(3, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                if (!executor.awaitTermination(3, TimeUnit.SECONDS)) {
                    LOGGER.debug("{} did not terminate after shutdownNow", name);
                }
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOGGER.warn("Failed to shut down {}", name, e);
        }
    }

    private <T> T withRuntime(Function<RuntimeResources, T> action) {
        ReentrantReadWriteLock.ReadLock readLock = lifecycleLock.readLock();
        readLock.lock();
        try {
            if (lifecycleState != LifecycleState.READY || runtime == null) {
                throw new IllegalStateException("Jedis L2 provider is not ready; state=" + lifecycleState);
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
            throw new IllegalStateException("Jedis L2 provider cannot initialize from state " + lifecycleState);
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

    private static final class RuntimeResources {
        private final RedisClusterClient redisClient;
        private final ExecutorService pubSubExecutor;
        private final PubSubMessageDispatcher messageProcessor;
        private final Duration lockWatchdogTimeout;
        private final Duration subscriptionReadyTimeout;
        private final RedisL2ReentrantLock.RenewalRegistry lockRenewalRegistry;
        private final Set<JedisPubSub> activePubSubs = ConcurrentHashMap.newKeySet();
        private final Set<JedisShardedPubSub> activeShardedPubSubs = ConcurrentHashMap.newKeySet();

        private RuntimeResources(
                RedisClusterClient redisClient,
                ExecutorService pubSubExecutor,
                PubSubMessageDispatcher messageProcessor,
                Duration lockWatchdogTimeout,
                Duration subscriptionReadyTimeout,
                RedisL2ReentrantLock.RenewalRegistry lockRenewalRegistry) {
            this.redisClient = redisClient;
            this.pubSubExecutor = pubSubExecutor;
            this.messageProcessor = messageProcessor;
            this.lockWatchdogTimeout = lockWatchdogTimeout;
            this.subscriptionReadyTimeout = subscriptionReadyTimeout;
            this.lockRenewalRegistry = lockRenewalRegistry;
        }
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
}
