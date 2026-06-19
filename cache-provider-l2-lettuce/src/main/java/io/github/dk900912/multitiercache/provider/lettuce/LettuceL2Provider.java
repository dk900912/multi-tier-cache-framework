package io.github.dk900912.multitiercache.provider.lettuce;

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
import io.lettuce.core.RedisChannelHandler;
import io.lettuce.core.RedisConnectionStateListener;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.pubsub.RedisPubSubAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Level 2 (L2) cache provider implementation using Lettuce.
 *
 * @author dukui
 */
public class LettuceL2Provider implements L2Provider, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(LettuceL2Provider.class);
    private static final int DEFAULT_PORT = 6379;
    private static final String EVAL_RESULT_NIL = "__mtcf_nil";
    private static final String EVAL_RESULT_SCALAR = "__mtcf_scalar";
    private static final String EVAL_RESULT_TABLE = "__mtcf_table";
    private static final String EVAL_WRAPPER_PREFIX = "local __mtcf_eval_result = (function()\n";
    private static final String EVAL_WRAPPER_SUFFIX = """
            end)()
            if __mtcf_eval_result == nil or __mtcf_eval_result == false then
                return {'__mtcf_nil'}
            end
            if type(__mtcf_eval_result) == 'table' then
                return {'__mtcf_table', __mtcf_eval_result}
            end
            return {'__mtcf_scalar', __mtcf_eval_result}""";

    private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock(true);
    private final String lockClientId = UUID.randomUUID().toString();
    private LifecycleState lifecycleState = LifecycleState.NEW;
    private RuntimeResources runtime;

    public LettuceL2Provider() {
    }

    @Override
    public CacheConfig.L2ProviderType providerType() {
        return CacheConfig.L2ProviderType.LETTUCE;
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
        List<RedisURI> redisURIs = parseRedisUris(config);
        CacheConfig.Subscriber subscriber = Objects.requireNonNull(
                config.getSubscriber(), "Subscriber config cannot be null");
        Duration watchdogTimeout = Objects.requireNonNull(
                config.getLockWatchdogTimeout(), "Lock watchdog timeout cannot be null");

        PubSubMessageDispatcher messageProcessor = null;
        RedisClusterClient clusterClient = null;
        StatefulRedisClusterConnection<String, String> connection = null;
        RedisL2ReentrantLock.RenewalRegistry renewalRegistry = null;
        try {
            messageProcessor = new PubSubMessageDispatcher(subscriber, "lettuce-pubsub-message");
            clusterClient = RedisClusterClient.create(redisURIs);
            ClusterTopologyRefreshOptions topologyRefreshOptions = ClusterTopologyRefreshOptions.builder()
                    .enablePeriodicRefresh(Duration.ofSeconds(15))
                    .closeStaleConnections(true)
                    .build();
            ClusterClientOptions options = ClusterClientOptions.builder()
                    .maxRedirects(config.getMaxRedirects() == null
                            ? ClusterClientOptions.DEFAULT_MAX_REDIRECTS
                            : config.getMaxRedirects())
                    .socketOptions(SocketOptions.builder()
                            .connectTimeout(config.getConnectionTimeout())
                            .build())
                    .topologyRefreshOptions(topologyRefreshOptions)
                    .build();
            clusterClient.setOptions(options);
            connection = clusterClient.connect();
            renewalRegistry = new RedisL2ReentrantLock.RenewalRegistry();
            return new RuntimeResources(
                    clusterClient, connection, messageProcessor, watchdogTimeout, renewalRegistry);
        } catch (RuntimeException | Error e) {
            if (renewalRegistry != null) {
                renewalRegistry.close();
            }
            closeConnection(connection, "Lettuce Redis connection");
            shutdownClusterClient(clusterClient);
            closeDispatcher(messageProcessor);
            throw e;
        }
    }

    @Override
    public String get(CacheKey key) {
        return withRuntime(resources -> resources.connection.sync().get(key.toKeyString()));
    }

    @Override
    public void set(CacheKey key, String value, Duration ttl) {
        withRuntimeVoid(resources -> {
            long ttlMillis = requirePositiveTtlMillis(ttl);
            resources.connection.sync().set(key.toKeyString(), value, new SetArgs().px(ttlMillis));
        });
    }

    @Override
    public void delete(CacheKey key) {
        withRuntimeVoid(resources -> resources.connection.sync().del(key.toKeyString()));
    }

    @Override
    public void publish(String channel, String message, L2PubSubMode mode) {
        Objects.requireNonNull(mode, "L2 Pub/Sub mode cannot be null");
        withRuntimeVoid(resources -> {
            if (mode == L2PubSubMode.SHARDED) {
                resources.connection.sync().spublish(channel, message);
            } else {
                resources.connection.sync().publish(channel, message);
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
        return withRuntime(resources -> subscribe(resources, channel, listener, mode));
    }

    private static CacheMessageSubscription subscribe(
            RuntimeResources resources,
            String channel,
            CacheMessageListener listener,
            L2PubSubMode mode) {
        boolean sharded = mode == L2PubSubMode.SHARDED;
        ConcurrentMap<String, Set<CacheMessageListener>> listenersByChannel =
                sharded ? resources.shardedChannelListeners : resources.channelListeners;

        synchronized (resources.pubSubLock) {
            Set<CacheMessageListener> listeners = listenersByChannel.computeIfAbsent(
                    channel, ignored -> ConcurrentHashMap.newKeySet());
            boolean firstListener = listeners.isEmpty();
            listeners.add(listener);
            try {
                PubSubConnectionState pubSubState = getOrCreatePubSubConnection(resources);
                if (!pubSubState.created() && firstListener) {
                    subscribeOnRedis(resources, channel, sharded);
                }
            } catch (RuntimeException e) {
                listeners.remove(listener);
                if (listeners.isEmpty()) {
                    listenersByChannel.remove(channel, listeners);
                }
                throw e;
            }
        }

        LOGGER.info("Successfully subscribed to Redis channel [{}] via Lettuce.", channel);

        return () -> {
            try {
                unsubscribe(resources, channel, listener, sharded);
            } catch (Exception e) {
                LOGGER.debug("Error unsubscribing from channel [{}]", channel, e);
            }
        };
    }

    @Override
    public Object eval(String script, List<String> keys, List<String> args) {
        return withRuntime(resources -> {
            Object result = resources.connection.sync().eval(
                    wrapLuaScript(script),
                    ScriptOutputType.MULTI,
                    keys.toArray(new String[0]),
                    args.toArray(new String[0]));
            return unwrapLettuceEvalResult(result);
        });
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
        resources.closing.set(true);
        resources.lockRenewalRegistry.close();
        closePubSubConnection(resources);
        resources.messageProcessor.close();
        closeConnection(resources.connection, "Lettuce Redis connection");
        shutdownClusterClient(resources.clusterClient);
    }

    private static void closeConnection(AutoCloseable connection, String name) {
        if (connection == null) {
            return;
        }
        try {
            connection.close();
        } catch (Exception e) {
            LOGGER.warn("Failed to close {}", name, e);
        }
    }

    private static void shutdownClusterClient(RedisClusterClient clusterClient) {
        if (clusterClient == null) {
            return;
        }
        try {
            clusterClient.shutdown();
        } catch (Exception e) {
            LOGGER.warn("Failed to shut down Lettuce cluster client", e);
        }
    }

    private static List<RedisURI> parseRedisUris(CacheConfig.L2Config config) {
        List<RedisURI> redisURIs = new ArrayList<>();
        List<String> hosts = Objects.requireNonNull(config.getHosts(), "Redis hosts cannot be null");
        for (String host : hosts) {
            Endpoint endpoint = Endpoint.parse(host);
            RedisURI.Builder builder = RedisURI.Builder.redis(endpoint.host, endpoint.port)
                    .withTimeout(config.getSocketTimeout());
            if (config.getUsername() != null && config.getPassword() != null) {
                builder.withAuthentication(config.getUsername(), config.getPassword());
            } else if (config.getPassword() != null) {
                builder.withPassword(config.getPassword());
            }
            redisURIs.add(builder.build());
        }
        if (redisURIs.isEmpty()) {
            throw new IllegalArgumentException("Redis hosts cannot be empty");
        }
        return redisURIs;
    }

    private static PubSubConnectionState getOrCreatePubSubConnection(RuntimeResources resources) {
        if (resources.pubSubConnection != null && resources.pubSubConnection.isOpen()) {
            return new PubSubConnectionState(false);
        }
        closePubSubConnectionWithoutLock(resources);
        try {
            resources.pubSubConnection = resources.clusterClient.connectPubSub();
            resources.pubSubConnection.addListener(resources.pubSubAdapter);
            resources.pubSubConnection.addListener(resources.connectionStateListener);
            resubscribeAll(resources);
            return new PubSubConnectionState(true);
        } catch (RuntimeException e) {
            closePubSubConnectionWithoutLock(resources);
            throw e;
        }
    }

    private static void resubscribeAll(RuntimeResources resources) {
        for (String subscribedChannel : resources.channelListeners.keySet()) {
            subscribeOnRedis(resources, subscribedChannel, false);
        }
        for (String subscribedChannel : resources.shardedChannelListeners.keySet()) {
            subscribeOnRedis(resources, subscribedChannel, true);
        }
    }

    private static void subscribeOnRedis(RuntimeResources resources, String channel, boolean sharded) {
        if (sharded) {
            resources.pubSubConnection.sync().ssubscribe(channel);
        } else {
            resources.pubSubConnection.sync().subscribe(channel);
        }
    }

    private static void unsubscribe(
            RuntimeResources resources,
            String channel,
            CacheMessageListener listener,
            boolean sharded) {
        ConcurrentMap<String, Set<CacheMessageListener>> listenersByChannel =
                sharded ? resources.shardedChannelListeners : resources.channelListeners;
        synchronized (resources.pubSubLock) {
            Set<CacheMessageListener> listeners = listenersByChannel.get(channel);
            if (listeners == null) {
                return;
            }
            listeners.remove(listener);
            if (!listeners.isEmpty()) {
                return;
            }
            listenersByChannel.remove(channel, listeners);
            if (resources.pubSubConnection == null || !resources.pubSubConnection.isOpen()) {
                return;
            }
            if (sharded) {
                resources.pubSubConnection.sync().sunsubscribe(channel);
            } else {
                resources.pubSubConnection.sync().unsubscribe(channel);
            }
        }
    }

    private static void closePubSubConnection(RuntimeResources resources) {
        synchronized (resources.pubSubLock) {
            closePubSubConnectionWithoutLock(resources);
            resources.channelListeners.clear();
            resources.shardedChannelListeners.clear();
        }
    }

    private static void closePubSubConnectionWithoutLock(RuntimeResources resources) {
        if (resources.pubSubConnection == null) {
            return;
        }
        try {
            resources.pubSubConnection.removeListener(resources.pubSubAdapter);
            resources.pubSubConnection.removeListener(resources.connectionStateListener);
            resources.pubSubConnection.close();
        } catch (Exception e) {
            LOGGER.debug("Error closing Lettuce Pub/Sub connection", e);
        } finally {
            resources.pubSubConnection = null;
        }
    }

    private static void dispatchMessage(
            PubSubMessageDispatcher messageProcessor,
            String channel,
            String payload,
            ConcurrentMap<String, Set<CacheMessageListener>> listenersByChannel) {
        Set<CacheMessageListener> listeners = listenersByChannel.get(channel);
        if (listeners == null || listeners.isEmpty()) {
            return;
        }
        for (CacheMessageListener listener : listeners) {
            messageProcessor.dispatch(channel, payload, listener);
        }
    }

    private static void closeDispatcher(PubSubMessageDispatcher dispatcher) {
        if (dispatcher != null) {
            dispatcher.close();
        }
    }

    private static String wrapLuaScript(String script) {
        return EVAL_WRAPPER_PREFIX
                + Objects.requireNonNull(script, "Lua script cannot be null")
                + "\n"
                + EVAL_WRAPPER_SUFFIX;
    }

    private static Object unwrapLettuceEvalResult(Object result) {
        if (!(result instanceof List<?> list) || list.isEmpty()) {
            return result;
        }
        Object marker = list.get(0);
        if (EVAL_RESULT_NIL.equals(marker)) {
            return null;
        }
        if (EVAL_RESULT_SCALAR.equals(marker)) {
            return list.size() > 1 ? list.get(1) : null;
        }
        if (EVAL_RESULT_TABLE.equals(marker)) {
            return list.size() > 1 ? list.get(1) : List.of();
        }
        return result;
    }

    private <T> T withRuntime(Function<RuntimeResources, T> action) {
        ReentrantReadWriteLock.ReadLock readLock = lifecycleLock.readLock();
        readLock.lock();
        try {
            if (lifecycleState != LifecycleState.READY || runtime == null) {
                throw new IllegalStateException("Lettuce L2 provider is not ready; state=" + lifecycleState);
            }
            if (!runtime.connection.isOpen()) {
                throw new IllegalStateException("Lettuce Redis connection is not open");
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
            throw new IllegalStateException("Lettuce L2 provider cannot initialize from state " + lifecycleState);
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
        private final RedisClusterClient clusterClient;
        private final StatefulRedisClusterConnection<String, String> connection;
        private final PubSubMessageDispatcher messageProcessor;
        private final Duration lockWatchdogTimeout;
        private final RedisL2ReentrantLock.RenewalRegistry lockRenewalRegistry;
        private final Object pubSubLock = new Object();
        private final ConcurrentMap<String, Set<CacheMessageListener>> channelListeners = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, Set<CacheMessageListener>> shardedChannelListeners = new ConcurrentHashMap<>();
        private final RedisPubSubAdapter<String, String> pubSubAdapter;
        private final RedisConnectionStateListener connectionStateListener;
        private final Set<ChannelSubscription> interruptedSubscriptions = ConcurrentHashMap.newKeySet();
        private final AtomicBoolean closing = new AtomicBoolean(false);
        private StatefulRedisClusterPubSubConnection<String, String> pubSubConnection;

        private RuntimeResources(
                RedisClusterClient clusterClient,
                StatefulRedisClusterConnection<String, String> connection,
                PubSubMessageDispatcher messageProcessor,
                Duration lockWatchdogTimeout,
                RedisL2ReentrantLock.RenewalRegistry lockRenewalRegistry) {
            this.clusterClient = clusterClient;
            this.connection = connection;
            this.messageProcessor = messageProcessor;
            this.lockWatchdogTimeout = lockWatchdogTimeout;
            this.lockRenewalRegistry = lockRenewalRegistry;
            this.pubSubAdapter = new RedisPubSubAdapter<>() {
                @Override
                public void message(String receivedChannel, String payload) {
                    dispatchMessage(
                            RuntimeResources.this.messageProcessor,
                            receivedChannel,
                            payload,
                            channelListeners);
                }

                @Override
                public void smessage(String receivedChannel, String payload) {
                    dispatchMessage(
                            RuntimeResources.this.messageProcessor,
                            receivedChannel,
                            payload,
                            shardedChannelListeners);
                }

                @Override
                public void subscribed(String channel, long count) {
                    notifySubscriptionRestored(channel, false);
                }

                @Override
                public void ssubscribed(String channel, long count) {
                    notifySubscriptionRestored(channel, true);
                }
            };
            this.connectionStateListener = new RedisConnectionStateListener() {
                @Override
                public void onRedisDisconnected(RedisChannelHandler<?, ?> connection) {
                    if (!closing.get()) {
                        notifySubscriptionsInterrupted(
                                new IllegalStateException("Lettuce Pub/Sub connection disconnected"));
                    }
                }
            };
        }

        private void notifySubscriptionsInterrupted(Throwable cause) {
            notifySubscriptionsInterrupted(channelListeners, false, cause);
            notifySubscriptionsInterrupted(shardedChannelListeners, true, cause);
        }

        private void notifySubscriptionsInterrupted(
                ConcurrentMap<String, Set<CacheMessageListener>> listenersByChannel,
                boolean sharded,
                Throwable cause) {
            listenersByChannel.forEach((channel, listeners) -> {
                ChannelSubscription subscription = new ChannelSubscription(channel, sharded);
                if (!interruptedSubscriptions.add(subscription)) {
                    return;
                }
                for (CacheMessageListener listener : listeners) {
                    messageProcessor.notifyDeliveryEvent(listener, new CacheMessageDeliveryEvent(
                            channel,
                            CacheMessageDeliveryEventType.SUBSCRIPTION_INTERRUPTED,
                            0L,
                            cause));
                }
            });
        }

        private void notifySubscriptionRestored(String channel, boolean sharded) {
            if (!interruptedSubscriptions.remove(new ChannelSubscription(channel, sharded))) {
                return;
            }
            ConcurrentMap<String, Set<CacheMessageListener>> listenersByChannel =
                    sharded ? shardedChannelListeners : channelListeners;
            Set<CacheMessageListener> listeners = listenersByChannel.get(channel);
            if (listeners == null) {
                return;
            }
            for (CacheMessageListener listener : listeners) {
                messageProcessor.notifyDeliveryEvent(listener, new CacheMessageDeliveryEvent(
                        channel,
                        CacheMessageDeliveryEventType.SUBSCRIPTION_RESTORED,
                        0L,
                        null));
            }
        }
    }

    private record ChannelSubscription(String channel, boolean sharded) {
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

    private record PubSubConnectionState(boolean created) {
    }
}
