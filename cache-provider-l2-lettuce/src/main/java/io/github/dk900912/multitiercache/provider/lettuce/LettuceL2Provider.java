package io.github.dk900912.multitiercache.provider.lettuce;

import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.CacheMessageListener;
import io.github.dk900912.multitiercache.api.CacheMessageSubscription;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.spi.L2Provider;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.pubsub.RedisPubSubAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Level 2 (L2) cache provider implementation using Lettuce.
 *
 * @author dukui
 */
public class LettuceL2Provider implements L2Provider, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(LettuceL2Provider.class);
    private static final AtomicInteger POOL_COUNTER = new AtomicInteger(1);

    private RedisClusterClient clusterClient;
    private StatefulRedisClusterConnection<String, String> connection;
    private ExecutorService messageProcessor;

    public LettuceL2Provider() {
    }

    @Override
    public void initialize(CacheConfig.L2Config config) {
        Objects.requireNonNull(config, "L2 config cannot be null");
        List<RedisURI> redisURIs = parseRedisUris(config);
        RedisClusterClient client = RedisClusterClient.create(redisURIs);
        ClusterClientOptions options = ClusterClientOptions.builder()
                .maxRedirects(config.getMaxRedirects() == null ? ClusterClientOptions.DEFAULT_MAX_REDIRECTS : config.getMaxRedirects())
                .socketOptions(SocketOptions.builder()
                        .connectTimeout(config.getConnectionTimeout())
                        .build())
                .build();
        client.setOptions(options);
        this.clusterClient = client;
        this.connection = clusterClient.connect();

        CacheConfig.Subscriber subscriber = config.getSubscriber();
        int corePoolSize = subscriber != null ? subscriber.getCorePoolSize() : 1;
        int maximumPoolSize = subscriber != null ? subscriber.getMaximumPoolSize() : 1;
        long keepAliveTime = subscriber != null && subscriber.getKeepAliveTime() != null ? subscriber.getKeepAliveTime().toMillis() : 0L;
        int capacity = subscriber != null ? subscriber.getCapacity() : 100;

        this.messageProcessor = new ThreadPoolExecutor(
                corePoolSize, maximumPoolSize,
                keepAliveTime, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(capacity),
                r -> {
                    Thread thread = new Thread(r);
                    thread.setName("lettuce-pubsub-worker-" + POOL_COUNTER.getAndIncrement());
                    thread.setDaemon(true);
                    return thread;
                },
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    @Override
    public String get(CacheKey key) {
        ensureInitialized();
        return connection.sync().get(key.toRedisKey());
    }

    @Override
    public void set(CacheKey key, String value, Duration ttl) {
        ensureInitialized();
        if (ttl != null) {
            connection.sync().set(key.toRedisKey(), value, new SetArgs().px(ttl.toMillis()));
            return;
        }
        connection.sync().set(key.toRedisKey(), value);
    }

    @Override
    public void delete(CacheKey key) {
        ensureInitialized();
        connection.sync().del(key.toRedisKey());
    }

    @Override
    public void publish(String channel, String message) {
        ensureInitialized();
        connection.sync().publish(channel, message);
    }

    @Override
    public CacheMessageSubscription subscribe(String channel, CacheMessageListener listener) {
        ensureInitialized();
        StatefulRedisClusterPubSubConnection<String, String> pubSubConnection = clusterClient.connectPubSub();
        RedisPubSubAdapter<String, String> adapter = new RedisPubSubAdapter<>() {
            @Override
            public void message(String receivedChannel, String payload) {
                messageProcessor.submit(() -> {
                    try {
                        listener.onMessage(receivedChannel, payload);
                    } catch (Exception e) {
                        LOGGER.error("Lettuce cache message processing failed for channel: {}, payload: {}", receivedChannel, payload, e);
                    }
                });
            }
        };
        pubSubConnection.addListener(adapter);
        pubSubConnection.sync().subscribe(channel);
        
        LOGGER.info("Successfully subscribed to Redis channel [{}] via Lettuce.", channel);
        
        return () -> {
            try {
                pubSubConnection.sync().unsubscribe(channel);
                pubSubConnection.removeListener(adapter);
                pubSubConnection.close();
            } catch (Exception e) {
                LOGGER.debug("Error unsubscribing/closing connection for channel [{}]", channel, e);
            }
        };
    }

    @Override
    public Object eval(String script, List<String> keys, List<String> args) {
        ensureInitialized();
        return connection.sync().eval(script, ScriptOutputType.INTEGER, keys.toArray(new String[0]), args.toArray(new String[0]));
    }

    @Override
    public void close() {
        if (messageProcessor != null && !messageProcessor.isShutdown()) {
            messageProcessor.shutdownNow();
        }
        if (connection != null) {
            connection.close();
        }
        if (clusterClient != null) {
            clusterClient.shutdown();
        }
    }

    private static List<RedisURI> parseRedisUris(CacheConfig.L2Config config) {
        if (config.getHosts() == null || config.getHosts().isEmpty()) {
            throw new IllegalArgumentException("Redis cluster hosts cannot be empty");
        }
        List<RedisURI> redisURIs = new ArrayList<>();
        for (String host : config.getHosts()) {
            HostPort hostPort = HostPort.parse(host);
            RedisURI.Builder builder = RedisURI.Builder.redis(hostPort.host, hostPort.port)
                    .withTimeout(config.getSocketTimeout());
            if (config.getUsername() != null && config.getPassword() != null) {
                builder.withAuthentication(config.getUsername(), config.getPassword());
            } else if (config.getPassword() != null) {
                builder.withPassword(config.getPassword());
            }
            redisURIs.add(builder.build());
        }
        return redisURIs;
    }

    private void ensureInitialized() {
        if (clusterClient == null || connection == null) {
            throw new IllegalStateException("Lettuce L2 provider is not initialized");
        }
    }

    private static class HostPort {
        private static final int DEFAULT_PORT = 6379;

        private final String host;
        private final int port;

        private HostPort(String host, int port) {
            this.host = host;
            this.port = port;
        }

        private static HostPort parse(String value) {
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException("Redis host cannot be blank");
            }
            String normalized = value.trim();
            int lastColon = normalized.lastIndexOf(':');
            if (lastColon < 0) {
                return new HostPort(normalized, DEFAULT_PORT);
            }
            return new HostPort(normalized.substring(0, lastColon), Integer.parseInt(normalized.substring(lastColon + 1)));
        }
    }
}
