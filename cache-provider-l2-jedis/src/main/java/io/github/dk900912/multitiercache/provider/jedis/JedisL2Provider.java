package io.github.dk900912.multitiercache.provider.jedis;

import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.CacheMessageListener;
import io.github.dk900912.multitiercache.api.CacheMessageSubscription;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.spi.L2Provider;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Connection;
import redis.clients.jedis.ConnectionPoolConfig;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.RedisClusterClient;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.params.SetParams;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Level 2 (L2) cache provider implementation using Jedis.
 *
 * @author dukui
 */
public class JedisL2Provider implements L2Provider, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(JedisL2Provider.class);

    private static final AtomicInteger POOL_COUNTER = new AtomicInteger(1);

    private RedisClusterClient redisClient;
    private ExecutorService pubSubExecutor;

    private final Set<JedisPubSub> activePubSubs = ConcurrentHashMap.newKeySet();

    public JedisL2Provider() {
    }

    @Override
    public CacheConfig.L2ProviderType providerType() {
        return CacheConfig.L2ProviderType.JEDIS;
    }

    @Override
    public void initialize(CacheConfig.L2Config config) {
        Objects.requireNonNull(config, "L2 config cannot be null");
        Set<HostAndPort> nodes = parseNodes(config.getHosts());
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

        this.redisClient = RedisClusterClient.builder()
                .nodes(nodes)
                .clientConfig(clientConfigBuilder.build())
                .poolConfig(poolConfig)
                .maxAttempts(config.getMaxRedirects() == null ? RedisClusterClient.DEFAULT_MAX_ATTEMPTS : config.getMaxRedirects())
                .build();

        CacheConfig.Subscriber subscriber = Objects.requireNonNull(
                config.getSubscriber(), "Subscriber config cannot be null");
        int corePoolSize = subscriber.getCorePoolSize();
        int maximumPoolSize = subscriber.getMaximumPoolSize();
        long keepAliveTime = subscriber.getKeepAliveTime().toMillis();
        int capacity = subscriber.getCapacity();

        this.pubSubExecutor = new ThreadPoolExecutor(
                corePoolSize, maximumPoolSize,
                keepAliveTime, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(capacity),
                r -> {
                    Thread thread = new Thread(r);
                    thread.setName("jedis-pubsub-worker-" + POOL_COUNTER.getAndIncrement());
                    thread.setDaemon(true);
                    return thread;
                },
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    private static GenericObjectPoolConfig<Connection> getObjectPoolConfig(CacheConfig.L2Config config) {
        GenericObjectPoolConfig<Connection> poolConfig = new ConnectionPoolConfig();
        CacheConfig.Jedis jedis = Objects.requireNonNull(config.getJedis(), "Jedis config cannot be null");
        Integer maxTotal = config.getMaxTotal() != null ? config.getMaxTotal() : jedis.getMaxTotal();
        Integer maxIdle = config.getMaxIdle() != null ? config.getMaxIdle() : jedis.getMaxIdle();
        Integer minIdle = config.getMinIdle() != null ? config.getMinIdle() : jedis.getMinIdle();
        Duration maxWait = config.getMaxWait() != null ? config.getMaxWait() : jedis.getMaxWait();
        if (maxTotal != null) {
            poolConfig.setMaxTotal(maxTotal);
        }
        if (maxIdle != null) {
            poolConfig.setMaxIdle(maxIdle);
        }
        if (minIdle != null) {
            poolConfig.setMinIdle(minIdle);
        }
        if (maxWait != null) {
            poolConfig.setMaxWait(maxWait);
        }
        return poolConfig;
    }

    @Override
    public String get(CacheKey key) {
        ensureInitialized();
        return redisClient.get(key.toKeyString());
    }

    @Override
    public void set(CacheKey key, String value, Duration ttl) {
        ensureInitialized();
        if (ttl != null && ttl.toMillis() > 0) {
            redisClient.set(key.toKeyString(), value, SetParams.setParams().px(ttl.toMillis()));
        } else {
            redisClient.set(key.toKeyString(), value);
        }
    }

    @Override
    public void delete(CacheKey key) {
        ensureInitialized();
        redisClient.del(key.toKeyString());
    }

    @Override
    public void publish(String channel, String message) {
        ensureInitialized();
        redisClient.publish(channel, message);
    }

    @Override
    public CacheMessageSubscription subscribe(String channel, CacheMessageListener listener) {
        ensureInitialized();
        AtomicBoolean closing = new AtomicBoolean(false);
        JedisPubSub pubSub = new JedisPubSub() {
            @Override
            public void onMessage(String receivedChannel, String payload) {
                try {
                    listener.onMessage(receivedChannel, payload);
                } catch (Exception e) {
                    LOGGER.error("Failed to process cache message. Channel: {}, Payload: {}", receivedChannel, payload, e);
                }
            }
        };

        activePubSubs.add(pubSub);

        pubSubExecutor.submit(() -> {
            int retryDelay = 1;
            while (!closing.get() && !Thread.currentThread().isInterrupted()) {
                try {
                    redisClient.subscribe(pubSub, channel);
                    break;
                } catch (JedisConnectionException e) {
                    if (!closing.get()) {
                        LOGGER.warn("Redis Pub/Sub connection lost for channel [{}]. Reconnecting in {} seconds...", channel, retryDelay, e);
                        sleepSilently(retryDelay);
                        retryDelay = Math.min(retryDelay * 2, 10);
                    }
                } catch (Exception e) {
                    if (!closing.get()) {
                        LOGGER.error("Unexpected error in Pub/Sub loop for channel [{}]. Retrying in 5 seconds...", channel, e);
                        sleepSilently(5);
                    }
                }
            }
        });

        return () -> {
            closing.set(true);
            activePubSubs.remove(pubSub);
            try {
                if (pubSub.isSubscribed()) {
                    pubSub.unsubscribe();
                }
            } catch (Exception e) {
                LOGGER.debug("Exception swallowed while unsubscribing from channel: {}", channel, e);
            }
        };
    }

    @Override
    public Object eval(String script, List<String> keys, List<String> args) {
        ensureInitialized();
        return redisClient.eval(script, keys, args);
    }

    private void sleepSilently(int seconds) {
        try {
            TimeUnit.SECONDS.sleep(seconds);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() throws Exception {
        for (JedisPubSub pubSub : activePubSubs) {
            try {
                if (pubSub.isSubscribed()) {
                    pubSub.unsubscribe();
                }
            } catch (Exception e) {
                LOGGER.trace("Ignored exception during graceful shutdown unsubscribe", e);
            }
        }
        activePubSubs.clear();

        if (pubSubExecutor != null && !pubSubExecutor.isShutdown()) {
            try {
                pubSubExecutor.shutdownNow();
            } catch (Exception e) {
                LOGGER.warn("Failed to shut down Jedis pub/sub executor", e);
            }
        }

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
            HostPort hostPort = HostPort.parse(host);
            nodes.add(new HostAndPort(hostPort.host, hostPort.port));
        }
        return nodes;
    }

    private static int toMillis(Duration duration) {
        return Math.toIntExact(Objects.requireNonNull(duration, "Redis timeout cannot be null").toMillis());
    }

    private void ensureInitialized() {
        if (redisClient == null || pubSubExecutor == null) {
            throw new IllegalStateException("Jedis L2 provider is not initialized");
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
