package io.github.dk900912.multitiercache.provider.redisson;

import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.CacheMessageListener;
import io.github.dk900912.multitiercache.api.CacheMessageSubscription;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.spi.L2Provider;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RScript;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Level 2 (L2) cache provider implementation using Redisson.
 *
 * @author dukui
 */
public class RedissonL2Provider implements L2Provider, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedissonL2Provider.class);
    private static final AtomicInteger POOL_COUNTER = new AtomicInteger(1);

    private RedissonClient redissonClient;
    private ExecutorService messageProcessor;

    public RedissonL2Provider() {
    }

    @Override
    public void initialize(CacheConfig.L2Config config) {
        Objects.requireNonNull(config, "L2 config cannot be null");
        Config redissonConfig = new Config();
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
        if (config.getMaxIdle() != null) {
            clusterConfig.setMasterConnectionMinimumIdleSize(config.getMaxIdle());
            clusterConfig.setSlaveConnectionMinimumIdleSize(config.getMaxIdle());
        }
        if (config.getMaxTotal() != null) {
            clusterConfig.setMasterConnectionPoolSize(config.getMaxTotal());
            clusterConfig.setSlaveConnectionPoolSize(config.getMaxTotal());
        }
        if (config.getConnectionTimeout() != null) {
            clusterConfig.setConnectTimeout(Math.toIntExact(config.getConnectionTimeout().toMillis()));
        }
        if (config.getSocketTimeout() != null) {
            clusterConfig.setTimeout(Math.toIntExact(config.getSocketTimeout().toMillis()));
        }
        this.redissonClient = Redisson.create(redissonConfig);

        CacheConfig.Subscriber subscriber = Objects.requireNonNull(
                config.getSubscriber(), "Subscriber config cannot be null");
        int corePoolSize = subscriber.getCorePoolSize();
        int maximumPoolSize = subscriber.getMaximumPoolSize();
        long keepAliveTime = subscriber.getKeepAliveTime().toMillis();
        int capacity = subscriber.getCapacity();

        this.messageProcessor = new ThreadPoolExecutor(
                corePoolSize, maximumPoolSize,
                keepAliveTime, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(capacity),
                r -> {
                    Thread thread = new Thread(r);
                    thread.setName("redisson-pubsub-worker-" + POOL_COUNTER.getAndIncrement());
                    thread.setDaemon(true);
                    return thread;
                },
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    @Override
    public String get(CacheKey key) {
        ensureInitialized();
        return (String) redissonClient.getBucket(key.toKeyString(), StringCodec.INSTANCE).get();
    }

    @Override
    public void set(CacheKey key, String value, Duration ttl) {
        ensureInitialized();
        RBucket<String> bucket = redissonClient.getBucket(key.toKeyString(), StringCodec.INSTANCE);
        if (ttl != null) {
            bucket.set(value, ttl);
            return;
        }
        bucket.set(value);
    }

    @Override
    public void delete(CacheKey key) {
        ensureInitialized();
        redissonClient.getBucket(key.toKeyString(), StringCodec.INSTANCE).delete();
    }

    @Override
    public void publish(String channel, String message) {
        ensureInitialized();
        redissonClient.getTopic(channel, StringCodec.INSTANCE).publish(message);
    }

    @Override
    public CacheMessageSubscription subscribe(String channel, CacheMessageListener listener) {
        ensureInitialized();
        RTopic topic = redissonClient.getTopic(channel, StringCodec.INSTANCE);

        int listenerId = topic.addListener(String.class, (receivedChannel, payload) -> {
            messageProcessor.submit(() -> {
                try {
                    listener.onMessage(receivedChannel.toString(), payload);
                } catch (Exception e) {
                    LOGGER.error("Redisson cache message processing failed for channel: {}, payload: {}", receivedChannel, payload, e);
                }
            });
        });

        LOGGER.info("Successfully subscribed to Redis channel [{}] via Redisson.", channel);

        return () -> {
            try {
                topic.removeListener(listenerId);
            } catch (Exception e) {
                LOGGER.debug("Error removing listener for channel [{}]", channel, e);
            }
        };
    }

    @Override
    public Object eval(String script, List<String> keys, List<String> args) {
        ensureInitialized();
        return redissonClient.getScript(StringCodec.INSTANCE).eval(
                RScript.Mode.READ_WRITE,
                script,
                RScript.ReturnType.VALUE,
                new java.util.ArrayList<Object>(keys),
                args.toArray()
        );
    }

    @Override
    public void close() {
        if (messageProcessor != null && !messageProcessor.isShutdown()) {
            messageProcessor.shutdownNow();
        }
        if (redissonClient != null && !redissonClient.isShutdown()) {
            redissonClient.shutdown();
        }
    }

    private static String normalizeAddress(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Redis host cannot be blank");
        }
        String normalized = value.trim();
        if (normalized.startsWith("redis://") || normalized.startsWith("rediss://")) {
            return normalized;
        }
        if (normalized.lastIndexOf(':') < 0) {
            normalized = normalized + ":6379";
        }
        return "redis://" + normalized;
    }

    private void ensureInitialized() {
        if (redissonClient == null || messageProcessor == null) {
            throw new IllegalStateException("Redisson L2 provider is not initialized");
        }
    }
}
