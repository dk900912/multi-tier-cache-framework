package io.github.dk900912.multitiercache.core;

import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.CacheLoader;
import io.github.dk900912.multitiercache.api.CacheManager;
import io.github.dk900912.multitiercache.api.CacheMessageRepository;
import io.github.dk900912.multitiercache.api.CacheMessageSubscription;
import io.github.dk900912.multitiercache.api.CacheMonitor;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.api.model.CacheLoadResult;
import io.github.dk900912.multitiercache.api.model.CacheMessage;
import io.github.dk900912.multitiercache.api.model.CacheMessageType;
import io.github.dk900912.multitiercache.api.model.L1CacheStats;
import io.github.dk900912.multitiercache.spi.CacheCodec;
import io.github.dk900912.multitiercache.spi.L1Provider;
import io.github.dk900912.multitiercache.spi.L2Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static io.github.dk900912.multitiercache.api.model.CacheMessageType.BACKFILL;
import static io.github.dk900912.multitiercache.api.model.CacheMessageType.DELETE;
import static io.github.dk900912.multitiercache.api.model.CacheMessageType.INSERT;
import static io.github.dk900912.multitiercache.api.model.CacheMessageType.PENETRATE;
import static io.github.dk900912.multitiercache.api.model.CacheMessageType.UPDATE;

/**
 * Default implementation of the {@link io.github.dk900912.multitiercache.api.CacheManager}.
 * <p>
 * Coordinates the L1 and L2 providers, handles cache misses, and uses {@link SingleFlight} to prevent cache breakdown.
 * </p>
 *
 * @author dukui
 */
public class DefaultCacheManager implements CacheManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultCacheManager.class);

    private static final Long SUCCESS = 1L;

    private final CacheConfig cacheConfig;
    private final L1Provider l1Provider;
    private final L2Provider l2Provider;
    private final CacheMessageRepository cacheMessageRepository;

    private final AtomicReference<CacheMessageSubscription> subscription = new AtomicReference<>();
    private final LifecycleStateMachine lifecycleStateMachine = new LifecycleStateMachine("CacheManager");

    private final CacheCodec cacheCodec;
    private final SingleFlight singleFlight;

    public DefaultCacheManager(CacheConfig cacheConfig,
                               L1Provider l1Provider,
                               L2Provider l2Provider,
                               CacheMessageRepository cacheMessageRepository,
                               CacheCodec cacheCodec,
                               SingleFlight singleFlight) {
        this.cacheConfig = Objects.requireNonNull(cacheConfig, "CacheConfig cannot be null");
        this.l1Provider = Objects.requireNonNull(l1Provider, "L1Provider cannot be null");
        this.l2Provider = Objects.requireNonNull(l2Provider, "L2Provider cannot be null");
        this.cacheMessageRepository = Objects.requireNonNull(cacheMessageRepository, "CacheMessageRepository cannot be null");
        this.cacheCodec = Objects.requireNonNull(cacheCodec, "CacheCodec cannot be null");
        this.singleFlight = Objects.requireNonNull(singleFlight, "SingleFlight cannot be null");
    }

    @Override
    public <T> T get(CacheKey key, Supplier<T> loader) {
        return get(key, loader, cacheConfig.getCacheMiss().getDefaultTtl());
    }

    @Override
    public <T> T get(CacheKey key, Supplier<T> loader, Duration ttl) {
        Objects.requireNonNull(loader, "Cache loader cannot be null");
        return get(key, () -> {
            T data = loader.get();
            if (data == null) {
                return CacheLoadResult.penetration(resolvePenetrationTtl(ttl));
            }
            return CacheLoadResult.of(data, AnnotationVersionExtractor.extract(data), ttl);
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(CacheKey key, CacheLoader<T> loader) {
        Objects.requireNonNull(key, "CacheKey cannot be null");
        Objects.requireNonNull(loader, "CacheLoader cannot be null");

        CacheMessage<?> l1Message = readFromL1(key);
        if (l1Message != null) {
            return (T) unwrapCacheMessage(l1Message);
        }

        CacheMessage<?> l2Message = readFromL2(key);
        if (l2Message != null) {
            return (T) unwrapCacheMessage(l2Message);
        }

        return (T) loadWithSingleFlight(key, loader);
    }

    @Override
    public void insert(CacheKey key, Object data, Long version, Duration ttl) {
        apply(createMutationMessage(key, data, version, INSERT, ttl));
    }

    @Override
    public void update(CacheKey key, Object data, Long version, Duration ttl) {
        apply(createMutationMessage(key, data, version, UPDATE, ttl));
    }

    @Override
    public void evict(CacheKey key, Long version, Duration ttl) {
        apply(createMutationMessage(key, null, version, DELETE, ttl));
    }

    @Override
    public CacheMonitor getMonitor() {
        return new CacheMonitor() {
            @Override
            public L1CacheStats getL1CacheStats() {
                if (!isL1Enabled()) {
                    return null;
                }
                return l1Provider.getStats();
            }
        };
    }

    @Override
    public void bootstrap() {
        if (!lifecycleStateMachine.beginBootstrap()) {
            return;
        }

        try {
            Objects.requireNonNull(cacheConfig, "CacheConfig cannot be null");
            if (!isL2Enabled()) {
                lifecycleStateMachine.markStarted();
                return;
            }
            Objects.requireNonNull(cacheConfig.getL2().getMutationChannelName(), "MutationChannelName cannot be null");

            CacheMessageSubscription newSubscription = l2Provider.subscribe(
                    cacheConfig.getL2().getMutationChannelName(),
                    (receivedChannel, payload) -> {
                        try {
                            CacheMessage<?> message = cacheCodec.decodeMessage(payload, Object.class);
                            CacheKey key = message::getKey;
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("Invalidating L1 cache for key {} from pub/sub message", key.toRedisKey());
                            }
                            l1Provider.invalidate(key);
                        } catch (Exception e) {
                            LOGGER.warn("Ignoring malformed L1 invalidation message from channel {}", receivedChannel, e);
                        }
                    });

            CacheMessageSubscription oldSubscription = this.subscription.getAndSet(newSubscription);
            if (oldSubscription != null) {
                try {
                    oldSubscription.close();
                } catch (Exception e) {
                    LOGGER.warn("Failed to close previous cache message subscription", e);
                }
            }
            lifecycleStateMachine.markStarted();
        } catch (Exception e) {
            closeSubscription(subscription.getAndSet(null), "Failed to close cache message subscription after bootstrap failure");
            lifecycleStateMachine.markBootstrapFailed();
            throw e;
        }
    }

    @Override
    public void shutdown() {
        if (!lifecycleStateMachine.beginShutdown()) {
            return;
        }

        CacheMessageSubscription currentSubscription = subscription.getAndSet(null);
        closeSubscription(currentSubscription, "Failed to close cache message subscription during shutdown");

        if (l1Provider instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception e) {
                LOGGER.warn("Failed to close L1Provider during shutdown", e);
            }
        }

        if (l2Provider instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception e) {
                LOGGER.warn("Failed to close L2Provider during shutdown", e);
            }
        }

        if (cacheMessageRepository instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception e) {
                LOGGER.warn("Failed to close CacheMessageRepository during shutdown", e);
            }
        }
    }

    @Override
    public void apply(CacheMessage<?> message) {
        Objects.requireNonNull(message, "CacheMessage cannot be null");
        validateCacheMessage(message);

        CacheKey key = message::getKey;
        invalidateLocalL1(key);

        if (!isL2Enabled()) {
            return;
        }

        String payload = cacheCodec.encode(message);
        try {
            propagateMutationToL2(key, message, payload);
        } catch (Exception e) {
            cacheMessageRepository.save(message);
            throw new IllegalStateException("Failed to apply cache mutation for key " + message.getKey(), e);
        }
    }

    private Object loadWithSingleFlight(CacheKey key, CacheLoader<?> loader) {
        Duration awaitTimeout = cacheConfig.getSingleFlight().getAwaitTimeout();
        String keyString = key.toRedisKey();
        return singleFlight.execute(keyString, awaitTimeout, () -> loadAsSingleFlightOwner(key, loader));
    }

    private Object loadAsSingleFlightOwner(CacheKey key, CacheLoader<?> loader) {
        CacheMessage<?> localRetry = readFromL1(key, true);
        if (localRetry != null) {
            return unwrapCacheMessage(localRetry);
        }

        if (isL2Enabled()) {
            CacheMessage<?> remoteRetry = readFromL2(key, true);
            if (remoteRetry != null) {
                return unwrapCacheMessage(remoteRetry);
            }
        }

        CacheLoadResult<?> loadResult = Objects.requireNonNull(loader.load(), "CacheLoadResult cannot be null");
        CacheMessage<?> cacheMessage = createReadCacheMessage(key, loadResult);
        writeReadResult(key, cacheMessage, loadResult);
        return loadResult.getData();
    }

    private CacheMessage<?> readFromL1(CacheKey key) {
        return readFromL1(key, false);
    }

    private CacheMessage<?> readFromL1(CacheKey key, boolean quiet) {
        if (!isL1Enabled()) {
            return null;
        }
        Object cached = l1Provider.get(key);
        if (cached == null) {
            if (!quiet && LOGGER.isDebugEnabled()) {
                LOGGER.debug("L1 cache miss for key {}", key.toRedisKey());
            }
            return null;
        }

        if (!(cached instanceof CacheMessage<?> message)) {
            if (!quiet && LOGGER.isDebugEnabled()) {
                LOGGER.debug("Unexpected value type in L1 cache for key {}: {}", key.toRedisKey(), cached.getClass());
            }
            return null;
        }

        if (!quiet && LOGGER.isDebugEnabled()) {
            LOGGER.debug("L1 cache hit for key {}", key.toRedisKey());
        }
        return message;
    }

    private CacheMessage<Object> readFromL2(CacheKey key) {
        return readFromL2(key, false);
    }

    private CacheMessage<Object> readFromL2(CacheKey key, boolean quiet) {
        if (!isL2Enabled()) {
            return null;
        }
        String payload = l2Provider.get(key);
        if (payload == null) {
            if (!quiet && LOGGER.isDebugEnabled()) {
                LOGGER.debug("L2 cache miss for key {}", key.toRedisKey());
            }
            return null;
        }
        CacheMessage<Object> message = cacheCodec.decodeMessage(payload, Object.class);
        if (isL1Enabled()) {
            if (!quiet && LOGGER.isDebugEnabled()) {
                LOGGER.debug("L2 cache hit for key {}", key.toRedisKey());
            }
            l1Provider.put(key, message);
        }
        return message;
    }

    private Object unwrapCacheMessage(CacheMessage<?> message) {
        if (message.getType() == DELETE || message.getType() == PENETRATE) {
            return null;
        }
        return message.getData();
    }

    private CacheMessage<?> createReadCacheMessage(CacheKey key, CacheLoadResult<?> loadResult) {
        CacheMessage<Object> message;
        if (loadResult.isPenetration()) {
            Duration penetrationTtl = resolvePenetrationTtl(loadResult.getTtl());
            message = new CacheMessage<>(key.toRedisKey(), null, loadResult.getVersion(), PENETRATE, toTtlMillis(penetrationTtl));
        } else {
            Duration backfillTtl = resolveBackfillTtl(loadResult.getTtl());
            message = new CacheMessage<>(key.toRedisKey(), loadResult.getData(), loadResult.getVersion(), BACKFILL, toTtlMillis(backfillTtl));
        }
        validateCacheMessage(message);
        return message;
    }

    private void writeReadResult(CacheKey key, CacheMessage<?> cacheMessage, CacheLoadResult<?> loadResult) {
        if (isL2Enabled()) {
            if (cacheMessage.getType() == PENETRATE) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Backfilling L2 cache for key {} with value {} due to cache penetration", key.toRedisKey(), loadResult.getData());
                }
                l2Provider.set(key, cacheCodec.encode(cacheMessage), resolveTtl(cacheMessage));
            } else if (cacheMessage.getType() == BACKFILL) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Backfilling L2 cache for key {} with value {} due to cache miss", key.toRedisKey(), loadResult.getData());
                }
                l2Provider.set(key, cacheCodec.encode(cacheMessage), resolveTtl(cacheMessage));
            }
        }

        if (isL1Enabled()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Backfilling L1 cache for key {} with value {} due to cache penetration/miss", key.toRedisKey(), loadResult.getData());
            }
            l1Provider.put(key, cacheMessage);
        }
    }

    private CacheMessage<Object> createMutationMessage(CacheKey key, Object data, Long version, CacheMessageType type, Duration ttl) {
        Objects.requireNonNull(key, "CacheKey cannot be null");
        CacheMessage<Object> message = new CacheMessage<>(key.toRedisKey(), data, version, type, toTtlMillis(ttl));
        validateCacheMessage(message);
        return message;
    }

    private void validateCacheMessage(CacheMessage<?> message) {
        Objects.requireNonNull(message, "CacheMessage cannot be null");
        Objects.requireNonNull(message.getKey(), "CacheMessage's key cannot be null");
        Objects.requireNonNull(message.getVersion(), "CacheMessage's version cannot be null");
        Objects.requireNonNull(message.getType(), "CacheMessage's type cannot be null");
        Objects.requireNonNull(message.getTtlMillis(), "CacheMessage's ttl-millis cannot be null");

        switch (message.getType()) {
            case INSERT, UPDATE -> {
                if (message.getData() == null) {
                    throw new IllegalArgumentException("Inserting/Updating mutation must carry a payload data");
                }
                if (message.getVersion() < 0) {
                    throw new IllegalArgumentException("Inserting/Updating mutation must carry an effective version");
                }
            }
            case DELETE -> {
                if (message.getData() != null) {
                    throw new IllegalArgumentException("Deleting mutation must not carry a payload data");
                }
                if (message.getVersion() < 0) {
                    throw new IllegalArgumentException("Deleting mutation must carry an effective version");
                }
            }
            case PENETRATE -> {
                if (message.getData() != null) {
                    throw new IllegalArgumentException("Penetrating mutation must not carry a payload data");
                }
                if (message.getVersion() != -1) {
                    throw new IllegalArgumentException("Penetrating mutation must carry an effective version");
                }
            }
            case BACKFILL -> {
                if (message.getData() == null) {
                    throw new IllegalArgumentException("Backfilling mutation must carry a payload data");
                }
                if (message.getVersion() < 0) {
                    throw new IllegalArgumentException("Backfilling mutation must carry an effective version");
                }
            }
        }
    }

    private void propagateMutationToL2(CacheKey key, CacheMessage<?> message, String payload) {
        String script = luaScript(message);
        List<String> keys = Collections.singletonList(key.toRedisKey());
        List<String> args = List.of(
                payload,
                String.valueOf(message.getTtlMillis()),
                String.valueOf(message.getVersion()),
                String.valueOf(cacheConfig.getL2().getMutationChannelName())
        );
        Object rst = l2Provider.eval(script, keys, args);
        if (SUCCESS.equals(rst)) {
            LOGGER.debug(
                    "Applied L2 cache mutation and published cache synchronization event for key {}",
                    key.toRedisKey()
            );
        } else {
            LOGGER.debug(
                    "Skipped L2 cache mutation for key {} because the incoming version was not newer",
                    key.toRedisKey()
            );
        }
    }

    private void invalidateLocalL1(CacheKey key) {
        if (isL1Enabled()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Directly Invalidating L1 cache for key {} due to cache mutation", key.toRedisKey());
            }
            l1Provider.invalidate(key);
        }
    }

    private boolean isL1Enabled() {
        return cacheConfig.getL1() != null && cacheConfig.getL1().isEnabled();
    }

    private boolean isL2Enabled() {
        return cacheConfig.getL2() != null && cacheConfig.getL2().isEnabled();
    }

    private void closeSubscription(CacheMessageSubscription currentSubscription, String warningMessage) {
        if (currentSubscription == null) {
            return;
        }
        try {
            currentSubscription.close();
        } catch (Exception e) {
            LOGGER.warn(warningMessage, e);
        }
    }

    private Duration resolvePenetrationTtl(Duration candidate) {
        return candidate != null ? candidate : cacheConfig.getCacheMiss().getPenetrationTtl();
    }

    private Duration resolveBackfillTtl(Duration candidate) {
        return candidate != null ? candidate : cacheConfig.getCacheMiss().getBackfillTtl();
    }

    private Duration resolveTtl(CacheMessage<?> message) {
        return message.getTtlMillis() == null ? null : Duration.ofMillis(message.getTtlMillis());
    }

    private Long toTtlMillis(Duration ttl) {
        if (ttl == null) {
            return null;
        }
        if (ttl.isNegative() || ttl.isZero()) {
            throw new IllegalArgumentException("TTL must be positive when provided");
        }
        return ttl.toMillis();
    }

    private String luaScript(CacheMessage<?> message) {
        String script;
        switch (message.getType()) {
            case INSERT, UPDATE -> script = CacheLuaScripts.UPSERT_LUA_SCRIPT;
            case DELETE -> script = CacheLuaScripts.DELETE_LUA_SCRIPT;
            case PENETRATE, BACKFILL -> script = CacheLuaScripts.CACHE_MISS_LUA_SCRIPT;
            default -> throw new IllegalArgumentException("Unsupported cache message type: " + message.getType());
        }
        return script;
    }

}
