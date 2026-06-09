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
import io.github.dk900912.multitiercache.api.model.CacheRuntimeStats;
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
    private final CacheRuntimeMetricsRecorder runtimeMetrics;

    public DefaultCacheManager(CacheConfig cacheConfig,
                               L1Provider l1Provider,
                               L2Provider l2Provider,
                               CacheMessageRepository cacheMessageRepository,
                               CacheCodec cacheCodec,
                               SingleFlight singleFlight) {
        this(cacheConfig, l1Provider, l2Provider, cacheMessageRepository, cacheCodec, singleFlight, new CacheRuntimeMetricsRecorder());
    }

    DefaultCacheManager(CacheConfig cacheConfig,
                        L1Provider l1Provider,
                        L2Provider l2Provider,
                        CacheMessageRepository cacheMessageRepository,
                        CacheCodec cacheCodec,
                        SingleFlight singleFlight,
                        CacheRuntimeMetricsRecorder runtimeMetrics) {
        this.cacheConfig = Objects.requireNonNull(cacheConfig, "CacheConfig cannot be null");
        this.l1Provider = Objects.requireNonNull(l1Provider, "L1Provider cannot be null");
        this.l2Provider = Objects.requireNonNull(l2Provider, "L2Provider cannot be null");
        this.cacheMessageRepository = Objects.requireNonNull(cacheMessageRepository, "CacheMessageRepository cannot be null");
        this.cacheCodec = Objects.requireNonNull(cacheCodec, "CacheCodec cannot be null");
        this.singleFlight = Objects.requireNonNull(singleFlight, "SingleFlight cannot be null");
        this.runtimeMetrics = Objects.requireNonNull(runtimeMetrics, "CacheRuntimeMetricsRecorder cannot be null");
    }

    @Override
    public <T> T get(CacheKey key, Supplier<T> loader) {
        return get(key, loader, cacheConfig.getCacheMiss().getDefaultTtl());
    }

    @Override
    public <T> T get(CacheKey key, Supplier<T> loader, Duration ttl) {
        Objects.requireNonNull(loader, "SupplierLoader cannot be null");
        Objects.requireNonNull(ttl, "TTL cannot be null");
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

            @Override
            public CacheRuntimeStats getRuntimeStats() {
                return runtimeMetrics.snapshot();
            }
        };
    }

    @Override
    public void apply(CacheMessage<?> message) {
        Objects.requireNonNull(message, "CacheMessage cannot be null");
        validateCacheMessage(message);

        if (!isL2Enabled()) {
            applyAcceptedMutationToLocalL1(message);
            return;
        }

        CacheKey key = message::getKey;
        String payload = cacheCodec.encode(message);
        try {
            // L2 must become the authority first; otherwise a concurrent local read can miss L1,
            // observe stale L2, and rehydrate the current node with old data.
            boolean applied = applyMessageToL2(key, message, payload, true);
            if (applied) {
                applyAcceptedMutationToLocalL1(message);
            }
        } catch (Exception e) {
            try {
                cacheMessageRepository.save(message);
                runtimeMetrics.recordCompensationSaveSuccess();
                LOGGER.warn("L2 propagation failed, message saved for compensation replay", e);
            } catch (Exception cause) {
                runtimeMetrics.recordCompensationSaveFailure();
                LOGGER.error("Failed to save message for compensation - data may be lost", cause);
            }
        }
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
                            handleIncomingMutation(message);
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

    private Object loadWithSingleFlight(CacheKey key, CacheLoader<?> loader) {
        Duration awaitTimeout = cacheConfig.getSingleFlight().getAwaitTimeout();
        String keyString = key.toKeyString();
        return singleFlight.execute(keyString, awaitTimeout, () -> loadAsSingleFlightOwner(key, loader));
    }

    private Object loadAsSingleFlightOwner(CacheKey key, CacheLoader<?> loader) {
        // The owner re-checks both cache tiers because another request may have populated them
        // while this thread was waiting to enter the single-flight critical section.
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
        runtimeMetrics.recordLoaderCall(loadResult.isPenetration());
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
            runtimeMetrics.recordL1Miss();
            if (!quiet && LOGGER.isDebugEnabled()) {
                LOGGER.debug("L1 cache miss for key {}", key.toKeyString());
            }
            return null;
        }

        if (!(cached instanceof CacheMessage<?> message)) {
            if (!quiet && LOGGER.isDebugEnabled()) {
                LOGGER.debug("Unexpected value type in L1 cache for key {}: {}", key.toKeyString(), cached.getClass());
            }
            return null;
        }

        if (!quiet && LOGGER.isDebugEnabled()) {
            LOGGER.debug("L1 cache hit for key {}", key.toKeyString());
        }
        runtimeMetrics.recordL1Hit();
        return message;
    }

    private CacheMessage<Object> readFromL2(CacheKey key) {
        return readFromL2(key, false);
    }

    private CacheMessage<Object> readFromL2(CacheKey key, boolean quiet) {
        if (!isL2Enabled()) {
            return null;
        }
        String payload;
        try {
            payload = l2Provider.get(CacheKeyspace.dataKey(key));
        } catch (Exception e) {
            runtimeMetrics.recordL2ReadFailure();
            logL2ReadFailure(key, quiet, "Failed to read L2 cache for key {}", e);
            return null;
        }
        if (payload == null) {
            runtimeMetrics.recordL2Miss();
            if (!quiet && LOGGER.isDebugEnabled()) {
                LOGGER.debug("L2 cache miss for key {}", key.toKeyString());
            }
            return null;
        }
        runtimeMetrics.recordL2Hit();
        CacheMessage<Object> message;
        try {
            message = cacheCodec.decodeMessage(payload, Object.class);
            validateCacheMessage(message);
        } catch (Exception e) {
            runtimeMetrics.recordL2ReadFailure();
            logL2ReadFailure(key, quiet, "Failed to decode L2 cache payload for key {}", e);
            return null;
        }
        if (isL1Enabled()) {
            if (!quiet && LOGGER.isDebugEnabled()) {
                LOGGER.debug("L2 cache hit for key {}", key.toKeyString());
            }
            cacheL1IfNewer(key, message);
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
            message = new CacheMessage<>(key.toKeyString(), null, loadResult.getVersion(), PENETRATE, toTtlMillis(penetrationTtl));
        } else {
            Duration backfillTtl = resolveBackfillTtl(loadResult.getTtl());
            message = new CacheMessage<>(
                    key.toKeyString(),
                    loadResult.getData(),
                    loadResult.getVersion(),
                    BACKFILL,
                    toTtlMillis(backfillTtl)
            );
        }
        validateCacheMessage(message);
        return message;
    }

    private void writeReadResult(CacheKey key, CacheMessage<?> cacheMessage, CacheLoadResult<?> loadResult) {
        boolean appliedToL2 = false;
        boolean l2ApplyFailed = false;
        if (isL2Enabled()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Writing read result to L2 for key {} with type {}", key.toKeyString(), cacheMessage.getType());
            }
            try {
                appliedToL2 = applyMessageToL2(key, cacheMessage, cacheCodec.encode(cacheMessage), false);
            } catch (Exception e) {
                l2ApplyFailed = true;
                LOGGER.warn("Failed to write read result to L2 for key {}; returning loader result without L1 backfill", key.toKeyString(), e);
            }
        }

        if (isL1Enabled()) {
            if (isL2Enabled() && !appliedToL2) {
                if (l2ApplyFailed) {
                    return;
                }
                // If L2 rejected the read result, another fresher state already won the race.
                // Re-read L2 so the local node converges to the authoritative winner instead of
                // writing a stale backfill into L1.
                readFromL2(key, true);
                return;
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Backfilling L1 cache for key {} with value {} due to cache penetration/miss", key.toKeyString(), loadResult.getData());
            }
            cacheL1IfNewer(key, cacheMessage);
        }
    }

    private void logL2ReadFailure(CacheKey key, boolean quiet, String message, Exception e) {
        if (quiet) {
            LOGGER.debug(message, key.toKeyString(), e);
        } else {
            LOGGER.warn(message, key.toKeyString(), e);
        }
    }

    private CacheMessage<Object> createMutationMessage(CacheKey key, Object data, Long version, CacheMessageType type, Duration ttl) {
        Objects.requireNonNull(key, "CacheKey cannot be null");
        return new CacheMessage<>(key.toKeyString(), data, version, type, toTtlMillis(ttl));
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

    private boolean applyMessageToL2(CacheKey key, CacheMessage<?> message, String payload, boolean publish) {
        try {
            List<String> keys = Collections.singletonList(CacheKeyspace.dataKey(key).toKeyString());
            List<String> args = List.of(
                    payload,
                    String.valueOf(message.getTtlMillis()),
                    message.getType().getWireValue(),
                    String.valueOf(message.getVersion()),
                    String.valueOf(cacheConfig.getL2().getMutationChannelName()),
                    publish ? "1" : "0"
            );
            Object rst = l2Provider.eval(CacheLuaScripts.APPLY_MESSAGE_LUA_SCRIPT, keys, args);
            if (SUCCESS.equals(rst)) {
                recordL2ApplyAccepted(publish);
                LOGGER.debug(
                        "Applied L2 cache message for key {} with type {} and version {}",
                        key.toKeyString(),
                        message.getType(),
                        message.getVersion()
                );
                return true;
            } else {
                recordL2ApplyRejected(publish);
                LOGGER.debug(
                        "Skipped L2 cache message for key {} with type {} and version {}",
                        key.toKeyString(),
                        message.getType(),
                        message.getVersion()
                );
                return false;
            }
        } catch (Exception e) {
            recordL2ApplyFailure(publish);
            throw e;
        }
    }

    private void recordL2ApplyAccepted(boolean publish) {
        if (publish) {
            runtimeMetrics.recordL2MutationApplyAccepted();
        } else {
            runtimeMetrics.recordL2ReadApplyAccepted();
        }
    }

    private void recordL2ApplyRejected(boolean publish) {
        if (publish) {
            runtimeMetrics.recordL2MutationApplyRejected();
        } else {
            runtimeMetrics.recordL2ReadApplyRejected();
        }
    }

    private void recordL2ApplyFailure(boolean publish) {
        if (publish) {
            runtimeMetrics.recordL2MutationApplyFailure();
        } else {
            runtimeMetrics.recordL2ReadApplyFailure();
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

    private void handleIncomingMutation(CacheMessage<?> message) {
        runtimeMetrics.recordPubSubMessageReceived();
        validateCacheMessage(message);
        applyRemoteMutationToLocalL1(message);
    }

    private void applyAcceptedMutationToLocalL1(CacheMessage<?> incomingMessage) {
        convergeLocalL1(incomingMessage, true);
    }

    private void applyRemoteMutationToLocalL1(CacheMessage<?> incomingMessage) {
        convergeLocalL1(incomingMessage, false);
    }

    private void convergeLocalL1(CacheMessage<?> incomingMessage, boolean writeWhenAbsent) {
        if (!isL1Enabled()) {
            return;
        }
        CacheKey businessKey = incomingMessage::getKey;
        l1Provider.compute(businessKey, (key, cachedValue) -> {
            if (cachedValue == null) {
                if (writeWhenAbsent) {
                    runtimeMetrics.recordL1InvalidationApplied();
                    return incomingMessage;
                }
                runtimeMetrics.recordL1InvalidationSkipped();
                return null;
            }
            if (!(cachedValue instanceof CacheMessage<?> localMessage)) {
                runtimeMetrics.recordL1InvalidationApplied();
                return writeWhenAbsent ? incomingMessage : null;
            }
            if (CacheMessageVersionComparator.shouldInvalidate(localMessage, incomingMessage)) {
                runtimeMetrics.recordL1InvalidationApplied();
                // Local accepted mutations should materialize the winning state in L1, including delete tombstones.
                // Remote Pub/Sub is only an invalidation accelerator; leave L1 empty so the next read converges from L2.
                return writeWhenAbsent ? incomingMessage : null;
            }
            runtimeMetrics.recordL1InvalidationSkipped();
            return localMessage;
        });
    }

    private void cacheL1IfNewer(CacheKey businessKey, CacheMessage<?> incomingMessage) {
        if (!isL1Enabled()) {
            return;
        }
        l1Provider.compute(businessKey, (key, cachedValue) -> {
            if (!(cachedValue instanceof CacheMessage<?> localMessage)) {
                runtimeMetrics.recordL1BackfillApplied();
                return incomingMessage;
            }
            // L1 writes are also ordered by version/type so an older L2 hit, backfill, or
            // penetration hint cannot overwrite a newer local state.
            if (CacheMessageVersionComparator.shouldReplace(incomingMessage, localMessage)) {
                runtimeMetrics.recordL1BackfillApplied();
                return incomingMessage;
            }
            runtimeMetrics.recordL1BackfillSkipped();
            return localMessage;
        });
    }

}
