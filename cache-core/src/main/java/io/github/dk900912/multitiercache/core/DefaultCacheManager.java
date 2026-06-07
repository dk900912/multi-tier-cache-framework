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

    @Override
    public void apply(CacheMessage<?> message) {
        Objects.requireNonNull(message, "CacheMessage cannot be null");
        CacheMessage<?> preparedMessage = prepareMutationMessage(message);
        validateCacheMessage(preparedMessage);

        if (!isL2Enabled()) {
            invalidateLocalL1IfStale(preparedMessage);
            return;
        }

        CacheKey key = preparedMessage::getKey;
        String payload = cacheCodec.encode(preparedMessage);
        try {
            // L2 must become the authority first; otherwise a concurrent local read can miss L1,
            // observe stale L2, and rehydrate the current node with old data.
            boolean applied = applyMessageToL2(key, preparedMessage, payload, true);
            if (applied) {
                invalidateLocalL1IfStale(preparedMessage);
            }
        } catch (Exception e) {
            try {
                cacheMessageRepository.save(preparedMessage);
                runtimeMetrics.recordCompensationSaveSuccess();
                LOGGER.warn("L2 propagation failed, message saved for compensation replay", e);
            } catch (Exception cause) {
                runtimeMetrics.recordCompensationSaveFailure();
                LOGGER.error("Failed to save message for compensation - data may be lost", cause);
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
        String payload = l2Provider.get(CacheKeyspace.dataKey(key));
        if (payload == null) {
            runtimeMetrics.recordL2Miss();
            if (!quiet && LOGGER.isDebugEnabled()) {
                LOGGER.debug("L2 cache miss for key {}", key.toKeyString());
            }
            return null;
        }
        runtimeMetrics.recordL2Hit();
        CacheMessage<Object> message = cacheCodec.decodeMessage(payload, Object.class);
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
            message = new CacheMessage<>(key.toKeyString(), null, 0L, loadResult.getVersion(), PENETRATE, toTtlMillis(penetrationTtl));
        } else {
            Duration backfillTtl = resolveBackfillTtl(loadResult.getTtl());
            Long generation = resolveGeneration(key.toKeyString(), BACKFILL);
            message = new CacheMessage<>(
                    key.toKeyString(),
                    loadResult.getData(),
                    generation,
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
        if (isL2Enabled()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Writing read result to L2 for key {} with type {}", key.toKeyString(), cacheMessage.getType());
            }
            appliedToL2 = applyMessageToL2(key, cacheMessage, cacheCodec.encode(cacheMessage), false);
        }

        if (isL1Enabled()) {
            if (isL2Enabled() && !appliedToL2) {
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

    private CacheMessage<Object> createMutationMessage(CacheKey key, Object data, Long version, CacheMessageType type, Duration ttl) {
        Objects.requireNonNull(key, "CacheKey cannot be null");
        return new CacheMessage<>(key.toKeyString(), data, null, version, type, toTtlMillis(ttl));
    }

    private void validateCacheMessage(CacheMessage<?> message) {
        Objects.requireNonNull(message, "CacheMessage cannot be null");
        Objects.requireNonNull(message.getKey(), "CacheMessage's key cannot be null");
        Objects.requireNonNull(message.getGeneration(), "CacheMessage's generation cannot be null");
        Objects.requireNonNull(message.getVersion(), "CacheMessage's version cannot be null");
        Objects.requireNonNull(message.getType(), "CacheMessage's type cannot be null");
        Objects.requireNonNull(message.getTtlMillis(), "CacheMessage's ttl-millis cannot be null");

        switch (message.getType()) {
            case INSERT, UPDATE -> {
                if (message.getData() == null) {
                    throw new IllegalArgumentException("Inserting/Updating mutation must carry a payload data");
                }
                if (message.getGeneration() < 1) {
                    throw new IllegalArgumentException("Inserting/Updating mutation must carry an effective generation");
                }
                if (message.getVersion() < 0) {
                    throw new IllegalArgumentException("Inserting/Updating mutation must carry an effective version");
                }
            }
            case DELETE -> {
                if (message.getData() != null) {
                    throw new IllegalArgumentException("Deleting mutation must not carry a payload data");
                }
                if (message.getGeneration() < 1) {
                    throw new IllegalArgumentException("Deleting mutation must carry an effective generation");
                }
                if (message.getVersion() < 0) {
                    throw new IllegalArgumentException("Deleting mutation must carry an effective version");
                }
            }
            case PENETRATE -> {
                if (message.getData() != null) {
                    throw new IllegalArgumentException("Penetrating mutation must not carry a payload data");
                }
                if (message.getGeneration() != 0L) {
                    throw new IllegalArgumentException("Penetrating mutation must carry generation 0");
                }
                if (message.getVersion() != -1) {
                    throw new IllegalArgumentException("Penetrating mutation must carry an effective version");
                }
            }
            case BACKFILL -> {
                if (message.getData() == null) {
                    throw new IllegalArgumentException("Backfilling mutation must carry a payload data");
                }
                if (message.getGeneration() < 1) {
                    throw new IllegalArgumentException("Backfilling mutation must carry an effective generation");
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
                    String.valueOf(message.getGeneration()),
                    String.valueOf(message.getVersion()),
                    String.valueOf(cacheConfig.getL2().getMutationChannelName()),
                    publish ? "1" : "0"
            );
            Object rst = l2Provider.eval(CacheLuaScripts.APPLY_MESSAGE_LUA_SCRIPT, keys, args);
            if (SUCCESS.equals(rst)) {
                recordL2ApplyAccepted(publish);
                LOGGER.debug(
                        "Applied L2 cache message for key {} with type {} and generation/version {}/{}",
                        key.toKeyString(),
                        message.getType(),
                        message.getGeneration(),
                        message.getVersion()
                );
                return true;
            } else {
                recordL2ApplyRejected(publish);
                LOGGER.debug(
                        "Skipped L2 cache message for key {} with type {} and generation/version {}/{}",
                        key.toKeyString(),
                        message.getType(),
                        message.getGeneration(),
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

    private CacheMessage<?> prepareMutationMessage(CacheMessage<?> message) {
        if (message.getGeneration() != null) {
            return message;
        }
        // Insert/reinsert may advance the lifecycle generation even when the business version restarts
        // from a smaller value, so generation resolution must happen centrally before validation/apply.
        Long generation = resolveGeneration(message.getKey(), message.getType());
        return new CacheMessage<>(
                message.getKey(),
                message.getData(),
                generation,
                message.getVersion(),
                message.getType(),
                message.getTtlMillis()
        );
    }

    private void handleIncomingMutation(CacheMessage<?> message) {
        runtimeMetrics.recordPubSubMessageReceived();
        invalidateLocalL1IfStale(message);
    }

    private void invalidateLocalL1IfStale(CacheMessage<?> incomingMessage) {
        if (!isL1Enabled()) {
            return;
        }
        CacheKey businessKey = incomingMessage::getKey;
        // This path is shared by two callers:
        // 1) the local write path after L2 has already accepted the mutation;
        // 2) the Pub/Sub subscriber that reacts to remote mutations.
        //
        // In both cases we never delete L1 blindly. The local node may already hold a newer value
        // because of a racing backfill/reinsert, and subscriber messages may arrive duplicated or
        // out of order. Therefore, invalidation must be guarded by the same ordering rule used by L2.
        l1Provider.compute(businessKey, (key, cachedValue) -> {
            if (cachedValue == null) {
                runtimeMetrics.recordL1InvalidationSkipped();
                return null;
            }
            if (!(cachedValue instanceof CacheMessage<?> localMessage)) {
                runtimeMetrics.recordL1InvalidationSkipped();
                return null;
            }
            // compute(...) gives us a per-key atomic read-modify-write on L1. Without it, one thread
            // could read an older local value while another thread is backfilling a newer one, and a
            // stale invalidation decision would clobber the winner after the comparison is made.
            //
            // shouldInvalidate(...) mirrors the L2 generation/version ordering:
            // - higher generation always wins
            // - within the same generation, DELETE uses <= and UPSERT uses <
            // So "skip" here is an expected steady-state outcome for duplicate, delayed, or older
            // messages, not a failure.
            if (CacheMessageVersionComparator.shouldInvalidate(localMessage, incomingMessage)) {
                runtimeMetrics.recordL1InvalidationApplied();
                return null;
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
            // L1 writes are also ordered by generation/version so an older L2 hit, backfill, or
            // penetration hint cannot overwrite a newer local state.
            if (CacheMessageVersionComparator.shouldReplace(incomingMessage, localMessage)) {
                runtimeMetrics.recordL1BackfillApplied();
                return incomingMessage;
            }
            runtimeMetrics.recordL1BackfillSkipped();
            return localMessage;
        });
    }

    private Long resolveGeneration(String businessKey, CacheMessageType type) {
        if (type == PENETRATE) {
            return 0L;
        }
        if (!isL2Enabled()) {
            return 1L;
        }
        // Generation is resolved in L2 so all nodes observe the same lifecycle fence:
        // delete keeps the current generation, while insert/reinsert can atomically advance it.
        List<String> keys = List.of(
                CacheKeyspace.dataKey(businessKey),
                CacheKeyspace.generationKey(businessKey)
        );
        Object result = l2Provider.eval(CacheLuaScripts.RESOLVE_GENERATION_LUA_SCRIPT, keys, List.of(type.getWireValue()));
        if (result instanceof Number number) {
            return number.longValue();
        }
        return Long.parseLong(String.valueOf(result));
    }
}
