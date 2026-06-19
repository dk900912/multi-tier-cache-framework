package io.github.dk900912.multitiercache.core;

import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.CacheLoader;
import io.github.dk900912.multitiercache.api.CacheManager;
import io.github.dk900912.multitiercache.api.CacheMessageDeliveryEvent;
import io.github.dk900912.multitiercache.api.CacheMessageListener;
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
import io.github.dk900912.multitiercache.spi.L2PubSubMode;
import io.github.dk900912.multitiercache.spi.L2Provider;
import io.github.dk900912.multitiercache.spi.L2ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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
    private static final long DISTRIBUTED_LOCK_WARNING_INTERVAL_NANOS = Duration.ofMinutes(1).toNanos();

    private final CacheConfig cacheConfig;
    private final L1Provider l1Provider;
    private final L2Provider l2Provider;

    private final AtomicReference<CacheMessageSubscription> subscription = new AtomicReference<>();
    private final AtomicLong nextDistributedLockWarningNanos = new AtomicLong();
    private final LifecycleStateMachine lifecycleStateMachine = new LifecycleStateMachine("CacheManager");

    private final CacheCodec cacheCodec;
    private final SingleFlight singleFlight;
    private final CacheRuntimeMetricsRecorder runtimeMetrics;
    private final ExecutorService l1RecoveryExecutor;
    private final ReentrantReadWriteLock l1AccessBarrier = new ReentrantReadWriteLock(true);
    private final Object l1TrustMonitor = new Object();
    private final EnumSet<L1DegradationReason> l1DegradationReasons =
            EnumSet.noneOf(L1DegradationReason.class);
    private volatile L1TrustState l1TrustState = L1TrustState.TRUSTED;
    private volatile boolean shuttingDown;
    private long activeProcessingOverloadEpisode = -1L;

    public DefaultCacheManager(CacheConfig cacheConfig,
                               L1Provider l1Provider,
                               L2Provider l2Provider,
                               CacheCodec cacheCodec,
                               SingleFlight singleFlight) {
        this(cacheConfig, l1Provider, l2Provider, cacheCodec, singleFlight, new CacheRuntimeMetricsRecorder());
    }

    DefaultCacheManager(CacheConfig cacheConfig,
                        L1Provider l1Provider,
                        L2Provider l2Provider,
                        CacheCodec cacheCodec,
                        SingleFlight singleFlight,
                        CacheRuntimeMetricsRecorder runtimeMetrics) {
        this.cacheConfig = Objects.requireNonNull(cacheConfig, "CacheConfig cannot be null");
        this.l1Provider = Objects.requireNonNull(l1Provider, "L1Provider cannot be null");
        this.l2Provider = Objects.requireNonNull(l2Provider, "L2Provider cannot be null");
        this.cacheCodec = Objects.requireNonNull(cacheCodec, "CacheCodec cannot be null");
        this.singleFlight = Objects.requireNonNull(singleFlight, "SingleFlight cannot be null");
        this.runtimeMetrics = Objects.requireNonNull(runtimeMetrics, "CacheRuntimeMetricsRecorder cannot be null");
        this.l1RecoveryExecutor = Executors.newSingleThreadExecutor(
                Thread.ofPlatform().daemon().name("cache-l1-recovery-", 1).factory());
    }

    @Override
    public <T> T get(CacheKey key, Supplier<T> loader) {
        return get(key, loader, cacheConfig.getLoadPolicy().getDefaultTtl());
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
    public void apply(CacheMessage<?> message) {
        Objects.requireNonNull(message, "CacheMessage cannot be null");
        validateCacheMessage(message);

        if (!isL2Enabled()) {
            applyAcceptedMutationToLocalL1(message);
            return;
        }

        CacheKey key = message::getKey;
        String payload = cacheCodec.encode(message);
        // L2 must become the authority first; otherwise a concurrent local read can miss L1,
        // observe stale L2, and rehydrate the current node with old data.
        boolean applied;
        try {
            applied = applyMessageToL2(key, message, payload, true);
        } catch (RuntimeException e) {
            invalidateLocalL1AfterL2ApplyFailure(key, e);
            throw e;
        }
        if (applied) {
            applyAcceptedMutationToLocalL1(message);
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
                    new CacheMessageListener() {
                        @Override
                        public void onMessage(String receivedChannel, String payload) {
                            try {
                                CacheMessage<?> message = cacheCodec.decodeMessage(payload, Object.class);
                                handleIncomingMutation(message);
                            } catch (Exception e) {
                                LOGGER.warn("Ignoring malformed L1 invalidation message from channel {}", receivedChannel, e);
                            }
                        }

                        @Override
                        public void onDeliveryEvent(CacheMessageDeliveryEvent event) {
                            handleDeliveryEvent(event);
                        }
                    },
                    L2PubSubMode.STANDARD);

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
        shutdownL1RecoveryExecutor();

        closeOwnedResource(cacheCodec, "CacheCodec");
        closeOwnedResource(l2Provider, "L2Provider");
        closeOwnedResource(l1Provider, "L1Provider");

    }

    private void closeOwnedResource(Object resource, String resourceName) {
        if (!(resource instanceof AutoCloseable closeable)) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception | LinkageError e) {
            LOGGER.warn("Failed to close {} during shutdown", resourceName, e);
        }
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

    private Object loadWithSingleFlight(CacheKey key, CacheLoader<?> loader) {
        Duration localLoadWaitTimeout = cacheConfig.getOriginLoadLimiter().getLocalLoadWaitTimeout();
        String keyString = key.toKeyString();
        return singleFlight.execute(keyString, localLoadWaitTimeout, () -> loadAsSingleFlightOwner(key, loader));
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

        if (cacheConfig.getOriginLoadLimiter().getOriginLoadLimitMode()
                == CacheConfig.OriginLoadLimitMode.GLOBAL) {
            return loadWithDistributedLock(key, loader);
        }

        return loadFromLoader(key, loader);
    }

    private Object loadWithDistributedLock(CacheKey key, CacheLoader<?> loader) {
        runtimeMetrics.recordDistributedLockAttemptCount();

        L2ReentrantLock lock;
        boolean acquired;
        try {
            lock = Objects.requireNonNull(
                    l2Provider.getLock(CacheKeyspace.loadLockKey(key)),
                    "L2Provider returned a null distributed lock");
            acquired = lock.tryLock(cacheConfig.getOriginLoadLimiter().getGlobalLoadWaitTimeout());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for distributed cache load lock", e);
        } catch (Exception e) {
            runtimeMetrics.recordDistributedLockFailureCount();
            logDistributedLockWarning("Failed to acquire distributed cache load lock", e);
            return handleDistributedLockFailure(key, loader, "Failed to acquire distributed cache load lock", e);
        }

        if (!acquired) {
            runtimeMetrics.recordDistributedLockTimeoutCount();
            CacheMessage<?> remoteRetry = readFromL2(key, true);
            if (remoteRetry != null) {
                return unwrapCacheMessage(remoteRetry);
            }
            return handleDistributedLockFailure(
                    key, loader, "Timed out while waiting for distributed cache load lock", null);
        }

        Throwable primaryFailure = null;
        try {
            CacheMessage<?> remoteRetry = readFromL2(key, true);
            if (remoteRetry != null) {
                return unwrapCacheMessage(remoteRetry);
            }
            return loadFromLoader(key, loader);
        } catch (RuntimeException | Error e) {
            primaryFailure = e;
            throw e;
        } finally {
            releaseDistributedLock(lock, primaryFailure);
        }
    }

    private Object handleDistributedLockFailure(CacheKey key,
                                                CacheLoader<?> loader,
                                                String message,
                                                Exception cause) {
        if (cacheConfig.getOriginLoadLimiter().getGlobalLoadFailurePolicy()
                == CacheConfig.GlobalLoadFailurePolicy.FAIL_OPEN) {
            runtimeMetrics.recordDistributedLockFailOpenLoadCount();
            return loadFromLoader(key, loader);
        }
        if (cause == null) {
            throw new IllegalStateException(message);
        }
        throw new IllegalStateException(message, cause);
    }

    private void releaseDistributedLock(L2ReentrantLock lock, Throwable primaryFailure) {
        try {
            lock.unlock();
        } catch (Exception e) {
            runtimeMetrics.recordDistributedLockFailureCount();
            if (primaryFailure != null) {
                primaryFailure.addSuppressed(e);
            }
            logDistributedLockWarning(
                    "Failed to release distributed cache load lock; Redis TTL will provide eventual release", e);
        }
    }

    private void logDistributedLockWarning(String message, Exception failure) {
        long now = System.nanoTime();
        long next = nextDistributedLockWarningNanos.get();
        if ((next == 0L || now - next >= 0L)
                && nextDistributedLockWarningNanos.compareAndSet(
                        next, now + DISTRIBUTED_LOCK_WARNING_INTERVAL_NANOS)) {
            LOGGER.warn(message, failure);
        }
    }

    private Object loadFromLoader(CacheKey key, CacheLoader<?> loader) {
        CacheLoadResult<?> loadResult = Objects.requireNonNull(loader.load(), "CacheLoadResult cannot be null");
        runtimeMetrics.recordOriginLoadCount(loadResult.isPenetration());
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
        if (!isL1Trusted()) {
            runtimeMetrics.recordL1UntrustedBypassCount();
            return null;
        }
        ReentrantReadWriteLock.ReadLock readLock = l1AccessBarrier.readLock();
        readLock.lock();
        try {
            if (!isL1Trusted()) {
                runtimeMetrics.recordL1UntrustedBypassCount();
                return null;
            }
            Object cached = l1Provider.get(key);
            if (cached == null) {
                runtimeMetrics.recordL1MissCount();
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
            runtimeMetrics.recordL1HitCount();
            return message;
        } finally {
            readLock.unlock();
        }
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
            runtimeMetrics.recordL2ReadPathFailureCount();
            logL2ReadFailure(key, quiet, "Failed to read L2 cache for key {}", e);
            return null;
        }
        if (payload == null) {
            runtimeMetrics.recordL2MissCount();
            if (!quiet && LOGGER.isDebugEnabled()) {
                LOGGER.debug("L2 cache miss for key {}", key.toKeyString());
            }
            return null;
        }
        runtimeMetrics.recordL2HitCount();
        CacheMessage<Object> message;
        try {
            message = cacheCodec.decodeMessage(payload, Object.class);
            validateCacheMessage(message);
        } catch (Exception e) {
            runtimeMetrics.recordL2ReadPathFailureCount();
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
        Long ttlMillis = Objects.requireNonNull(
                message.getTtlMillis(), "CacheMessage's ttl-millis cannot be null");
        if (ttlMillis <= 0) {
            throw new IllegalArgumentException("CacheMessage's ttl-millis must be positive");
        }

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
                LOGGER.debug(
                        "Applied L2 cache message for key {} with type {} and version {}",
                        key.toKeyString(),
                        message.getType(),
                        message.getVersion()
                );
                return true;
            } else {
                if (publish) {
                    runtimeMetrics.recordL2MutationRejectedCount();
                }
                LOGGER.debug(
                        "Skipped L2 cache message for key {} with type {} and version {}",
                        key.toKeyString(),
                        message.getType(),
                        message.getVersion()
                );
                return false;
            }
        } catch (Exception e) {
            if (publish) {
                runtimeMetrics.recordL2MutationFailureCount();
            } else {
                runtimeMetrics.recordL2ReadPathFailureCount();
            }
            throw e;
        }
    }

    private void invalidateLocalL1AfterL2ApplyFailure(CacheKey key, RuntimeException primaryFailure) {
        if (!isL1Enabled()) {
            return;
        }
        try {
            l1Provider.invalidate(key);
        } catch (RuntimeException l1Failure) {
            primaryFailure.addSuppressed(l1Failure);
            LOGGER.warn("Failed to invalidate local L1 for key {} after L2 mutation apply failure",
                    key.toKeyString(), l1Failure);
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
        return candidate != null ? candidate : cacheConfig.getLoadPolicy().getPenetrationTtl();
    }

    private Duration resolveBackfillTtl(Duration candidate) {
        return candidate != null ? candidate : cacheConfig.getLoadPolicy().getBackfillTtl();
    }

    private Duration resolveTtl(CacheMessage<?> message) {
        return message.getTtlMillis() == null ? null : Duration.ofMillis(message.getTtlMillis());
    }

    private Long toTtlMillis(Duration ttl) {
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

    private void handleIncomingMutation(CacheMessage<?> message) {
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
        if (!isL1Trusted()) {
            runtimeMetrics.recordL1UntrustedBypassCount();
            return;
        }
        ReentrantReadWriteLock.ReadLock readLock = l1AccessBarrier.readLock();
        readLock.lock();
        try {
            if (!isL1Trusted()) {
                runtimeMetrics.recordL1UntrustedBypassCount();
                return;
            }
            CacheKey businessKey = incomingMessage::getKey;
            l1Provider.compute(businessKey, (key, cachedValue) -> {
                if (cachedValue == null) {
                    if (writeWhenAbsent) {
                        return incomingMessage;
                    }
                    return null;
                }
                if (!(cachedValue instanceof CacheMessage<?> localMessage)) {
                    return writeWhenAbsent ? incomingMessage : null;
                }
                if (CacheMessageVersionComparator.shouldInvalidate(localMessage, incomingMessage)) {
                    // Local accepted mutations should materialize the winning state in L1, including delete tombstones.
                    // Remote Pub/Sub is only an invalidation accelerator; leave L1 empty so the next read converges from L2.
                    return writeWhenAbsent ? incomingMessage : null;
                }
                return localMessage;
            });
        } finally {
            readLock.unlock();
        }
    }

    private void cacheL1IfNewer(CacheKey businessKey, CacheMessage<?> incomingMessage) {
        if (!isL1Enabled()) {
            return;
        }
        if (!isL1Trusted()) {
            runtimeMetrics.recordL1UntrustedBypassCount();
            return;
        }
        ReentrantReadWriteLock.ReadLock readLock = l1AccessBarrier.readLock();
        readLock.lock();
        try {
            if (!isL1Trusted()) {
                runtimeMetrics.recordL1UntrustedBypassCount();
                return;
            }
            l1Provider.compute(businessKey, (key, cachedValue) -> {
                if (!(cachedValue instanceof CacheMessage<?> localMessage)) {
                    return incomingMessage;
                }
                // L1 writes are also ordered by version/type so an older L2 hit, backfill, or
                // penetration hint cannot overwrite a newer local state.
                if (CacheMessageVersionComparator.shouldReplace(incomingMessage, localMessage)) {
                    return incomingMessage;
                }
                return localMessage;
            });
        } finally {
            readLock.unlock();
        }
    }

    private void handleDeliveryEvent(CacheMessageDeliveryEvent event) {
        Objects.requireNonNull(event, "Delivery event cannot be null");
        switch (event.type()) {
            case PROCESSING_OVERLOADED -> {
                if (enterProcessingOverload(event.episodeId())) {
                    runtimeMetrics.recordPubSubDroppedMessageCount(event.droppedMessages());
                }
            }
            case PROCESSING_RECOVERED -> {
                runtimeMetrics.recordPubSubDroppedMessageCount(Math.max(0L, event.droppedMessages() - 1L));
                resolveProcessingOverload(event.episodeId());
            }
            case SUBSCRIPTION_INTERRUPTED -> {
                if (enterL1Degradation(L1DegradationReason.SUBSCRIPTION_INTERRUPTED)) {
                    runtimeMetrics.recordPubSubInterruptionCount();
                }
            }
            case SUBSCRIPTION_RESTORED ->
                    resolveL1Degradation(L1DegradationReason.SUBSCRIPTION_INTERRUPTED);
        }
    }

    private boolean enterL1Degradation(L1DegradationReason reason) {
        if (!isL1Enabled()) {
            return false;
        }
        synchronized (l1TrustMonitor) {
            boolean added = l1DegradationReasons.add(reason);
            if (added) {
                l1TrustState = L1TrustState.UNTRUSTED;
            }
            return added;
        }
    }

    private boolean enterProcessingOverload(long episodeId) {
        if (!isL1Enabled()) {
            return false;
        }
        synchronized (l1TrustMonitor) {
            if (episodeId <= activeProcessingOverloadEpisode) {
                return false;
            }
            activeProcessingOverloadEpisode = episodeId;
            l1DegradationReasons.add(L1DegradationReason.PROCESSING_OVERLOADED);
            l1TrustState = L1TrustState.UNTRUSTED;
            return true;
        }
    }

    private void resolveProcessingOverload(long episodeId) {
        boolean recover = false;
        synchronized (l1TrustMonitor) {
            if (episodeId != activeProcessingOverloadEpisode) {
                return;
            }
            l1DegradationReasons.remove(L1DegradationReason.PROCESSING_OVERLOADED);
            if (l1DegradationReasons.isEmpty()
                    && l1TrustState == L1TrustState.UNTRUSTED
                    && !shuttingDown) {
                l1TrustState = L1TrustState.RECOVERING;
                recover = true;
            }
        }
        if (recover) {
            submitL1Recovery();
        }
    }

    private void resolveL1Degradation(L1DegradationReason reason) {
        if (!isL1Enabled()) {
            return;
        }
        boolean recover = false;
        synchronized (l1TrustMonitor) {
            if (!l1DegradationReasons.remove(reason)) {
                return;
            }
            if (l1DegradationReasons.isEmpty()
                    && l1TrustState == L1TrustState.UNTRUSTED
                    && !shuttingDown) {
                l1TrustState = L1TrustState.RECOVERING;
                recover = true;
            }
        }
        if (recover) {
            submitL1Recovery();
        }
    }

    private void submitL1Recovery() {
        try {
            l1RecoveryExecutor.execute(this::recoverL1);
        } catch (RejectedExecutionException e) {
            synchronized (l1TrustMonitor) {
                l1TrustState = L1TrustState.UNTRUSTED;
            }
            if (!shuttingDown) {
                LOGGER.error("Failed to schedule L1 fail-safe recovery; L1 remains untrusted", e);
            }
        }
    }

    private void recoverL1() {
        long retryMillis = 100L;
        while (!shuttingDown && !Thread.currentThread().isInterrupted()) {
            synchronized (l1TrustMonitor) {
                if (!l1DegradationReasons.isEmpty()) {
                    l1TrustState = L1TrustState.UNTRUSTED;
                    return;
                }
            }

            boolean cleared = false;
            ReentrantReadWriteLock.WriteLock writeLock = l1AccessBarrier.writeLock();
            writeLock.lock();
            try {
                synchronized (l1TrustMonitor) {
                    if (!l1DegradationReasons.isEmpty() || shuttingDown) {
                        l1TrustState = L1TrustState.UNTRUSTED;
                        return;
                    }
                }
                l1Provider.clear();
                cleared = true;
            } catch (RuntimeException e) {
                runtimeMetrics.recordL1RecoveryClearFailureCount();
                LOGGER.error("Failed to clear L1 during fail-safe recovery; retrying in {} ms", retryMillis, e);
            } finally {
                writeLock.unlock();
            }

            if (cleared) {
                synchronized (l1TrustMonitor) {
                    l1TrustState = l1DegradationReasons.isEmpty() && !shuttingDown
                            ? L1TrustState.TRUSTED
                            : L1TrustState.UNTRUSTED;
                }
                return;
            }

            try {
                TimeUnit.MILLISECONDS.sleep(retryMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            retryMillis = Math.min(retryMillis * 2L, 5_000L);
        }
    }

    private boolean isL1Trusted() {
        return l1TrustState == L1TrustState.TRUSTED;
    }

    private void shutdownL1RecoveryExecutor() {
        synchronized (l1TrustMonitor) {
            shuttingDown = true;
            l1TrustState = L1TrustState.UNTRUSTED;
        }
        l1RecoveryExecutor.shutdownNow();
        try {
            l1RecoveryExecutor.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Tracks whether local L1 entries can be trusted after remote invalidation delivery issues.
     */
    private enum L1TrustState {
        /**
         * L1 may be used for reads and conditional writes.
         */
        TRUSTED,

        /**
         * L1 may have missed invalidation messages, so all L1 reads and writes are bypassed.
         */
        UNTRUSTED,

        /**
         * All degradation reasons are cleared and a recovery task is clearing L1 before trust is restored.
         */
        RECOVERING
    }

    /**
     * Reasons that force L1 into an untrusted state until the provider reports recovery.
     */
    private enum L1DegradationReason {
        /**
         * Pub/Sub callback processing dropped messages because the local dispatcher was overloaded.
         */
        PROCESSING_OVERLOADED,

        /**
         * The Pub/Sub subscription was interrupted, so remote invalidation delivery may have a gap.
         */
        SUBSCRIPTION_INTERRUPTED
    }

}
