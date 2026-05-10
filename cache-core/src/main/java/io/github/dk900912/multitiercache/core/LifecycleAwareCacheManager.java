package io.github.dk900912.multitiercache.core;

import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.CacheLoader;
import io.github.dk900912.multitiercache.api.CacheManager;
import io.github.dk900912.multitiercache.api.CacheMonitor;
import io.github.dk900912.multitiercache.api.LifecycleManager;
import io.github.dk900912.multitiercache.api.model.CacheMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * Lifecycle-aware cache manager wrapper with rollback semantics.
 *
 * @author dukui
 */
final class LifecycleAwareCacheManager implements CacheManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(LifecycleAwareCacheManager.class);

    private final CacheManager delegate;
    private final LifecycleManager[] lifecycleManagers;
    private final LifecycleStateMachine lifecycleStateMachine = new LifecycleStateMachine("CacheManager");

    LifecycleAwareCacheManager(CacheManager delegate, LifecycleManager... lifecycleManagers) {
        this.delegate = delegate;
        this.lifecycleManagers = lifecycleManagers;
    }

    @Override
    public <T> T get(CacheKey key, Supplier<T> loader) {
        return delegate.get(key, loader);
    }

    @Override
    public <T> T get(CacheKey key, Supplier<T> loader, Duration ttl) {
        return delegate.get(key, loader, ttl);
    }

    @Override
    public <T> T get(CacheKey key, CacheLoader<T> loader) {
        return delegate.get(key, loader);
    }

    @Override
    public void insert(CacheKey key, Object data, Long version, Duration ttl) {
        delegate.insert(key, data, version, ttl);
    }

    @Override
    public void update(CacheKey key, Object data, Long version, Duration ttl) {
        delegate.update(key, data, version, ttl);
    }

    @Override
    public void evict(CacheKey key, Long version, Duration ttl) {
        delegate.evict(key, version, ttl);
    }

    @Override
    public CacheMonitor getMonitor() {
        return delegate.getMonitor();
    }

    @Override
    public void apply(CacheMessage<?> message) {
        delegate.apply(message);
    }

    @Override
    public void bootstrap() {
        if (!lifecycleStateMachine.beginBootstrap()) {
            return;
        }

        boolean delegateStarted = false;
        int startedLifecycleManagers = 0;
        try {
            delegate.bootstrap();
            delegateStarted = true;
            for (LifecycleManager lifecycleManager : lifecycleManagers) {
                lifecycleManager.bootstrap();
                startedLifecycleManagers++;
            }
            lifecycleStateMachine.markStarted();
        } catch (Exception e) {
            rollbackBootstrap(delegateStarted, startedLifecycleManagers, e);
            lifecycleStateMachine.markBootstrapFailed();
            throw e;
        }
    }

    @Override
    public void shutdown() {
        if (!lifecycleStateMachine.beginShutdown()) {
            return;
        }

        RuntimeException shutdownFailure = null;
        for (int i = lifecycleManagers.length - 1; i >= 0; i--) {
            shutdownFailure = shutdownLifecycleManager(lifecycleManagers[i], shutdownFailure);
        }
        shutdownFailure = shutdownDelegate(shutdownFailure);
        if (shutdownFailure != null) {
            throw shutdownFailure;
        }
    }

    private void rollbackBootstrap(boolean delegateStarted, int startedLifecycleManagers, Exception bootstrapFailure) {
        RuntimeException rollbackFailure = null;
        for (int i = startedLifecycleManagers - 1; i >= 0; i--) {
            rollbackFailure = shutdownLifecycleManager(lifecycleManagers[i], rollbackFailure);
        }
        if (delegateStarted) {
            rollbackFailure = shutdownDelegate(rollbackFailure);
        }
        if (rollbackFailure != null) {
            bootstrapFailure.addSuppressed(rollbackFailure);
        }
    }

    private RuntimeException shutdownLifecycleManager(LifecycleManager lifecycleManager, RuntimeException priorFailure) {
        try {
            lifecycleManager.shutdown();
            return priorFailure;
        } catch (Exception e) {
            LOGGER.warn("Failed to shut down lifecycle manager {}", lifecycleManager.getClass().getName(), e);
            return appendFailure(priorFailure, "Failed to shut down lifecycle manager " + lifecycleManager.getClass().getName(), e);
        }
    }

    private RuntimeException shutdownDelegate(RuntimeException priorFailure) {
        try {
            delegate.shutdown();
            return priorFailure;
        } catch (Exception e) {
            LOGGER.warn("Failed to shut down delegate cache manager", e);
            return appendFailure(priorFailure, "Failed to shut down delegate cache manager", e);
        }
    }

    private RuntimeException appendFailure(RuntimeException priorFailure, String message, Exception cause) {
        if (priorFailure == null) {
            return new IllegalStateException(message, cause);
        }
        priorFailure.addSuppressed(cause);
        return priorFailure;
    }
}
