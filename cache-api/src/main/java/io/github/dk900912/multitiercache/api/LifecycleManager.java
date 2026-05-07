package io.github.dk900912.multitiercache.api;

/**
 * Manages the lifecycle of cache components.
 * <p>
 * Provides hooks for initialization (bootstrap) and resource cleanup (shutdown).
 * </p>
 *
 * @author dukui
 */
public interface LifecycleManager {

    /**
     * Initializes the component and allocates necessary resources.
     */
    void bootstrap();

    /**
     * Shuts down the component and releases any held resources.
     */
    void shutdown();
}
