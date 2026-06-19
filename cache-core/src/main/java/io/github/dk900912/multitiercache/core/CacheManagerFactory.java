package io.github.dk900912.multitiercache.core;

import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.CacheManager;
import io.github.dk900912.multitiercache.api.CacheMessageListener;
import io.github.dk900912.multitiercache.api.CacheMessageSubscription;
import io.github.dk900912.multitiercache.api.LifecycleManager;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.spi.CacheCodec;
import io.github.dk900912.multitiercache.spi.L1Provider;
import io.github.dk900912.multitiercache.spi.L2PubSubMode;
import io.github.dk900912.multitiercache.spi.L2Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;


/**
 * Factory for creating and configuring CacheManager instances.
 * 
 * <p>Thread-safety: This class is thread-safe.</p>
 * 
 * @see io.github.dk900912.multitiercache.api.CacheManager
 * @see DefaultCacheManager
 * @author dukui
 */
public class CacheManagerFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(CacheManagerFactory.class);

    public static CacheManager create(CacheConfig cacheConfig) {
        return create(cacheConfig, new ServiceLoaderProviderLoader(), () -> Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("cache-single-flight-", 1).factory()));
    }

    static CacheManager create(CacheConfig cacheConfig,
                               ProviderLoader providerLoader,
                               Supplier<ExecutorService> executorFactory) {
        CacheConfigValidator.validateBase(cacheConfig);

        L1Provider l1Provider = null;
        L2Provider l2Provider = null;
        CacheCodec cacheCodec = null;
        ExecutorService singleFlightExecutor = null;
        DefaultCacheManager cacheManager = null;
        try {
            l1Provider = createL1Provider(cacheConfig, providerLoader);
            l2Provider = createL2Provider(cacheConfig, providerLoader);
            cacheCodec = createCacheCodec(cacheConfig, providerLoader);

            singleFlightExecutor = executorFactory.get();
            SingleFlight singleFlight = new SingleFlight(singleFlightExecutor);
            LifecycleManager singleFlightExecutorLifecycle =
                    new ExecutorLifecycleManager(singleFlightExecutor, cacheConfig.getOriginLoadLimiter().getLocalLoadWaitTimeout());
            CacheRuntimeMetricsRecorder runtimeMetrics = new CacheRuntimeMetricsRecorder();

            cacheManager = new DefaultCacheManager(
                    cacheConfig, l1Provider, l2Provider, cacheCodec, singleFlight, runtimeMetrics);

            return new LifecycleAwareCacheManager(cacheManager, singleFlightExecutorLifecycle);
        } catch (RuntimeException | Error failure) {
            if (cacheManager != null) {
                shutdownCacheManagerAfterCreationFailure(cacheManager, failure);
                shutdownExecutorAfterCreationFailure(singleFlightExecutor, failure);
            } else {
                shutdownExecutorAfterCreationFailure(singleFlightExecutor, failure);
                closeAfterCreationFailure(cacheCodec, failure);
                closeAfterCreationFailure(l2Provider, failure);
                closeAfterCreationFailure(l1Provider, failure);
            }
            throw failure;
        }
    }

    private static L1Provider createL1Provider(CacheConfig cacheConfig, ProviderLoader providerLoader) {
        CacheConfig.L1Config l1Config = cacheConfig.getL1();
        if (l1Config == null || !l1Config.isEnabled()) {
            LOGGER.info("L1 cache is disabled.");
            return new NoopL1Provider();
        }

        List<L1Provider> providers = providerLoader.load(L1Provider.class);
        if (providers.isEmpty()) {
            throw new IllegalStateException("No L1Provider was loaded through SPI and L1 cache is enabled");
        }

        L1Provider provider = selectL1Provider(providers, l1Config.getProvider());
        CacheConfigValidator.validateResolvedL1Provider(cacheConfig, provider);
        initializeL1Provider(provider, l1Config);
        LOGGER.info("Selected L1Provider: {}", provider.getClass().getName());
        return provider;
    }

    private static L2Provider createL2Provider(CacheConfig cacheConfig, ProviderLoader providerLoader) {
        CacheConfig.L2Config l2Config = cacheConfig.getL2();
        if (l2Config == null || !l2Config.isEnabled()) {
            LOGGER.info("L2 cache is disabled.");
            return new NoopL2Provider();
        }

        List<L2Provider> providers = providerLoader.load(L2Provider.class);
        if (providers.isEmpty()) {
            throw new IllegalStateException("No L2Provider was loaded through SPI and L2 cache is enabled");
        }

        L2Provider provider = selectL2Provider(providers, l2Config.getProvider());
        CacheConfigValidator.validateResolvedL2Provider(cacheConfig, provider);
        initializeL2Provider(provider, l2Config);
        LOGGER.info("Selected L2Provider: {}", provider.getClass().getName());
        return provider;
    }

    private static CacheCodec createCacheCodec(CacheConfig cacheConfig, ProviderLoader providerLoader) {
        List<CacheCodec> codecs = providerLoader.load(CacheCodec.class);
        if (codecs.isEmpty()) {
            throw new IllegalStateException("No CacheCodec was loaded through SPI");
        }
        if (codecs.size() > 1) {
            throw new IllegalStateException("Multiple CacheCodec implementations were loaded through SPI: "
                    + codecs.stream().map(c -> c.getClass().getName()).toList());
        }
        CacheCodec codec = codecs.getFirst();
        try {
            codec.initialize(cacheConfig);
        } catch (Exception e) {
            IllegalStateException failure =
                    new IllegalStateException("Failed to initialize CacheCodec: " + codec.getClass().getName(), e);
            closeAfterCreationFailure(codec, failure);
            throw failure;
        }
        LOGGER.info("Selected CacheCodec: {}", codec.getClass().getName());
        return codec;
    }

    private static void initializeL1Provider(L1Provider provider, CacheConfig.L1Config config) {
        try {
            provider.initialize(config);
        } catch (Exception e) {
            IllegalStateException failure =
                    new IllegalStateException("Failed to initialize L1Provider: " + provider.getClass().getName(), e);
            closeAfterCreationFailure(provider, failure);
            throw failure;
        }
    }

    private static void initializeL2Provider(L2Provider provider, CacheConfig.L2Config config) {
        try {
            provider.initialize(config);
        } catch (Exception e) {
            IllegalStateException failure =
                    new IllegalStateException("Failed to initialize L2Provider: " + provider.getClass().getName(), e);
            closeAfterCreationFailure(provider, failure);
            throw failure;
        }
    }

    private static void closeAfterCreationFailure(Object resource, Throwable creationFailure) {
        if (!(resource instanceof AutoCloseable closeable)) {
            return;
        }
        try {
            closeable.close();
        } catch (Throwable closeFailure) {
            if (closeFailure != creationFailure) {
                creationFailure.addSuppressed(closeFailure);
            }
        }
    }

    private static void shutdownExecutorAfterCreationFailure(
            ExecutorService executor,
            Throwable creationFailure) {
        if (executor == null) {
            return;
        }
        try {
            executor.shutdownNow();
        } catch (Throwable shutdownFailure) {
            if (shutdownFailure != creationFailure) {
                creationFailure.addSuppressed(shutdownFailure);
            }
        }
    }

    private static void shutdownCacheManagerAfterCreationFailure(
            CacheManager cacheManager,
            Throwable creationFailure) {
        try {
            cacheManager.shutdown();
        } catch (Throwable shutdownFailure) {
            if (shutdownFailure != creationFailure) {
                creationFailure.addSuppressed(shutdownFailure);
            }
        }
    }

    private static <T> List<T> loadProviders(Class<T> providerType) {
        List<T> providers = new ArrayList<>();
        ServiceLoader.load(providerType).forEach(providers::add);
        return providers;
    }

    private static L1Provider selectL1Provider(List<L1Provider> providers, CacheConfig.L1ProviderType configuredProvider) {
        if (configuredProvider != CacheConfig.L1ProviderType.AUTO) {
            return providers.stream()
                    .filter(provider -> provider.providerType() == configuredProvider)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException(
                            "Configured L1 provider " + configuredProvider + " was not loaded. Available providers: "
                                    + providers.stream().map(CacheManagerFactory::describeL1Provider).toList()));
        }
        return providers.stream()
                .min(Comparator
                        .comparingInt(CacheManagerFactory::l1Priority)
                        .thenComparing(p -> p.getClass().getName()))
                .orElseThrow();
    }

    private static L2Provider selectL2Provider(List<L2Provider> providers, CacheConfig.L2ProviderType configuredProvider) {
        if (configuredProvider != CacheConfig.L2ProviderType.AUTO) {
            return providers.stream()
                    .filter(provider -> provider.providerType() == configuredProvider)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException(
                            "Configured L2 provider " + configuredProvider + " was not loaded. Available providers: "
                                    + providers.stream().map(CacheManagerFactory::describeL2Provider).toList()));
        }
        return providers.stream()
                .min(Comparator
                        .comparingInt(CacheManagerFactory::l2Priority)
                        .thenComparing(p -> p.getClass().getName()))
                .orElseThrow();
    }

    private static int l1Priority(L1Provider provider) {
        return switch (provider.providerType()) {
            case CAFFEINE -> 0;
            case GUAVA -> 1;
            case JDK -> 2;
            case AUTO -> 100;
        };
    }

    private static int l2Priority(L2Provider provider) {
        return switch (provider.providerType()) {
            case LETTUCE -> 0;
            case REDISSON -> 1;
            case JEDIS -> 2;
            case AUTO -> 100;
        };
    }

    private static String describeL1Provider(L1Provider provider) {
        return provider.providerType() + ":" + provider.getClass().getName();
    }

    private static String describeL2Provider(L2Provider provider) {
        return provider.providerType() + ":" + provider.getClass().getName();
    }

    private static class NoopL1Provider implements L1Provider {
        @Override
        public Object get(CacheKey key) {
            return null;
        }

        @Override
        public void put(CacheKey key, Object value) {
        }

        @Override
        public void invalidate(CacheKey key) {
        }

        @Override
        public void clear() {
        }
    }

    private static class NoopL2Provider implements L2Provider {
        @Override
        public void initialize(CacheConfig.L2Config config) {
        }

        @Override
        public String get(CacheKey key) {
            throw new UnsupportedOperationException("L2 cache is disabled");
        }

        @Override
        public void set(CacheKey key, String value, Duration ttl) {
            throw new UnsupportedOperationException("L2 cache is disabled");
        }

        @Override
        public void delete(CacheKey key) {
            throw new UnsupportedOperationException("L2 cache is disabled");
        }

        @Override
        public void publish(String channel, String message, L2PubSubMode mode) {
            throw new UnsupportedOperationException("L2 cache is disabled");
        }

        @Override
        public CacheMessageSubscription subscribe(String channel, CacheMessageListener listener, L2PubSubMode mode) {
            throw new UnsupportedOperationException("L2 cache is disabled");
        }

        @Override
        public Object eval(String script, List<String> keys, List<String> args) {
            throw new UnsupportedOperationException("L2 cache is disabled");
        }
    }

    interface ProviderLoader {
        <T> List<T> load(Class<T> providerType);
    }

    private static final class ServiceLoaderProviderLoader implements ProviderLoader {
        @Override
        public <T> List<T> load(Class<T> providerType) {
            return loadProviders(providerType);
        }
    }

    private static final class ExecutorLifecycleManager implements LifecycleManager {

        private final ExecutorService executor;
        private final Duration shutdownTimeout;

        private ExecutorLifecycleManager(ExecutorService executor, Duration shutdownTimeout) {
            this.executor = executor;
            this.shutdownTimeout = shutdownTimeout;
        }

        @Override
        public void bootstrap() {
            // Executor is ready when constructed.
        }

        @Override
        public void shutdown() {
            executor.shutdownNow();
            try {
                if (!executor.awaitTermination(shutdownTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
                    LOGGER.warn("SingleFlight executor did not terminate within {} ms", shutdownTimeout.toMillis());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while shutting down SingleFlight executor", e);
            }
        }
    }
}
