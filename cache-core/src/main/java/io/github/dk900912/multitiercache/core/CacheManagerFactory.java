package io.github.dk900912.multitiercache.core;

import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.CacheManager;
import io.github.dk900912.multitiercache.api.CacheMessageListener;
import io.github.dk900912.multitiercache.api.CacheMessageRepository;
import io.github.dk900912.multitiercache.api.CacheMessageSubscription;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.spi.CacheCodec;
import io.github.dk900912.multitiercache.spi.L1Provider;
import io.github.dk900912.multitiercache.spi.L2Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;


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
        CacheConfigValidator.validateBase(cacheConfig);

        L1Provider l1Provider = createL1Provider(cacheConfig);
        L2Provider l2Provider = createL2Provider(cacheConfig);
        CacheMessageRepository cacheMessageRepository = createCacheMessageRepository();
        CacheCodec cacheCodec = createCacheCodec(cacheConfig);

        SingleFlight singleFlight = new SingleFlight();

        DefaultCacheManager cacheManager = new DefaultCacheManager(
                cacheConfig, l1Provider, l2Provider, cacheMessageRepository, cacheCodec, singleFlight);

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "cache-message-replayer");
            thread.setDaemon(true);
            return thread;
        });

        CacheMessageReplayer cacheMessageReplayer = new CacheMessageReplayer(
                cacheMessageRepository, cacheManager, cacheConfig, scheduler);

        return new LifecycleAwareCacheManager(cacheManager, cacheMessageReplayer);
    }

    private static L1Provider createL1Provider(CacheConfig cacheConfig) {
        CacheConfig.L1Config l1Config = cacheConfig.getL1();
        if (l1Config == null || !l1Config.isEnabled()) {
            LOGGER.info("L1 cache is disabled.");
            return new NoopL1Provider();
        }

        List<L1Provider> providers = loadProviders(L1Provider.class);
        if (providers.isEmpty()) {
            throw new IllegalStateException("No L1Provider was loaded through SPI and L1 cache is enabled");
        }

        L1Provider provider = providers.stream()
                .min(Comparator
                        .comparingInt(CacheManagerFactory::l1Priority)
                        .thenComparing(p -> p.getClass().getName()))
                .orElseThrow();
        CacheConfigValidator.validateResolvedL1Provider(cacheConfig, provider);
        initializeL1Provider(provider, l1Config);
        LOGGER.info("Selected L1Provider: {}", provider.getClass().getName());
        return provider;
    }

    private static L2Provider createL2Provider(CacheConfig cacheConfig) {
        CacheConfig.L2Config l2Config = cacheConfig.getL2();
        if (l2Config == null || !l2Config.isEnabled()) {
            LOGGER.info("L2 cache is disabled.");
            return new NoopL2Provider();
        }

        List<L2Provider> providers = loadProviders(L2Provider.class);
        if (providers.isEmpty()) {
            throw new IllegalStateException("No L2Provider was loaded through SPI and L2 cache is enabled");
        }

        L2Provider provider = providers.stream()
                .min(Comparator
                        .comparingInt(CacheManagerFactory::l2Priority)
                        .thenComparing(p -> p.getClass().getName()))
                .orElseThrow();
        CacheConfigValidator.validateResolvedL2Provider(cacheConfig, provider);
        initializeL2Provider(provider, l2Config);
        LOGGER.info("Selected L2Provider: {}", provider.getClass().getName());
        return provider;
    }

    private static CacheMessageRepository createCacheMessageRepository() {
        List<CacheMessageRepository> repositories = loadProviders(CacheMessageRepository.class);
        if (repositories.isEmpty()) {
            LOGGER.info("No CacheMessageRepository was loaded through SPI. Falling back to DefaultCacheMessageRepository.");
            return new DefaultCacheMessageRepository();
        }
        if (repositories.size() > 1) {
            throw new IllegalStateException("Multiple CacheMessageRepository implementations were loaded through SPI: "
                    + repositories.stream().map(r -> r.getClass().getName()).toList());
        }
        CacheMessageRepository repository = repositories.getFirst();
        LOGGER.info("Selected CacheMessageRepository: {}", repository.getClass().getName());
        return repository;
    }

    private static CacheCodec createCacheCodec(CacheConfig cacheConfig) {
        List<CacheCodec> codecs = loadProviders(CacheCodec.class);
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
            throw new IllegalStateException("Failed to initialize CacheCodec: " + codec.getClass().getName(), e);
        }
        LOGGER.info("Selected CacheCodec: {}", codec.getClass().getName());
        return codec;
    }

    private static void initializeL1Provider(L1Provider provider, CacheConfig.L1Config config) {
        try {
            provider.initialize(config);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to initialize L1Provider: " + provider.getClass().getName(), e);
        }
    }

    private static void initializeL2Provider(L2Provider provider, CacheConfig.L2Config config) {
        try {
            provider.initialize(config);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to initialize L2Provider: " + provider.getClass().getName(), e);
        }
    }

    private static <T> List<T> loadProviders(Class<T> providerType) {
        List<T> providers = new ArrayList<>();
        ServiceLoader.load(providerType).forEach(providers::add);
        return providers;
    }

    private static int l1Priority(L1Provider provider) {
        String className = provider.getClass().getName().toLowerCase();
        if (className.contains("caffeine")) {
            return 0;
        }
        if (className.contains("guava")) {
            return 1;
        }
        if (className.contains("jdk")) {
            return 2;
        }
        return 100;
    }

    private static int l2Priority(L2Provider provider) {
        String className = provider.getClass().getName().toLowerCase();
        if (className.contains("lettuce")) {
            return 0;
        }
        if (className.contains("redisson")) {
            return 1;
        }
        if (className.contains("jedis")) {
            return 2;
        }
        return 100;
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
        public void publish(String channel, String message) {
            throw new UnsupportedOperationException("L2 cache is disabled");
        }

        @Override
        public CacheMessageSubscription subscribe(String channel, CacheMessageListener listener) {
            throw new UnsupportedOperationException("L2 cache is disabled");
        }

        @Override
        public Object eval(String script, List<String> keys, List<String> args) {
            throw new UnsupportedOperationException("L2 cache is disabled");
        }
    }
}
