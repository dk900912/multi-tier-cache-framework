package io.github.dk900912.multitiercache.core;

import io.github.dk900912.multitiercache.api.CacheKey;
import io.github.dk900912.multitiercache.api.CacheMessageListener;
import io.github.dk900912.multitiercache.api.CacheMessageSubscription;
import io.github.dk900912.multitiercache.api.FineGrainedExpiry;
import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.spi.L1Provider;
import io.github.dk900912.multitiercache.spi.L2Provider;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CacheConfigValidatorTest {

    @Test
    void shouldAcceptValidConfiguration() {
        CacheConfig config = validConfig();

        assertDoesNotThrow(() -> CacheConfigValidator.validateBase(config));
        assertDoesNotThrow(() -> CacheConfigValidator.validateResolvedL1Provider(
                config, new CaffeineStyleL1Provider()));
        assertDoesNotThrow(() -> CacheConfigValidator.validateResolvedL2Provider(
                config, new NoopL2Provider()));
    }

    @Test
    void shouldFailWhenL2HostsAreEmpty() {
        CacheConfig config = validConfig();
        config.getL2().setHosts(List.of());

        assertThrows(IllegalArgumentException.class, () -> CacheConfigValidator.validateBase(config));
    }

    @Test
    void shouldFailWhenMutationChannelIsBlank() {
        CacheConfig config = validConfig();
        config.getL2().setMutationChannelName("  ");

        assertThrows(IllegalArgumentException.class, () -> CacheConfigValidator.validateBase(config));
    }

    @Test
    void shouldFailWhenSubscriberPoolBoundsAreInvalid() {
        CacheConfig config = validConfig();
        config.getL2().getSubscriber().setCorePoolSize(4);
        config.getL2().getSubscriber().setMaximumPoolSize(3);

        assertThrows(IllegalArgumentException.class, () -> CacheConfigValidator.validateBase(config));
    }

    @Test
    void shouldFailWhenSingleFlightAwaitTimeoutIsNonPositive() {
        CacheConfig config = validConfig();
        config.getSingleFlight().setAwaitTimeout(Duration.ZERO);

        assertThrows(IllegalArgumentException.class, () -> CacheConfigValidator.validateBase(config));
    }

    @Test
    void shouldFailWhenCacheMissDefaultTtlIsNonPositive() {
        CacheConfig config = validConfig();
        config.getCacheMiss().setDefaultTtl(Duration.ZERO);

        assertThrows(IllegalArgumentException.class, () -> CacheConfigValidator.validateBase(config));
    }

    @Test
    void shouldFailWhenJdkProviderRequestsStats() {
        CacheConfig config = validConfig();
        config.getL1().setRecordStats(true);

        assertThrows(IllegalArgumentException.class, () -> CacheConfigValidator.validateResolvedL1Provider(
                config, new JdkStyleL1Provider()));
    }

    @Test
    void shouldFailWhenFineGrainedExpiryIsUsedWithoutCaffeine() {
        CacheConfig config = validConfig();
        config.getL1().setFineGrainedExpiry(new TestFineGrainedExpiry());

        assertThrows(IllegalArgumentException.class, () -> CacheConfigValidator.validateResolvedL1Provider(
                config, new GuavaStyleL1Provider()));
    }

    @Test
    void shouldFailWhenConfiguredL1ProviderDoesNotMatchResolvedProvider() {
        CacheConfig config = validConfig();
        config.getL1().setProvider(CacheConfig.L1ProviderType.CAFFEINE);

        assertThrows(IllegalArgumentException.class, () -> CacheConfigValidator.validateResolvedL1Provider(
                config, new GuavaStyleL1Provider()));
    }

    @Test
    void shouldFailWhenConfiguredL2ProviderDoesNotMatchResolvedProvider() {
        CacheConfig config = validConfig();
        config.getL2().setProvider(CacheConfig.L2ProviderType.REDISSON);

        assertThrows(IllegalArgumentException.class, () -> CacheConfigValidator.validateResolvedL2Provider(
                config, new JedisStyleL2Provider()));
    }

    @Test
    void shouldFailWhenLegacyL2PoolConfigIsUsedByNonJedisProvider() {
        CacheConfig config = validConfig();
        config.getL2().setMaxTotal(20);

        assertThrows(IllegalArgumentException.class, () -> CacheConfigValidator.validateResolvedL2Provider(
                config, new LettuceStyleL2Provider()));
    }

    @Test
    void shouldAllowLegacyL2PoolConfigForJedisProvider() {
        CacheConfig config = validConfig();
        config.getL2().setMaxTotal(20);

        assertDoesNotThrow(() -> CacheConfigValidator.validateResolvedL2Provider(
                config, new JedisStyleL2Provider()));
    }

    @Test
    void shouldFailWhenRedissonMinimumIdleExceedsPoolSize() {
        CacheConfig config = validConfig();
        config.getL2().getRedisson().setMasterConnectionPoolSize(2);
        config.getL2().getRedisson().setMasterConnectionMinimumIdleSize(3);

        assertThrows(IllegalArgumentException.class, () -> CacheConfigValidator.validateBase(config));
    }

    @Test
    void shouldFailWhenJedisPoolBoundsAreInvalid() {
        CacheConfig config = validConfig();
        config.getL2().getJedis().setMaxIdle(1);
        config.getL2().getJedis().setMinIdle(2);

        assertThrows(IllegalArgumentException.class, () -> CacheConfigValidator.validateBase(config));
    }

    private static CacheConfig validConfig() {
        CacheConfig config = new CacheConfig();
        config.getL2().setHosts(List.of("127.0.0.1:6379"));
        return config;
    }

    private static final class CaffeineStyleL1Provider implements L1Provider {
        @Override
        public CacheConfig.L1ProviderType providerType() {
            return CacheConfig.L1ProviderType.CAFFEINE;
        }

        @Override
        public boolean supportsRecordStats() {
            return true;
        }

        @Override
        public boolean supportsFineGrainedExpiry() {
            return true;
        }

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

    private static final class GuavaStyleL1Provider implements L1Provider {
        @Override
        public CacheConfig.L1ProviderType providerType() {
            return CacheConfig.L1ProviderType.GUAVA;
        }

        @Override
        public boolean supportsRecordStats() {
            return true;
        }

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

    private static final class JdkStyleL1Provider implements L1Provider {
        @Override
        public CacheConfig.L1ProviderType providerType() {
            return CacheConfig.L1ProviderType.JDK;
        }

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
            return null;
        }

        @Override
        public void set(CacheKey key, String value, Duration ttl) {
        }

        @Override
        public void delete(CacheKey key) {
        }

        @Override
        public void publish(String channel, String message) {
        }

        @Override
        public CacheMessageSubscription subscribe(String channel, CacheMessageListener listener) {
            return () -> {
            };
        }

        @Override
        public Object eval(String script, List<String> keys, List<String> args) {
            return null;
        }
    }

    private static final class JedisStyleL2Provider extends NoopL2Provider {
        @Override
        public CacheConfig.L2ProviderType providerType() {
            return CacheConfig.L2ProviderType.JEDIS;
        }
    }

    private static final class LettuceStyleL2Provider extends NoopL2Provider {
        @Override
        public CacheConfig.L2ProviderType providerType() {
            return CacheConfig.L2ProviderType.LETTUCE;
        }
    }

    private static final class TestFineGrainedExpiry implements FineGrainedExpiry<String, Object> {
        @Override
        public long expireAfterCreate(String key, Object value, long currentTimeNanos) {
            return 1L;
        }

        @Override
        public long expireAfterUpdate(String key, Object value, long currentTimeNanos, long currentDurationNanos) {
            return currentDurationNanos;
        }

        @Override
        public long expireAfterRead(String key, Object value, long currentTimeNanos, long currentDurationNanos) {
            return currentDurationNanos;
        }
    }
}
