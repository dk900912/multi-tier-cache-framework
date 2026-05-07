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
        assertDoesNotThrow(() -> CacheConfigValidator.validateResolvedProviders(
                config, new CaffeineStyleL1Provider(), new NoopL2Provider()));
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

        assertThrows(IllegalArgumentException.class, () -> CacheConfigValidator.validateResolvedProviders(
                config, new JdkStyleL1Provider(), new NoopL2Provider()));
    }

    @Test
    void shouldFailWhenFineGrainedExpiryIsUsedWithoutCaffeine() {
        CacheConfig config = validConfig();
        config.getL1().setFineGrainedExpiry(new TestFineGrainedExpiry());

        assertThrows(IllegalArgumentException.class, () -> CacheConfigValidator.validateResolvedProviders(
                config, new GuavaStyleL1Provider(), new NoopL2Provider()));
    }

    private static CacheConfig validConfig() {
        CacheConfig config = new CacheConfig();
        config.getL2().setHosts(List.of("127.0.0.1:6379"));
        return config;
    }

    private static final class CaffeineStyleL1Provider implements L1Provider {
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

    private static final class NoopL2Provider implements L2Provider {
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
