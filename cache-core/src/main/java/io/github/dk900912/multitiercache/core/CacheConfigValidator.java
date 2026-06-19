package io.github.dk900912.multitiercache.core;

import io.github.dk900912.multitiercache.api.model.CacheConfig;
import io.github.dk900912.multitiercache.spi.L1Provider;
import io.github.dk900912.multitiercache.spi.L2Provider;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Centralized validator for framework configuration before runtime wiring begins.
 *
 * @author dukui
 */
final class CacheConfigValidator {

    private CacheConfigValidator() {
    }

    static void validateBase(CacheConfig cacheConfig) {
        Objects.requireNonNull(cacheConfig, "CacheConfig cannot be null");

        CacheConfig.L1Config l1 = Objects.requireNonNull(cacheConfig.getL1(), "L1 config cannot be null");
        CacheConfig.L2Config l2 = Objects.requireNonNull(cacheConfig.getL2(), "L2 config cannot be null");
        CacheConfig.OriginLoadLimiter singleFlight = Objects.requireNonNull(
                cacheConfig.getOriginLoadLimiter(), "OriginLoadLimiter config cannot be null");
        CacheConfig.LoadPolicy loadPolicy = Objects.requireNonNull(
                cacheConfig.getLoadPolicy(), "LoadPolicy config cannot be null");

        validateL1(l1);
        validateL2(l2);
        validateOriginLoadLimitMode(singleFlight, l2);
        validateCrossTierExpiry(l1, l2);
        validateSubscriber(Objects.requireNonNull(l2.getSubscriber(), "Subscriber config cannot be null"));
        validateSingleFlight(singleFlight);
        validateLoadPolicy(loadPolicy);
    }

    static void validateResolvedL1Provider(CacheConfig cacheConfig, L1Provider l1Provider) {
        Objects.requireNonNull(cacheConfig, "CacheConfig cannot be null");
        Objects.requireNonNull(l1Provider, "L1Provider cannot be null");

        CacheConfig.L1Config l1 = cacheConfig.getL1();
        if (l1 != null
                && l1.getProvider() != CacheConfig.L1ProviderType.AUTO
                && l1Provider.providerType() != l1.getProvider()) {
            throw new IllegalArgumentException("Selected L1 provider " + l1Provider.providerType()
                    + " does not match configured provider " + l1.getProvider());
        }
        if (l1 != null && l1.isRecordStats() && !l1Provider.supportsRecordStats()) {
            throw new IllegalArgumentException("Selected L1 provider does not support recordStats=true");
        }
        if (l1 != null && l1.getFineGrainedExpiry() != null && !l1Provider.supportsFineGrainedExpiry()) {
            throw new IllegalArgumentException("Selected L1 provider does not support fineGrainedExpiry");
        }
    }

    static void validateResolvedL2Provider(CacheConfig cacheConfig, L2Provider l2Provider) {
        Objects.requireNonNull(cacheConfig, "CacheConfig cannot be null");
        Objects.requireNonNull(l2Provider, "L2Provider cannot be null");

        CacheConfig.L2Config l2 = cacheConfig.getL2();
        if (l2 != null
                && l2.getProvider() != CacheConfig.L2ProviderType.AUTO
                && l2Provider.providerType() != l2.getProvider()) {
            throw new IllegalArgumentException("Selected L2 provider " + l2Provider.providerType()
                    + " does not match configured provider " + l2.getProvider());
        }
        if (cacheConfig.getOriginLoadLimiter().getOriginLoadLimitMode()
                == CacheConfig.OriginLoadLimitMode.GLOBAL
                && !l2Provider.supportsDistributedLock()) {
            throw new IllegalArgumentException("Selected L2 provider does not support distributed locks required by "
                    + "SingleFlight breakdownProtectionMode=GLOBAL");
        }
    }

    private static void validateL1(CacheConfig.L1Config l1) {
        Objects.requireNonNull(l1.getProvider(), "L1 provider cannot be null");
        requirePositiveIfPresent(l1.getMaximumSize(), "L1 maximumSize");
        requirePositiveDurationIfPresent(l1.getExpireAfterWrite(), "L1 expireAfterWrite");
        requirePositiveDurationIfPresent(l1.getExpireAfterAccess(), "L1 expireAfterAccess");
    }

    private static void validateL2(CacheConfig.L2Config l2) {
        if (!l2.isEnabled()) {
            return;
        }

        Objects.requireNonNull(l2.getProvider(), "L2 provider cannot be null");
        List<String> hosts = l2.getHosts();
        if (hosts == null || hosts.isEmpty()) {
            throw new IllegalArgumentException("L2 hosts cannot be empty when L2 cache is enabled");
        }
        for (String host : hosts) {
            requireNonBlank(host, "L2 host");
        }

        requireNonBlank(l2.getMutationChannelName(), "L2 mutationChannelName");
        requirePositiveDurationIfPresent(l2.getConnectionTimeout(), "L2 connectionTimeout");
        requirePositiveDurationIfPresent(l2.getSocketTimeout(), "L2 socketTimeout");
        requirePositiveMillisDuration(
                Objects.requireNonNull(l2.getLockWatchdogTimeout(), "L2 lockWatchdogTimeout cannot be null"),
                "L2 lockWatchdogTimeout");
        requireNonNegativeIfPresent(l2.getMaxRedirects(), "L2 maxRedirects");

        validateJedis(Objects.requireNonNull(l2.getJedis(), "Jedis config cannot be null"));
        validateRedisson(Objects.requireNonNull(l2.getRedisson(), "Redisson config cannot be null"));

        String username = trimToNull(l2.getUsername());
        String password = trimToNull(l2.getPassword());
        if (l2.getUsername() != null && username == null) {
            throw new IllegalArgumentException("L2 username must not be blank when provided");
        }
        if (l2.getPassword() != null && password == null) {
            throw new IllegalArgumentException("L2 password must not be blank when provided");
        }
        if (username != null && password == null) {
            throw new IllegalArgumentException("L2 password must be provided when username is set");
        }
    }

    private static void validateCrossTierExpiry(CacheConfig.L1Config l1, CacheConfig.L2Config l2) {
        if (l1.isEnabled() && l2.isEnabled() && l1.getExpireAfterWrite() == null) {
            throw new IllegalArgumentException(
                    "L1 expireAfterWrite must be configured when L1 and L2 are both enabled");
        }
    }

    private static void validateJedis(CacheConfig.Jedis jedis) {
        requirePositiveIfPresent(jedis.getMaxTotal(), "Jedis maxTotal");
        requireNonNegativeIfPresent(jedis.getMaxIdle(), "Jedis maxIdle");
        requireNonNegativeIfPresent(jedis.getMinIdle(), "Jedis minIdle");
        requirePositiveDurationIfPresent(jedis.getMaxWait(), "Jedis maxWait");
        validatePoolBounds(jedis.getMaxTotal(), jedis.getMaxIdle(), jedis.getMinIdle(), "Jedis");
    }

    private static void validateRedisson(CacheConfig.Redisson redisson) {
        requirePositiveIfPresent(redisson.getMasterConnectionPoolSize(), "Redisson masterConnectionPoolSize");
        requirePositiveIfPresent(redisson.getSlaveConnectionPoolSize(), "Redisson slaveConnectionPoolSize");
        requireNonNegativeIfPresent(redisson.getMasterConnectionMinimumIdleSize(), "Redisson masterConnectionMinimumIdleSize");
        requireNonNegativeIfPresent(redisson.getSlaveConnectionMinimumIdleSize(), "Redisson slaveConnectionMinimumIdleSize");
        if (redisson.getMasterConnectionPoolSize() != null
                && redisson.getMasterConnectionMinimumIdleSize() != null
                && redisson.getMasterConnectionMinimumIdleSize() > redisson.getMasterConnectionPoolSize()) {
            throw new IllegalArgumentException(
                    "Redisson masterConnectionMinimumIdleSize must be less than or equal to masterConnectionPoolSize");
        }
        if (redisson.getSlaveConnectionPoolSize() != null
                && redisson.getSlaveConnectionMinimumIdleSize() != null
                && redisson.getSlaveConnectionMinimumIdleSize() > redisson.getSlaveConnectionPoolSize()) {
            throw new IllegalArgumentException(
                    "Redisson slaveConnectionMinimumIdleSize must be less than or equal to slaveConnectionPoolSize");
        }
    }

    private static void validatePoolBounds(Integer maxTotal, Integer maxIdle, Integer minIdle, String prefix) {
        if (maxTotal != null && maxIdle != null && maxIdle > maxTotal) {
            throw new IllegalArgumentException(prefix + " maxIdle must be less than or equal to maxTotal");
        }
        if (maxIdle != null && minIdle != null && minIdle > maxIdle) {
            throw new IllegalArgumentException(prefix + " minIdle must be less than or equal to maxIdle");
        }
        if (maxTotal != null && minIdle != null && minIdle > maxTotal) {
            throw new IllegalArgumentException(prefix + " minIdle must be less than or equal to maxTotal");
        }
    }

    private static void validateSubscriber(CacheConfig.Subscriber subscriber) {
        if (subscriber.getCorePoolSize() < 1) {
            throw new IllegalArgumentException("Subscriber corePoolSize must be greater than or equal to 1");
        }
        if (subscriber.getMaximumPoolSize() < subscriber.getCorePoolSize()) {
            throw new IllegalArgumentException("Subscriber maximumPoolSize must be greater than or equal to corePoolSize");
        }
        Duration keepAliveTime = Objects.requireNonNull(
                subscriber.getKeepAliveTime(), "Subscriber keepAliveTime cannot be null");
        if (keepAliveTime.isNegative()) {
            throw new IllegalArgumentException("Subscriber keepAliveTime must be greater than or equal to 0");
        }
        if (subscriber.getCapacity() < 1) {
            throw new IllegalArgumentException("Subscriber capacity must be greater than or equal to 1");
        }
        double recoveryLowWatermarkRatio = subscriber.getRecoveryLowWatermarkRatio();
        if (!Double.isFinite(recoveryLowWatermarkRatio)
                || recoveryLowWatermarkRatio <= 0.0d
                || recoveryLowWatermarkRatio >= 1.0d) {
            throw new IllegalArgumentException(
                    "Subscriber recoveryLowWatermarkRatio must be greater than 0 and less than 1");
        }
        requirePositiveDuration(
                Objects.requireNonNull(
                        subscriber.getRecoveryQuietPeriod(),
                        "Subscriber recoveryQuietPeriod cannot be null"),
                "Subscriber recoveryQuietPeriod");
    }

    private static void validateSingleFlight(CacheConfig.OriginLoadLimiter singleFlight) {
        Duration localLoadWaitTimeout = Objects.requireNonNull(
                singleFlight.getLocalLoadWaitTimeout(), "OriginLoadLimiter localLoadWaitTimeout cannot be null");
        Duration globalLoadWaitTimeout = Objects.requireNonNull(
                singleFlight.getGlobalLoadWaitTimeout(),
                "OriginLoadLimiter globalLoadWaitTimeout cannot be null");
        Objects.requireNonNull(
                singleFlight.getOriginLoadLimitMode(),
                "SingleFlight breakdownProtectionMode cannot be null");
        Objects.requireNonNull(
                singleFlight.getGlobalLoadFailurePolicy(),
                "OriginLoadLimiter globalLoadFailurePolicy cannot be null");

        requirePositiveDuration(localLoadWaitTimeout, "OriginLoadLimiter localLoadWaitTimeout");
        requirePositiveDuration(globalLoadWaitTimeout, "OriginLoadLimiter globalLoadWaitTimeout");
        if (singleFlight.getOriginLoadLimitMode()
                == CacheConfig.OriginLoadLimitMode.GLOBAL
                && globalLoadWaitTimeout.compareTo(localLoadWaitTimeout) >= 0) {
            throw new IllegalArgumentException(
                    "OriginLoadLimiter globalLoadWaitTimeout must be less than localLoadWaitTimeout");
        }
    }

    private static void validateOriginLoadLimitMode(CacheConfig.OriginLoadLimiter singleFlight,
                                                        CacheConfig.L2Config l2) {
        if (singleFlight.getOriginLoadLimitMode()
                == CacheConfig.OriginLoadLimitMode.GLOBAL
                && !l2.isEnabled()) {
            throw new IllegalArgumentException(
                    "L2 cache must be enabled when SingleFlight breakdownProtectionMode=GLOBAL");
        }
    }

    private static void validateLoadPolicy(CacheConfig.LoadPolicy loadPolicy) {
        requirePositiveDuration(
                Objects.requireNonNull(loadPolicy.getPenetrationTtl(), "LoadPolicy penetrationTtl cannot be null"),
                "LoadPolicy penetrationTtl");
        requirePositiveDuration(
                Objects.requireNonNull(loadPolicy.getBackfillTtl(), "LoadPolicy backfillTtl cannot be null"),
                "LoadPolicy backfillTtl");
        requirePositiveDuration(
                Objects.requireNonNull(loadPolicy.getDefaultTtl(), "LoadPolicy defaultTtl cannot be null"),
                "LoadPolicy defaultTtl");
    }

    private static void requirePositiveIfPresent(Number value, String fieldName) {
        if (value != null && value.longValue() <= 0) {
            throw new IllegalArgumentException(fieldName + " must be greater than 0");
        }
    }

    private static void requireNonNegativeIfPresent(Number value, String fieldName) {
        if (value != null && value.longValue() < 0) {
            throw new IllegalArgumentException(fieldName + " must be greater than or equal to 0");
        }
    }

    private static void requirePositiveDurationIfPresent(Duration value, String fieldName) {
        if (value != null) {
            requirePositiveDuration(value, fieldName);
        }
    }

    private static void requirePositiveDuration(Duration value, String fieldName) {
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(fieldName + " must be greater than 0");
        }
    }

    private static void requirePositiveMillisDuration(Duration value, String fieldName) {
        if (value.toMillis() <= 0) {
            throw new IllegalArgumentException(fieldName + " must be greater than or equal to 1ms");
        }
    }

    private static void requireNonNegativeDuration(Duration value, String fieldName) {
        if (value.isNegative()) {
            throw new IllegalArgumentException(fieldName + " must be greater than or equal to 0");
        }
    }

    private static void requireNonBlank(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

}
