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
        CacheConfig.SingleFlight singleFlight = Objects.requireNonNull(
                cacheConfig.getSingleFlight(), "SingleFlight config cannot be null");
        CacheConfig.Compensation compensation = Objects.requireNonNull(
                cacheConfig.getCompensation(), "Compensation config cannot be null");
        CacheConfig.CacheMiss cacheMiss = Objects.requireNonNull(
                cacheConfig.getCacheMiss(), "CacheMiss config cannot be null");

        validateL1(l1);
        validateL2(l2);
        validateSubscriber(Objects.requireNonNull(l2.getSubscriber(), "Subscriber config cannot be null"));
        validateSingleFlight(singleFlight);
        validateCompensation(compensation);
        validateCacheMiss(cacheMiss);
    }

    static void validateResolvedProviders(CacheConfig cacheConfig, L1Provider l1Provider, L2Provider l2Provider) {
        Objects.requireNonNull(cacheConfig, "CacheConfig cannot be null");
        Objects.requireNonNull(l1Provider, "L1Provider cannot be null");
        Objects.requireNonNull(l2Provider, "L2Provider cannot be null");

        CacheConfig.L1Config l1 = cacheConfig.getL1();
        if (l1 != null && l1.isRecordStats() && isProvider(l1Provider, "jdk")) {
            throw new IllegalArgumentException("JDK L1 provider does not support recordStats=true");
        }
        if (l1 != null && l1.getFineGrainedExpiry() != null && !isProvider(l1Provider, "caffeine")) {
            throw new IllegalArgumentException("fineGrainedExpiry requires the Caffeine L1 provider");
        }
    }

    private static void validateL1(CacheConfig.L1Config l1) {
        requirePositiveIfPresent(l1.getMaximumSize(), "L1 maximumSize");
        requirePositiveDurationIfPresent(l1.getExpireAfterWrite(), "L1 expireAfterWrite");
        requirePositiveDurationIfPresent(l1.getExpireAfterAccess(), "L1 expireAfterAccess");
    }

    private static void validateL2(CacheConfig.L2Config l2) {
        if (!l2.isEnabled()) {
            return;
        }

        List<String> hosts = l2.getHosts();
        if (hosts == null || hosts.isEmpty()) {
            throw new IllegalArgumentException("L2 hosts cannot be empty when L2 cache is enabled");
        }
        for (String host : hosts) {
            requireNonBlank(host, "L2 host");
        }

        requireNonBlank(l2.getMutationChannelName(), "L2 mutationChannelName");
        requirePositiveIfPresent(l2.getMaxTotal(), "L2 maxTotal");
        requireNonNegativeIfPresent(l2.getMaxIdle(), "L2 maxIdle");
        requireNonNegativeIfPresent(l2.getMinIdle(), "L2 minIdle");
        requirePositiveDurationIfPresent(l2.getMaxWait(), "L2 maxWait");
        requirePositiveDurationIfPresent(l2.getConnectionTimeout(), "L2 connectionTimeout");
        requirePositiveDurationIfPresent(l2.getSocketTimeout(), "L2 socketTimeout");
        requireNonNegativeIfPresent(l2.getMaxRedirects(), "L2 maxRedirects");

        if (l2.getMaxTotal() != null && l2.getMaxIdle() != null && l2.getMaxIdle() > l2.getMaxTotal()) {
            throw new IllegalArgumentException("L2 maxIdle must be less than or equal to maxTotal");
        }
        if (l2.getMaxIdle() != null && l2.getMinIdle() != null && l2.getMinIdle() > l2.getMaxIdle()) {
            throw new IllegalArgumentException("L2 minIdle must be less than or equal to maxIdle");
        }
        if (l2.getMaxTotal() != null && l2.getMinIdle() != null && l2.getMinIdle() > l2.getMaxTotal()) {
            throw new IllegalArgumentException("L2 minIdle must be less than or equal to maxTotal");
        }

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
    }

    private static void validateSingleFlight(CacheConfig.SingleFlight singleFlight) {
        requirePositiveDuration(
                Objects.requireNonNull(singleFlight.getAwaitTimeout(), "SingleFlight awaitTimeout cannot be null"),
                "SingleFlight awaitTimeout");
    }

    private static void validateCompensation(CacheConfig.Compensation compensation) {
        requireNonNegativeDuration(
                Objects.requireNonNull(compensation.getInitialDelay(), "Compensation initialDelay cannot be null"),
                "Compensation initialDelay");
        requirePositiveDuration(
                Objects.requireNonNull(compensation.getPeriod(), "Compensation period cannot be null"),
                "Compensation period");
        if (compensation.getBatchSize() < 1) {
            throw new IllegalArgumentException("Compensation batchSize must be greater than or equal to 1");
        }
    }

    private static void validateCacheMiss(CacheConfig.CacheMiss cacheMiss) {
        requirePositiveDuration(
                Objects.requireNonNull(cacheMiss.getPenetrationTtl(), "CacheMiss penetrationTtl cannot be null"),
                "CacheMiss penetrationTtl");
        requirePositiveDuration(
                Objects.requireNonNull(cacheMiss.getBackfillTtl(), "CacheMiss backfillTtl cannot be null"),
                "CacheMiss backfillTtl");
        requirePositiveDuration(
                Objects.requireNonNull(cacheMiss.getDefaultTtl(), "CacheMiss defaultTtl cannot be null"),
                "CacheMiss defaultTtl");
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

    private static boolean isProvider(Object provider, String token) {
        return provider.getClass().getName().toLowerCase().contains(token);
    }
}
