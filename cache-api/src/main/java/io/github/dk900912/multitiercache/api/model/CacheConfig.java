package io.github.dk900912.multitiercache.api.model;

import io.github.dk900912.multitiercache.api.FineGrainedExpiry;

import java.time.Duration;
import java.util.List;

/**
 * The configuration model for the multi-tier cache framework.
 * <p>
 * Contains nested configurations for L1 cache, L2 cache, SingleFlight protection,
 * local message compensation, and cache miss behavior.
 * </p>
 *
 * @author dukui
 */
public class CacheConfig {

    public enum L1ProviderType {
        AUTO,
        CAFFEINE,
        GUAVA,
        JDK
    }

    public enum L2ProviderType {
        AUTO,
        LETTUCE,
        REDISSON,
        JEDIS
    }

    private L1Config l1 = new L1Config();
    private L2Config l2 = new L2Config();
    private CodecConfig codec = new CodecConfig();
    private SingleFlight singleFlight = new SingleFlight();
    private Compensation compensation = new Compensation();
    private CacheMiss cacheMiss = new CacheMiss();

    public CacheConfig() {
    }

    public L1Config getL1() {
        return l1;
    }

    public void setL1(L1Config l1) {
        this.l1 = l1;
    }

    public L2Config getL2() {
        return l2;
    }

    public void setL2(L2Config l2) {
        this.l2 = l2;
    }

    public CodecConfig getCodec() {
        return codec;
    }

    public void setCodec(CodecConfig codec) {
        this.codec = codec;
    }

    public SingleFlight getSingleFlight() {
        return singleFlight;
    }

    public void setSingleFlight(SingleFlight singleFlight) {
        this.singleFlight = singleFlight;
    }

    public Compensation getCompensation() {
        return compensation;
    }

    public void setCompensation(Compensation compensation) {
        this.compensation = compensation;
    }

    public CacheMiss getCacheMiss() {
        return cacheMiss;
    }

    public void setCacheMiss(CacheMiss cacheMiss) {
        this.cacheMiss = cacheMiss;
    }

    /**
     * Configuration for Cache Codec.
     */
    public static class CodecConfig {
        /**
         * List of trusted package prefixes for deserialization white-listing.
         */
        private List<String> trustedPackages = new java.util.ArrayList<>();

        public List<String> getTrustedPackages() {
            return trustedPackages;
        }

        public void setTrustedPackages(List<String> trustedPackages) {
            this.trustedPackages = trustedPackages;
        }
    }

    /**
     * Configuration for the Level 1 (L1) local cache.
     */
    public static class L1Config {
        /**
         * Whether the L1 cache is enabled. Default is true.
         */
        private boolean enabled = true;

        /**
         * The L1 provider implementation to use. AUTO keeps the historical classpath-based priority.
         */
        private L1ProviderType provider = L1ProviderType.AUTO;

        /**
         * Whether to record L1 cache statistics.
         */
        private boolean recordStats = false;

        /**
         * The maximum number of entries the L1 cache can hold.
         */
        private Long maximumSize = 1000L;

        /**
         * The global duration after which an entry should expire since it was last written.
         */
        private Duration expireAfterWrite = Duration.ofMillis(15000);

        /**
         * The global duration after which an entry should expire since it was last accessed.
         */
        private Duration expireAfterAccess = Duration.ofMillis(15000);

        /**
         * The fine-grained expiry strategy.
         * <p>
         * Note: This fine-grained expiry strategy currently only takes effect when using Caffeine as the L1 provider.
         * </p>
         */
        private FineGrainedExpiry<String, Object> fineGrainedExpiry;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public L1ProviderType getProvider() {
            return provider;
        }

        public void setProvider(L1ProviderType provider) {
            this.provider = provider;
        }

        public boolean isRecordStats() {
            return recordStats;
        }

        public void setRecordStats(boolean recordStats) {
            this.recordStats = recordStats;
        }

        public Long getMaximumSize() {
            return maximumSize;
        }

        public void setMaximumSize(Long maximumSize) {
            this.maximumSize = maximumSize;
        }

        public Duration getExpireAfterWrite() {
            return expireAfterWrite;
        }

        public void setExpireAfterWrite(Duration expireAfterWrite) {
            this.expireAfterWrite = expireAfterWrite;
        }

        public Duration getExpireAfterAccess() {
            return expireAfterAccess;
        }

        public void setExpireAfterAccess(Duration expireAfterAccess) {
            this.expireAfterAccess = expireAfterAccess;
        }

        public FineGrainedExpiry<String, Object> getFineGrainedExpiry() {
            return fineGrainedExpiry;
        }

        public void setFineGrainedExpiry(FineGrainedExpiry<String, Object> fineGrainedExpiry) {
            this.fineGrainedExpiry = fineGrainedExpiry;
        }
    }

    /**
     * Configuration for the Level 2 (L2) distributed cache (e.g., Redis).
     */
    public static class L2Config {
        /**
         * Whether the L2 cache is enabled. Default is true.
         */
        private boolean enabled = true;

        /**
         * The L2 provider implementation to use. AUTO keeps the historical classpath-based priority.
         */
        private L2ProviderType provider = L2ProviderType.AUTO;

        /**
         * The Pub/Sub channel name used for broadcasting cache mutations.
         */
        private String mutationChannelName = "multi-tier-cache-mutation";

        /**
         * The list of Redis cluster node addresses (e.g., "127.0.0.1:6379").
         */
        private List<String> hosts;

        /**
         * The maximum number of active connections in the pool.
         */
        @Deprecated
        private Integer maxTotal;

        /**
         * The maximum number of idle connections in the pool.
         */
        @Deprecated
        private Integer maxIdle;

        /**
         * The minimum number of idle connections in the pool.
         */
        @Deprecated
        private Integer minIdle;

        /**
         * The maximum time to wait for a connection from the pool.
         */
        @Deprecated
        private Duration maxWait;

        /**
         * The connection timeout duration.
         */
        private Duration connectionTimeout = Duration.ofMillis(6000);

        /**
         * The socket read/write timeout duration.
         */
        private Duration socketTimeout = Duration.ofMillis(6000);

        /**
         * The maximum number of redirects to follow in the cluster.
         */
        private Integer maxRedirects = 5;

        /**
         * The username for Redis ACL authentication.
         */
        private String username;

        /**
         * The password for Redis authentication.
         */
        private String password;

        /**
         * Configuration for the Pub/Sub message processing thread pool.
         */
        private Subscriber subscriber = new Subscriber();

        /**
         * Jedis-specific connection pool settings.
         */
        private Jedis jedis = new Jedis();

        /**
         * Redisson-specific connection pool settings.
         */
        private Redisson redisson = new Redisson();

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public L2ProviderType getProvider() {
            return provider;
        }

        public void setProvider(L2ProviderType provider) {
            this.provider = provider;
        }

        public String getMutationChannelName() {
            return mutationChannelName;
        }

        public void setMutationChannelName(String mutationChannelName) {
            this.mutationChannelName = mutationChannelName;
        }

        public List<String> getHosts() {
            return hosts;
        }

        public void setHosts(List<String> hosts) {
            this.hosts = hosts;
        }

        public Integer getMaxTotal() {
            return maxTotal;
        }

        public void setMaxTotal(Integer maxTotal) {
            this.maxTotal = maxTotal;
        }

        public Integer getMaxIdle() {
            return maxIdle;
        }

        public void setMaxIdle(Integer maxIdle) {
            this.maxIdle = maxIdle;
        }

        public Integer getMinIdle() {
            return minIdle;
        }

        public void setMinIdle(Integer minIdle) {
            this.minIdle = minIdle;
        }

        public Duration getMaxWait() {
            return maxWait;
        }

        public void setMaxWait(Duration maxWait) {
            this.maxWait = maxWait;
        }

        public Duration getConnectionTimeout() {
            return connectionTimeout;
        }

        public void setConnectionTimeout(Duration connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
        }

        public Duration getSocketTimeout() {
            return socketTimeout;
        }

        public void setSocketTimeout(Duration socketTimeout) {
            this.socketTimeout = socketTimeout;
        }

        public Integer getMaxRedirects() {
            return maxRedirects;
        }

        public void setMaxRedirects(Integer maxRedirects) {
            this.maxRedirects = maxRedirects;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public Subscriber getSubscriber() {
            return subscriber;
        }

        public void setSubscriber(Subscriber subscriber) {
            this.subscriber = subscriber;
        }

        public Jedis getJedis() {
            return jedis;
        }

        public void setJedis(Jedis jedis) {
            this.jedis = jedis;
        }

        public Redisson getRedisson() {
            return redisson;
        }

        public void setRedisson(Redisson redisson) {
            this.redisson = redisson;
        }
    }

    /**
     * Jedis-specific L2 provider settings.
     */
    public static class Jedis {
        private Integer maxTotal = 10;
        private Integer maxIdle = 1;
        private Integer minIdle = 1;
        private Duration maxWait = Duration.ofMillis(6000);

        public Integer getMaxTotal() {
            return maxTotal;
        }

        public void setMaxTotal(Integer maxTotal) {
            this.maxTotal = maxTotal;
        }

        public Integer getMaxIdle() {
            return maxIdle;
        }

        public void setMaxIdle(Integer maxIdle) {
            this.maxIdle = maxIdle;
        }

        public Integer getMinIdle() {
            return minIdle;
        }

        public void setMinIdle(Integer minIdle) {
            this.minIdle = minIdle;
        }

        public Duration getMaxWait() {
            return maxWait;
        }

        public void setMaxWait(Duration maxWait) {
            this.maxWait = maxWait;
        }
    }

    /**
     * Redisson-specific L2 provider settings.
     */
    public static class Redisson {
        private Integer masterConnectionPoolSize = 10;
        private Integer slaveConnectionPoolSize = 10;
        private Integer masterConnectionMinimumIdleSize = 1;
        private Integer slaveConnectionMinimumIdleSize = 1;

        public Integer getMasterConnectionPoolSize() {
            return masterConnectionPoolSize;
        }

        public void setMasterConnectionPoolSize(Integer masterConnectionPoolSize) {
            this.masterConnectionPoolSize = masterConnectionPoolSize;
        }

        public Integer getSlaveConnectionPoolSize() {
            return slaveConnectionPoolSize;
        }

        public void setSlaveConnectionPoolSize(Integer slaveConnectionPoolSize) {
            this.slaveConnectionPoolSize = slaveConnectionPoolSize;
        }

        public Integer getMasterConnectionMinimumIdleSize() {
            return masterConnectionMinimumIdleSize;
        }

        public void setMasterConnectionMinimumIdleSize(Integer masterConnectionMinimumIdleSize) {
            this.masterConnectionMinimumIdleSize = masterConnectionMinimumIdleSize;
        }

        public Integer getSlaveConnectionMinimumIdleSize() {
            return slaveConnectionMinimumIdleSize;
        }

        public void setSlaveConnectionMinimumIdleSize(Integer slaveConnectionMinimumIdleSize) {
            this.slaveConnectionMinimumIdleSize = slaveConnectionMinimumIdleSize;
        }
    }

    /**
     * Configuration for the Pub/Sub message processing thread pool in L2 providers.
     */
    public static class Subscriber {
        /**
         * The core number of threads.
         */
        private int corePoolSize = 4;

        /**
         * The maximum number of threads.
         */
        private int maximumPoolSize = 8;

        /**
         * The maximum time that excess idle threads will wait for new tasks before terminating.
         */
        private Duration keepAliveTime = Duration.ZERO;

        /**
         * The capacity of the blocking queue used to hold tasks before they are executed.
         */
        private int capacity = 100;

        public int getCorePoolSize() {
            return corePoolSize;
        }

        public void setCorePoolSize(int corePoolSize) {
            this.corePoolSize = corePoolSize;
        }

        public int getMaximumPoolSize() {
            return maximumPoolSize;
        }

        public void setMaximumPoolSize(int maximumPoolSize) {
            this.maximumPoolSize = maximumPoolSize;
        }

        public Duration getKeepAliveTime() {
            return keepAliveTime;
        }

        public void setKeepAliveTime(Duration keepAliveTime) {
            this.keepAliveTime = keepAliveTime;
        }

        public int getCapacity() {
            return capacity;
        }

        public void setCapacity(int capacity) {
            this.capacity = capacity;
        }
    }

    /**
     * Configuration for the SingleFlight protection mechanism against cache breakdowns.
     */
    public static class SingleFlight {
        /**
         * The maximum time to wait for a concurrent request to load the data.
         */
        private Duration awaitTimeout = Duration.ofSeconds(10);

        public Duration getAwaitTimeout() {
            return awaitTimeout;
        }

        public void setAwaitTimeout(Duration awaitTimeout) {
            this.awaitTimeout = awaitTimeout;
        }
    }

    /**
     * Configuration for the local message compensation mechanism.
     */
    public static class Compensation {
        /**
         * Whether to start the local compensation replayer.
         */
        private boolean enabled = true;

        /**
         * The initial delay before the compensation replayer starts.
         */
        private Duration initialDelay = Duration.ofSeconds(10);

        /**
         * The period between successive executions of the compensation replayer.
         */
        private Duration period = Duration.ofSeconds(10);

        /**
         * The maximum number of unprocessed messages to fetch per batch.
         */
        private int batchSize = 100;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public Duration getInitialDelay() {
            return initialDelay;
        }

        public void setInitialDelay(Duration initialDelay) {
            this.initialDelay = initialDelay;
        }

        public Duration getPeriod() {
            return period;
        }

        public void setPeriod(Duration period) {
            this.period = period;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }
    }

    /**
     * Configuration for handling cache misses and cache penetrations.
     */
    public static class CacheMiss {
        /**
         * The TTL to apply for cache penetration (when the loaded data is null).
         */
        private Duration penetrationTtl = Duration.ofMillis(30000);

        /**
         * The TTL to apply for backfilling the cache upon a miss.
         */
        private Duration backfillTtl = Duration.ofMillis(15000);

        /**
         * The default TTL used when no explicit TTL is provided during a load.
         */
        private Duration defaultTtl = Duration.ofMillis(15000);

        public Duration getPenetrationTtl() {
            return penetrationTtl;
        }

        public void setPenetrationTtl(Duration penetrationTtl) {
            this.penetrationTtl = penetrationTtl;
        }

        public Duration getBackfillTtl() {
            return backfillTtl;
        }

        public void setBackfillTtl(Duration backfillTtl) {
            this.backfillTtl = backfillTtl;
        }

        public Duration getDefaultTtl() {
            return defaultTtl;
        }

        public void setDefaultTtl(Duration defaultTtl) {
            this.defaultTtl = defaultTtl;
        }
    }
}
