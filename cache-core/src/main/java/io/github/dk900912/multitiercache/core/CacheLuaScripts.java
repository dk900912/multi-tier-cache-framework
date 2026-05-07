package io.github.dk900912.multitiercache.core;

/**
 * Holds Lua scripts used for atomic cache operations in Redis.
 *
 * @author dukui
 */
public final class CacheLuaScripts {

    public static final String UPSERT_LUA_SCRIPT = """
            local current = redis.call('GET', KEYS[1])
            local ttlMillis = tonumber(ARGV[2])
            if not current then
                redis.call('SET', KEYS[1], ARGV[1], 'PX', ttlMillis)
                redis.call('PUBLISH', ARGV[4], ARGV[1])
                return 1
            end
            local newVersion = tonumber(ARGV[3])
            local currentPayload = cjson.decode(current)
            local currentVersion = tonumber(currentPayload.version)
            if newVersion > currentVersion then
                redis.call('SET', KEYS[1], ARGV[1], 'PX', ttlMillis)
                redis.call('PUBLISH', ARGV[4], ARGV[1])
                return 1
            end
            return 0""";

    public static final String DELETE_LUA_SCRIPT = """
            local current = redis.call('GET', KEYS[1])
            local ttlMillis = tonumber(ARGV[2])
            if not current then
                redis.call('SET', KEYS[1], ARGV[1], 'PX', ttlMillis)
                redis.call('PUBLISH', ARGV[4], ARGV[1])
                return 1
            end
            local newVersion = tonumber(ARGV[3])
            local currentPayload = cjson.decode(current)
            local currentVersion = tonumber(currentPayload.version)
            if newVersion >= currentVersion then
                redis.call('SET', KEYS[1], ARGV[1], 'PX', ttlMillis)
                redis.call('PUBLISH', ARGV[4], ARGV[1])
                return 1
            end
            return 0""";

    public static final String CACHE_MISS_LUA_SCRIPT = """
            local current = redis.call('GET', KEYS[1])
            local ttlMillis = tonumber(ARGV[2])
            if not current then
                redis.call('SET', KEYS[1], ARGV[1], 'PX', ttlMillis)
                return 1
            end
            return 0""";

    private CacheLuaScripts() {
        throw new UnsupportedOperationException("Utility class");
    }
}
