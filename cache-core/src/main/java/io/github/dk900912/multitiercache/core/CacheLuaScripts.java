package io.github.dk900912.multitiercache.core;

/**
 * Holds Lua scripts used for atomic cache operations in Redis.
 *
 * @author dukui
 */
public final class CacheLuaScripts {

    /*
     * ARGV layout:
     *  1 payload
     *  2 ttlMillis
     *  3 incomingType
     *  4 incomingVersion
     *  5 publishChannel
     *  6 publishEnabled
     */
    public static final String APPLY_MESSAGE_LUA_SCRIPT = """
            local current = redis.call('GET', KEYS[1])
            local ttlMillis = tonumber(ARGV[2])
            local incomingType = ARGV[3]
            local incomingVersion = tonumber(ARGV[4])
            local publishChannel = ARGV[5]
            local publishEnabled = tonumber(ARGV[6])

            local function writeValue()
                if ttlMillis and ttlMillis > 0 then
                    redis.call('SET', KEYS[1], ARGV[1], 'PX', ttlMillis)
                else
                    redis.call('SET', KEYS[1], ARGV[1])
                end
                if publishEnabled == 1 then
                    redis.call('PUBLISH', publishChannel, ARGV[1])
                end
                return 1
            end

            if not current then
                return writeValue()
            end

            local ok, decoded = pcall(cjson.decode, current)
            if (not ok) or type(decoded) ~= 'table' then
                return writeValue()
            end
            local currentType = decoded.type
            local currentVersion = tonumber(decoded.version or -1)

            if incomingType == 'penetrate' then
                if currentType == 'penetrate' then
                    return writeValue()
                end
                return 0
            end

            if currentType == 'penetrate' then
                return writeValue()
            end

            if incomingType == 'delete' then
                if incomingVersion >= currentVersion then
                    return writeValue()
                end
                return 0
            end

            if currentType == 'delete' then
                return 0
            end

            if incomingVersion > currentVersion then
                return writeValue()
            end
            return 0""";

    private CacheLuaScripts() {
        throw new UnsupportedOperationException("Utility class");
    }
}
