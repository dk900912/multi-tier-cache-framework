package io.github.dk900912.multitiercache.core;

/**
 * Holds Lua scripts used for atomic cache operations in Redis.
 *
 * @author dukui
 */
public final class CacheLuaScripts {

    /*
     * RESOLVE_GENERATION_LUA_SCRIPT line-by-line notes:
     *  1  local current = redis.call('GET', KEYS[1])
     *     Read the current data payload so generation resolution can observe the latest applied state.
     *  2  local op = ARGV[1]
     *     Operation type: insert/update/delete/backfill/penetrate.
     *  3  local storedGeneration = redis.call('GET', KEYS[2])
     *     Read the dedicated generation fence key, which survives data value replacement.
     *  4  if storedGeneration then
     *     Normalize the persisted generation into a Lua number when the metadata key already exists.
     *  5      storedGeneration = tonumber(storedGeneration)
     *     Convert Redis bulk string to number for later ordering comparisons.
     *  6  end
     *     End of generation metadata normalization.
     *
     *  7  local currentGeneration = nil
     *     Placeholder for the generation embedded in the current data payload.
     *  8  local currentType = nil
     *     Placeholder for the current payload type so insert can detect reinsert after delete/penetrate.
     *  9  if current then
     *     Decode the current payload only when a data key already exists.
     * 10      local currentPayload = cjson.decode(current)
     *     Parse the stored CacheMessage JSON.
     * 11      currentType = currentPayload.type
     *     Capture the stored message type for lifecycle decisions.
     * 12      currentGeneration = tonumber(currentPayload.generation or 0)
     *     Treat missing generation defensively as 0.
     * 13      if (not storedGeneration) or currentGeneration > storedGeneration then
     *     If payload generation is newer than the side-key, repair the side-key view in memory first.
     * 14          storedGeneration = currentGeneration
     *     Use the newer payload generation as the effective base generation.
     * 15      end
     *     End of payload-vs-side-key reconciliation.
     * 16  end
     *     End of current payload parsing.
     *
     * 17  local function persistGeneration(value)
     *     Helper used by all branches to persist and return the chosen lifecycle generation.
     * 18      redis.call('SET', KEYS[2], tostring(value))
     *     Keep the generation side-key durable and numeric.
     * 19      return value
     *     Return the resolved generation to Java without extra reads.
     * 20  end
     *     End of helper.
     *
     * 21  if op == 'penetrate' then
     *     Penetration hints do not participate in business lifecycle ordering.
     * 22      return 0
     *     Force generation 0 so penetrate never outranks real business states.
     * 23  end
     *     End of penetrate branch.
     *
     * 24  if op == 'insert' then
     *     Insert is special because it may actually be a reinsert into a new lifecycle generation.
     * 25      if (not current) or currentType == 'delete' or currentType == 'penetrate' then
     *     Missing current value, delete tombstone, or penetrate hint all mean a fresh lifecycle may start.
     * 26          return redis.call('INCR', KEYS[2])
     *     Atomically advance the generation fence so reinsert can beat an older tombstone/version.
     * 27      end
     *     End of new-lifecycle insert branch.
     * 28      if storedGeneration and storedGeneration > 0 then
     *     Reuse the existing positive generation when this insert is really an upsert in the same lifecycle.
     * 29          return persistGeneration(storedGeneration)
     *     Persist and return the existing lifecycle fence.
     * 30      end
     *     End of stored-generation reuse branch.
     * 31      if currentGeneration and currentGeneration > 0 then
     *     Fall back to the generation embedded in the current payload if the side-key was absent.
     * 32          return persistGeneration(currentGeneration)
     *     Repair the side-key from payload state.
     * 33      end
     *     End of payload-generation fallback branch.
     * 34      return persistGeneration(1)
     *     First-ever insert starts lifecycle generation 1.
     * 35  end
     *     End of insert branch.
     *
     * 36  local baseGeneration = storedGeneration
     *     Non-insert mutations normally stay within the current lifecycle fence.
     * 37  if ((not baseGeneration) or baseGeneration < 1) and currentGeneration and currentGeneration > 0 then
     *     If the side-key is absent or invalid, recover from the payload generation instead.
     * 38      baseGeneration = currentGeneration
     *     Use current payload generation as the base lifecycle fence.
     * 39  end
     *     End of recovery branch.
     * 40  if (not baseGeneration) or baseGeneration < 1 then
     *     Final defensive fallback when neither side-key nor payload provided a valid generation.
     * 41      baseGeneration = 1
     *     Updates/deletes/backfills outside L2-disabled mode should still operate in lifecycle 1 minimum.
     * 42  end
     *     End of minimum-generation normalization.
     * 43  return persistGeneration(baseGeneration)
     *     Persist and return the lifecycle generation used by this mutation/backfill.
     */
    public static final String RESOLVE_GENERATION_LUA_SCRIPT = """
            local current = redis.call('GET', KEYS[1])
            local op = ARGV[1]
            local storedGeneration = redis.call('GET', KEYS[2])
            if storedGeneration then
                storedGeneration = tonumber(storedGeneration)
            end

            local currentGeneration = nil
            local currentType = nil
            if current then
                local currentPayload = cjson.decode(current)
                currentType = currentPayload.type
                currentGeneration = tonumber(currentPayload.generation or 0)
                if (not storedGeneration) or currentGeneration > storedGeneration then
                    storedGeneration = currentGeneration
                end
            end

            local function persistGeneration(value)
                redis.call('SET', KEYS[2], tostring(value))
                return value
            end

            if op == 'penetrate' then
                return 0
            end

            if op == 'insert' then
                if (not current) or currentType == 'delete' or currentType == 'penetrate' then
                    return redis.call('INCR', KEYS[2])
                end
                if storedGeneration and storedGeneration > 0 then
                    return persistGeneration(storedGeneration)
                end
                if currentGeneration and currentGeneration > 0 then
                    return persistGeneration(currentGeneration)
                end
                return persistGeneration(1)
            end

            local baseGeneration = storedGeneration
            if ((not baseGeneration) or baseGeneration < 1) and currentGeneration and currentGeneration > 0 then
                baseGeneration = currentGeneration
            end
            if (not baseGeneration) or baseGeneration < 1 then
                baseGeneration = 1
            end
            return persistGeneration(baseGeneration)""";

    /*
     * APPLY_MESSAGE_LUA_SCRIPT line-by-line notes:
     *  1  local current = redis.call('GET', KEYS[1])
     *     Read the current payload once so the entire compare-and-set decision is made atomically in Redis.
     *  2  local ttlMillis = tonumber(ARGV[2])
     *     TTL in milliseconds; nil means write without PX.
     *  3  local incomingType = ARGV[3]
     *     Incoming message type: insert/update/delete/backfill/penetrate.
     *  4  local incomingGeneration = tonumber(ARGV[4])
     *     Lifecycle fence of the incoming state.
     *  5  local incomingVersion = tonumber(ARGV[5])
     *     Version within the lifecycle.
     *  6  local publishChannel = ARGV[6]
     *     Pub/Sub channel used only for mutation propagation.
     *  7  local publishEnabled = tonumber(ARGV[7])
     *     1 means mutation path should publish; 0 means read-path backfill should stay local to Redis.
     *
     *  8  local function writeValue()
     *     Shared write helper so every winning branch performs identical set/publish behavior.
     *  9      if ttlMillis and ttlMillis > 0 then
     *     TTL-aware branch for normal cached values and tombstones.
     * 10          redis.call('SET', KEYS[1], ARGV[1], 'PX', ttlMillis)
     *     Persist the payload with millisecond TTL.
     * 11      else
     *     No-TTL branch for callers that intentionally omit expiration.
     * 12          redis.call('SET', KEYS[1], ARGV[1])
     *     Persist without PX.
     * 13      end
     *     End of TTL handling.
     * 14      if publishEnabled == 1 then
     *     Only mutation writes broadcast invalidation; read backfills never publish.
     * 15          redis.call('PUBLISH', publishChannel, ARGV[1])
     *     Publish the exact winning payload so subscribers compare against the same generation/version.
     * 16      end
     *     End of publish branch.
     * 17      return 1
     *     Signal "applied" back to Java.
     * 18  end
     *     End of shared write helper.
     *
     * 19  if not current then
     *     Empty slot accepts any incoming state, including delete tombstone and penetrate hint.
     * 20      return writeValue()
     *     Fast path: no comparison needed because there is no existing winner.
     * 21  end
     *     End of absent-key fast path.
     *
     * 22  local currentPayload = cjson.decode(current)
     *     Parse the current CacheMessage once comparisons are required.
     * 23  local currentType = currentPayload.type
     *     Current state type influences penetrate handling.
     * 24  local currentGeneration = tonumber(currentPayload.generation or 0)
     *     Missing generation is treated as 0 for backward compatibility.
     * 25  local currentVersion = tonumber(currentPayload.version or -1)
     *     Missing version is treated as -1 so real data dominates legacy/empty values.
     *
     * 26  if incomingType == 'penetrate' then
     *     Penetration hints are intentionally lowest priority and may only refresh themselves.
     * 27      if currentType == 'penetrate' then
     *     A newer read miss may refresh the short TTL of an existing penetrate hint.
     * 28          return writeValue()
     *     Replace penetrate with penetrate to refresh TTL/payload shape.
     * 29      end
     *     End of penetrate-refresh branch.
     * 30      return 0
     *     Never allow penetrate to override real value or delete tombstone.
     * 31  end
     *     End of incoming-penetrate branch.
     *
     * 32  if currentType == 'penetrate' then
     *     Any real business state or delete tombstone outranks a stale penetrate hint.
     * 33      return writeValue()
     *     Replace penetrate immediately without generation/version comparison.
     * 34  end
     *     End of current-penetrate branch.
     *
     * 35  if incomingGeneration > currentGeneration then
     *     Higher lifecycle fence always wins, even if its business version numerically restarted.
     * 36      return writeValue()
     *     Accept the new lifecycle state.
     * 37  end
     *     End of higher-generation branch.
     * 38  if incomingGeneration < currentGeneration then
     *     Older lifecycle state must never overwrite a newer lifecycle winner.
     * 39      return 0
     *     Reject stale lifecycle update.
     * 40  end
     *     End of lower-generation branch.
     *
     * 41  if incomingType == 'delete' then
     *     Deletes use >= so a same-version tombstone can still evict the live value deterministically.
     * 42      if incomingVersion >= currentVersion then
     *     Same-generation delete wins on equal-or-newer version.
     * 43          return writeValue()
     *     Persist tombstone and optionally broadcast invalidation.
     * 44      end
     *     End of delete-accept branch.
     * 45      return 0
     *     Reject stale delete.
     * 46  end
     *     End of delete branch.
     *
     * 47  if incomingVersion > currentVersion then
     *     Non-delete writes require a strictly newer version within the same lifecycle.
     * 48      return writeValue()
     *     Persist fresher insert/update/backfill state.
     * 49  end
     *     End of version-accept branch.
     * 50  return 0
     *     Equal-or-older non-delete message loses the race and is ignored.
     */
    public static final String APPLY_MESSAGE_LUA_SCRIPT = """
            local current = redis.call('GET', KEYS[1])
            local ttlMillis = tonumber(ARGV[2])
            local incomingType = ARGV[3]
            local incomingGeneration = tonumber(ARGV[4])
            local incomingVersion = tonumber(ARGV[5])
            local publishChannel = ARGV[6]
            local publishEnabled = tonumber(ARGV[7])

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

            local currentPayload = cjson.decode(current)
            local currentType = currentPayload.type
            local currentGeneration = tonumber(currentPayload.generation or 0)
            local currentVersion = tonumber(currentPayload.version or -1)

            if incomingType == 'penetrate' then
                if currentType == 'penetrate' then
                    return writeValue()
                end
                return 0
            end

            if currentType == 'penetrate' then
                return writeValue()
            end

            if incomingGeneration > currentGeneration then
                return writeValue()
            end
            if incomingGeneration < currentGeneration then
                return 0
            end

            if incomingType == 'delete' then
                if incomingVersion >= currentVersion then
                    return writeValue()
                end
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
