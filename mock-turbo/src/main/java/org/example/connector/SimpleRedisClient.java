package org.example.connector;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.support.AsyncConnectionPoolSupport;
import io.lettuce.core.support.BoundedAsyncPool;
import io.lettuce.core.support.BoundedPoolConfig;
import lombok.extern.slf4j.Slf4j;
import org.example.RedisClientAdapter;
import org.example.RedisClientConfig;

import java.util.ArrayList;
import java.util.List;


@Slf4j
public class SimpleRedisClient implements AutoCloseable, RedisClientAdapter {

    private final RedisClient client;

    /**
     * The connection used to do non-exclusive operations. (no transaction, no blocking call)
     */
    private StatefulRedisConnection<String, String> connection;

    /**
     * The connection pool used to do exclusive operations. (transactions, blocking calls)
     */
    private BoundedAsyncPool<StatefulRedisConnection<String, String>> connectionPool;

    private final RedisURI uri;

    private final RedisClientConfig redisClientConfig;

    public SimpleRedisClient(RedisClientConfig config) {
        client = RedisClient.create();
        client.setOptions(ClientOptions.builder().autoReconnect(config.isReconnect()).build());
        uri = RedisURI.create(config.getUri());
        log.info("Connect to redis with uri [{}]", config.getUri());
        connection = client.connect(uri);
        BoundedPoolConfig poolConfig = BoundedPoolConfig.builder()
            .maxTotal(config.getMaxTotalConnections())
            .maxIdle(config.getMaxIdleConnections())
            .minIdle(config.getMinIdleConnections())
            .build();
        redisClientConfig = config;
        connectionPool = AsyncConnectionPoolSupport.createBoundedObjectPool(
            () -> client.connectAsync(StringCodec.UTF8, uri), poolConfig);
    }

    @Override
    public void close() throws Exception {
        connection.close();
        connectionPool.close();
        client.shutdown();
    }

    @Override
    public boolean isConnectionActive() {
        return connection.isOpen();
    }

    @Override
    public void reConnect() {
        StatefulRedisConnection<String, String> newConnection = client.connect(uri);
        BoundedPoolConfig poolConfig = BoundedPoolConfig.builder()
            .maxTotal(redisClientConfig.getMaxTotalConnections())
            .maxIdle(redisClientConfig.getMaxIdleConnections())
            .minIdle(redisClientConfig.getMinIdleConnections())
            .build();
        BoundedAsyncPool<StatefulRedisConnection<String, String>> newConnectionPool
            = AsyncConnectionPoolSupport.createBoundedObjectPool(
            () -> client.connectAsync(StringCodec.UTF8, uri), poolConfig);
        connection.close();
        connectionPool.close();

        connection = newConnection;
        connectionPool = newConnectionPool;
    }

    /**
     * Expose sync commands directly, for common operations.
     */
    public RedisCommands<String, String> syncCommands() {
        return connection.sync();
    }

    // Non-exclusive Operations ==//
    @Override
    public String setWithExpire(String key, String value, long expireSeconds) {
        return syncCommands().set(key, value, SetArgs.Builder.ex(expireSeconds));
    }

    public String setXxWithExpire(String key, String value, long expireSeconds) {
        return syncCommands().set(key, value, SetArgs.Builder.xx().ex(expireSeconds));
    }

    public Long setInstanceSet(String instanceKey) {
        return syncCommands().sadd("instanceinfo_keys", instanceKey);
    }

    @Override
    public String get(String key) {
        return syncCommands().get(key);
    }

    public String hget(String key, String filed) {
        return syncCommands().hget(key, filed);
    }


    public String setNxWithExpire(String key, long expireSeconds) {
        return syncCommands().set(key, "1", SetArgs.Builder.nx().ex(expireSeconds));
    }

    @Override
    public String setNxWithExpire(String key, String value, long expireSeconds) {
        return syncCommands().set(key, value, SetArgs.Builder.nx().ex(expireSeconds));
    }

    public long del(String key) {
        return syncCommands().del(key);
    }

    public long exists(String key) {
        return syncCommands().exists(key);
    }

    public String type(String key) {
        return syncCommands().type(key);
    }

    public List<Object> evalWithListOutput(String script, String queueKey, int minScore, int maxScore, int intParam) {
        return syncCommands().eval(script,
            ScriptOutputType.MULTI,
            new String[] {queueKey},
            String.valueOf(minScore),
            String.valueOf(maxScore),
            String.valueOf(intParam));
    }

    public List<Object> evalWithListOutput(String script, String[] keys, int param1, int param2, int param3) {
        return syncCommands().eval(script,
            ScriptOutputType.MULTI,
            keys,
            String.valueOf(param1),
            String.valueOf(param2),
            String.valueOf(param3));
    }

    public List<Object> evalWithListOutput(
        String script, String queueKey,
        int startIdx, int endIdx,
        int loopStartIdx, int loopEndIdx,
        int alternativeStartIdx, int alternativeEndIdx,
        int alternativeLoopStartIdx, int alternativeLoopEndIdx,
        int seizeThreshold, int globalSeizeThreshold
    ) {
        return syncCommands().eval(script,
            ScriptOutputType.MULTI,
            new String[] {queueKey},
            String.valueOf(startIdx), String.valueOf(endIdx),
            String.valueOf(loopStartIdx), String.valueOf(loopEndIdx),
            String.valueOf(alternativeStartIdx), String.valueOf(alternativeEndIdx),
            String.valueOf(alternativeLoopStartIdx), String.valueOf(alternativeLoopEndIdx),
            String.valueOf(seizeThreshold), String.valueOf(globalSeizeThreshold));
    }

    public List<Object> evalWithListOutput(String script, String key, String oldValue, String newValue,
                                           int expireSeconds) {
        return syncCommands().eval(script,
            ScriptOutputType.MULTI,
            new String[] {key},
            oldValue,
            newValue,
            String.valueOf(expireSeconds));
    }

    public List<Object> evalWithListOutput(String script, String key, int value, int expireSeconds) {
        return syncCommands().eval(script,
            ScriptOutputType.MULTI,
            new String[] {key},
            String.valueOf(value),
            String.valueOf(expireSeconds));
    }

    public Object evalWithSingleOutput(String script, String queueKey, int minScore, int maxScore) {
        return syncCommands().eval(script,
            ScriptOutputType.VALUE,
            new String[] {queueKey},
            String.valueOf(minScore),
            String.valueOf(maxScore));
    }

    public Object evalWithSingleOutput(String script, String queueKey, long minScore, long maxScore) {
        return syncCommands().eval(script,
            ScriptOutputType.VALUE,
            new String[] {queueKey},
            String.valueOf(minScore),
            String.valueOf(maxScore));
    }

    public List<ScoredValue<String>> zRangeByScoreWithScores(String key, long min, long max) {
        return syncCommands().zrangebyscoreWithScores(key, Range.create(min, max));
    }

    public Long zRem(String key, String value) {
        return syncCommands().zrem(key, value);
    }

    public Long zCard(String key) {
        return syncCommands().zcard(key);
    }

    public long publish(String key, String value) {
        return syncCommands().publish(key, value);
    }

    public List<String> keys(String pattern) {
        return syncCommands().keys(pattern);
    }

    public List<String> mget(List<String> keys) {
        List<KeyValue<String, String>> values = syncCommands().mget(keys.toArray(new String[0]));
        List<String> result = new ArrayList<>();
        for (KeyValue<String, String> kv : values) {
            result.add(kv.getValueOrElse(null));
        }

        return result;
    }

    public RedisFuture<List<KeyValue<String, String>>> asyncMGet(List<String> keys) {
        return connection.async().mget(keys.toArray(new String[0]));
    }

    public String set(String key, String value) {
        return syncCommands().set(key, value);
    }

    //== Exclusive Operations ==//

    private interface Handler<T> {

        T handle(RedisCommands<String, String> commands) throws Throwable;
    }

    private interface AsyncHandler<T> {

        T handle(RedisAsyncCommands<String, String> commands) throws Throwable;
    }
}
