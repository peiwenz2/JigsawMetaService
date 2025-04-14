package org.example.connector;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.support.AsyncConnectionPoolSupport;
import io.lettuce.core.support.BoundedAsyncPool;
import io.lettuce.core.support.BoundedPoolConfig;
import org.example.MockTurboService;
import org.example.RedisClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;


public class ReactiveRedisClient implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(MockTurboService.class);

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

    public ReactiveRedisClient(RedisClientConfig config) {
        client = RedisClient.create();
        uri = RedisURI.create(config.getUri());
        redisClientConfig = config;
        log.info("Connect to redis with uri [{}]", config.getUri());
        connection = client.connect(uri);
        BoundedPoolConfig poolConfig = BoundedPoolConfig.builder()
            .maxTotal(config.getMaxTotalConnections())
            .maxIdle(config.getMaxIdleConnections())
            .minIdle(config.getMinIdleConnections())
            .build();
        connectionPool = AsyncConnectionPoolSupport.createBoundedObjectPool(
            () -> client.connectAsync(StringCodec.UTF8, uri), poolConfig);
    }

    @Override
    public void close() throws Exception {
        connection.close();
        connectionPool.close();
        client.shutdown();
    }

    /**
     * Expose sync commands directly, for common operations.
     */
    public RedisReactiveCommands<String, String> reactiveCommands() {
        return connection.reactive();
    }

    public RedisAsyncCommands<String, String> asyncCommands() {
        return connection.async();
    }

    public boolean isConnectionActive() {
        return connection.isOpen();
    }

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

    public Mono<String> setNxWithExpire(String key, String value, long expireSeconds) {
        return reactiveCommands().set(key, value, SetArgs.Builder.nx().ex(expireSeconds));
    }

    public Mono<Boolean> batchZAddWithExpire(List<String> keys, int userBucket, long expireSeconds,
                                             double score) {
        RedisAsyncCommands<String, String> commands = asyncCommands();
        List<Mono<Tuple2<Long, Boolean>>> operations = new ArrayList<>();
        for (String key : keys) {
            CompletableFuture<Long> future1 = commands.zadd(key, score, String.valueOf(userBucket)).toCompletableFuture();
            CompletableFuture<Boolean> future2 = commands.expire(key, expireSeconds).toCompletableFuture();
            Mono<Tuple2<Long, Boolean>> mono = Mono.zip(Mono.fromFuture(future1), Mono.fromFuture(future2));
            operations.add(mono);
        }
        commands.flushCommands();
        return Mono.when(operations).thenReturn(true);
    }

    public Mono<Map<String, List<String>>> batchZRangeWithExpire(List<String> keys, long expireSeconds) {
        RedisReactiveCommands<String, String> commands = reactiveCommands();

        Map<String, List<String>> hashResultMap = new ConcurrentHashMap<>();
        List<Mono<List<String>>> operations = new ArrayList<>();
        for (String key : keys) {
            operations.add(commands.expire(key, expireSeconds).flatMap(result -> Mono.just(Collections.emptyList())));
            operations.add(commands.zrange(key, 0, -1).collectList().flatMap(result -> {
                hashResultMap.put(key, result);
                return Mono.just(result);
            }));
        }
        commands.flushCommands();
        return Mono.when(operations).thenReturn(hashResultMap);
    }
}
