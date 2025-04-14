package org.example;

import io.lettuce.core.api.sync.RedisCommands;

import java.util.List;

public interface RedisClientAdapter extends AutoCloseable {

    boolean isConnectionActive();
    void reConnect();
    void close() throws Exception;

    String setWithExpire(String key, String value, long expireSeconds);
    String setNxWithExpire(String key, String value, long expireSeconds);
    Long setInstanceSet(String instanceKey);
    String get(String key);
}