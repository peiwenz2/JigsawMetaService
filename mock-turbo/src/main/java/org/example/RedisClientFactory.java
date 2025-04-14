package org.example;

import org.example.connector.SimpleRedisClient;
import org.example.connector.SimpleRedisClusterClient;

public class RedisClientFactory {
    public static RedisClientAdapter createClient(RedisClientConfig config) {
        if (config.getClusterMode().equals("cluster")) {
            return new SimpleRedisClusterClient(config);
        } else {
            return new SimpleRedisClient(config);
        }
    }
}
