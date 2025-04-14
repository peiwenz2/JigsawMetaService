package org.example;

import lombok.Data;

@Data
public class RedisClientConfig {

    private String uri = "redis://:password@localhost";

    private int maxTotalConnections = 8;

    private int maxIdleConnections = 8;

    private int minIdleConnections = 0;

    private boolean reconnect = true;

    private String clusterMode;
}