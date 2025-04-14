package org.example;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.fasterxml.jackson.databind.ObjectMapper;

public class Main {
    private static final String REDIS_HOST = "redis://localhost:6379";

    private static final String serviceId = "test-service-1";

    public static TurboConfig readConfig(String configPath) {
        try (InputStream is = Files.newInputStream(Paths.get(configPath))) {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(is, TurboConfig.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        // print cur path
        String configPath = args.length > 0 ? args[0] : "./config.json";
        TurboConfig turboConfig = readConfig(configPath);
        int turboNum = turboConfig.getTurboNum();
        String redisUri = turboConfig.getRedisUri();
        RedisClientConfig redisConfig = new RedisClientConfig();
        redisConfig.setUri(redisUri);
        redisConfig.setClusterMode(turboConfig.getRedisMode());
        for (int i = 0; i < turboNum; i++) {
            String randomInstanceName = "instance-" + System.currentTimeMillis() + "-" + i;
            MockTurboService service = new MockTurboService(serviceId,  randomInstanceName, redisConfig);
            service.startHeartbeat();
        }
    }
}