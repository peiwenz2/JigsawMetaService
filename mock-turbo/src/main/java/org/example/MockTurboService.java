package org.example;

import io.lettuce.core.RedisClient;
import org.example.connector.SimpleRedisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MockTurboService {
    private static final Logger logger = LoggerFactory.getLogger(MockTurboService.class);

    private static final long HEARTBEAT_INTERVAL_SECONDS = 15;
    private static final long ALIVE_KEY_EXPIRE_SECONDS = 20;
    private static final int MAX_TURBO_NUM = 1000;

    private final String serviceId;
    private final String instanceName; // unique name of the instance
    private final RedisClientAdapter client;
    private final ScheduledExecutorService executor;
    private int instanceId = -1;

    public MockTurboService(String serviceId, String instanceName, RedisClientConfig config) {
        this.serviceId = serviceId;
        this.instanceName = instanceName;
        client = RedisClientFactory.createClient(config);
        executor = Executors.newScheduledThreadPool(1);
        logger.info("Create MockTurboService with serviceId [{}], instanceName [{}]", serviceId, instanceName);
    }

    public void startHeartbeat() {
        executor.scheduleAtFixedRate(this::getInstanceLock, 0, HEARTBEAT_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    private void setInstanceId() {
        boolean lockIdSuccess = false;
        for (int i = 0; i < MAX_TURBO_NUM; i++) {
            String instanceIdLockKey = buildInstanceIdLockKey(serviceId, i);
            String result = client.setNxWithExpire(instanceIdLockKey, instanceName, ALIVE_KEY_EXPIRE_SECONDS);
            if ("OK".equalsIgnoreCase(result)) {
                lockIdSuccess = true;
                instanceId = i;
                logger.info("This instance [{}] get instance id [{}]", instanceName, instanceId);
                break;
            }
        }
        if (!lockIdSuccess) {
            logger.info("This instance [{}] failed to get instance id! Current id [{}]", instanceName, instanceId);
        }
    }

    public void getInstanceLock() {
        // get a valid instance id
        if (instanceId == -1) {
            setInstanceId();
            return;
        }
        // check the lock
        String instanceIdLockKey = buildInstanceIdLockKey(serviceId, instanceId);
        String value = client.get(instanceIdLockKey);
        logger.info("Check instance id lock, instance id [{}], value [{}]", instanceId, value);
        if (value == null || !value.equals(instanceName)) {
            logger.info("This instance [{}] lost instance id [{}], current owner is [{}]", instanceName, instanceId, value);
            setInstanceId();
            return;
        }
        // refresh the lock
        client.setWithExpire(instanceIdLockKey, instanceName, ALIVE_KEY_EXPIRE_SECONDS);
        client.setInstanceSet(instanceIdLockKey);
    }

    public void shutdown() throws Exception {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        client.close();
    }

    public static String buildInstanceIdLockKey(String serviceId, int id) {
        return String.format("dashscope.api.batch.%s.instance.%d.lock", serviceId, id);
    }
}