package org.apache.flink.streaming.connectors.redis.container;

import org.apache.commons.lang3.Validate;
import org.apache.flink.streaming.connectors.redis.config.FlinkClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RedisClientUtil {

    private static final Logger LOG = LoggerFactory.getLogger(RedisClientUtil.class);

    public static <T> T executeWithRedisContainer(String endpoints,
                                                  String password,
                                                  String key,
                                                  RedisFunction<T> function) {
        Validate.notBlank(endpoints, "endpoints cant be blank");
        Validate.notBlank(key, "key cant be blank");
        RedisCommandsContainer redisCommandsContainer = null;
        try {
            redisCommandsContainer = RedisCommandsContainerBuilder.build(new FlinkClusterConfig.Builder()
                    .setNodesInfo(endpoints)
                    .setPassword(password)
                    .build());
            redisCommandsContainer.open();
            LOG.info("success to create redis container.");
            return function.apply(redisCommandsContainer);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Redis operation interrupted", e);
        } catch (Exception e) {
            LOG.error("Redis operation failed: ", e);
            throw new RuntimeException("Redis operation failed", e);
        } finally {
            shutdown(redisCommandsContainer);
        }
    }

    private static void shutdown(RedisCommandsContainer redisCommandsContainer) {
        if (redisCommandsContainer != null) {
            try {
                redisCommandsContainer.close();
            } catch (IOException e) {
                LOG.warn("close redis container fail", e);
            }
        }
    }
}
