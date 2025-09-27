package org.apache.flink.streaming.connectors.redis.container;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.flink.streaming.connectors.redis.config.FlinkClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RedisClientUtil {

    private static final Logger LOG = LoggerFactory.getLogger(RedisClientUtil.class);

    private static final int MAX_KEY_LENGTH = 200;
    private static final int MAX_PASSWORD_LENGTH = 500;
    private static final int MAX_ENDPOINT_SIZE = 5_000;

    public static <T> T executeWithRedisContainer(String endpoints,
                                                  String password,
                                                  String key,
                                                  RedisFunction<T> function) {
        validateInput(endpoints, password, key);

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

    private static void validateInput(String endpoints, String password, String key) {
        Validate.notBlank(endpoints, "endpoints cant be blank");
        Validate.notBlank(key, "key cant be blank");

        if (endpoints.length() > MAX_ENDPOINT_SIZE) {
            throw new IllegalArgumentException("endpoints too long");
        }
        if (StringUtils.isNotEmpty(password) && password.length() > MAX_PASSWORD_LENGTH) {
            throw new IllegalArgumentException("password too long");
        }
        if (key.length() > MAX_KEY_LENGTH) {
            throw new IllegalArgumentException("key too long");
        }
        // 白名单校验 key 是否恶意注入“host:port,host2:port2”以外的字符（如空格、换行、@、# 等）
        if (!key.matches("[\\w\\-\\.:@*]+")) {
            throw new IllegalArgumentException("key contains illegal char");
        }
        // 简单校验 endpoints 格式
        for (String node : endpoints.split(",")) {
            if (!node.matches("^[-\\w\\.]+:\\d{1,5}$")) {
                throw new IllegalArgumentException("invalid node format: " + node);
            }
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
