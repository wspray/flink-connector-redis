package org.apache.flink.streaming.connectors.redis.stream;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.redis.command.RedisCommand;
import org.apache.flink.streaming.connectors.redis.config.FlinkClusterConfig;
import org.apache.flink.streaming.connectors.redis.config.FlinkConfigBase;
import org.apache.flink.streaming.connectors.redis.config.FlinkSentinelConfig;
import org.apache.flink.streaming.connectors.redis.config.FlinkSingleConfig;
import org.apache.flink.streaming.connectors.redis.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.config.RedisValueDataStructure;
import org.apache.flink.streaming.connectors.redis.mapper.RedisSinkRowMapper;
import org.apache.flink.streaming.connectors.redis.mapper.RowRedisSinkRowMapper;
import org.apache.flink.types.Row;

import java.util.List;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.flink.streaming.connectors.redis.config.RedisOptions.CUSTOM_FIELD_NAME;
import static org.apache.flink.streaming.connectors.redis.config.RedisOptions.CUSTOM_SCORE_NAME;
import static org.apache.flink.streaming.connectors.redis.config.RedisOptions.CUSTOM_VALUE_NAME;
import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.REDIS_CLUSTER;
import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.REDIS_MODE;
import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.REDIS_SENTINEL;
import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.REDIS_SINGLE;

public class RedisSinkFunctionBuilder<T> {

    private RedisSinkRowMapper redisMapper;
    private Configuration configuration = new Configuration();
    private FlinkConfigBase flinkConfigBase;
    private List<TypeInformation> columnDataTypes;

    public static RedisSinkFunctionBuilder<Row> builder() {
        return new RedisSinkFunctionBuilder<>();
    }

    public RedisSinkFunctionBuilder<T> setRedisMapper(RedisSinkRowMapper redisMapper) {
        this.redisMapper = redisMapper;
        return this;
    }

    public RedisSinkFunctionBuilder<T> setFlinkConfigBase(FlinkConfigBase flinkConfigBase) {
        this.flinkConfigBase = flinkConfigBase;
        return this;
    }

    public RedisSinkFunctionBuilder<T> setColumnDataTypes(List<TypeInformation> columnDataTypes) {
        this.columnDataTypes = columnDataTypes;
        return this;
    }

//    public RedisSinkFunctionBuilder<T> setResolvedSchema(Map<String, String> map) {
//        this.map = map;
//        return this;
//    }

    public RedisSinkFunctionBuilder<T> setConfiguration(Configuration configuration) {
        this.configuration = configuration;
        return this;
    }

    public RedisSinkFunctionBuilder<T> setConnectionTimeout(int timeout) {
        configuration.set(RedisOptions.TIMEOUT, timeout);
        return this;
    }

    public RedisSinkFunctionBuilder<T> setMaxRetryTimes(int maxRetryTimes) {
        configuration.set(RedisOptions.MAX_RETRIES, maxRetryTimes);
        return this;
    }

    public RedisSinkFunctionBuilder<T> setKeyName(String keyName) {
        configuration.set(RedisOptions.CUSTOM_KEY_NAME, keyName);
        return this;
    }

    public RedisSinkFunctionBuilder<T> setValueName(String valueName) {
        configuration.set(CUSTOM_VALUE_NAME, valueName);
        return this;
    }

    public RedisSinkFunctionBuilder<T> setAllRowOutPut(boolean allRowOutPut) {
        if (allRowOutPut) {
            configuration.set(RedisOptions.VALUE_DATA_STRUCTURE, RedisValueDataStructure.row);
        }
        return this;
    }

    public RedisSinkFunctionBuilder<T> setMaxRetries(int maxRetries) {
        configuration.set(RedisOptions.MAX_RETRIES, maxRetries);
        return this;
    }

    public RedisSinkFunctionBuilder<T> setIfAbsent(boolean ifAbsent) {
        configuration.set(RedisOptions.SET_IF_ABSENT, ifAbsent);
        return this;
    }

    public RedisSinkFunctionBuilder<T> setTTL(int ttl) {
        configuration.set(RedisOptions.TTL, ttl);
        return this;
    }

    public RedisSinkFunctionBuilder<T> setTTLKeyNotAbsent(boolean ttlKeyNotAbsent) {
        configuration.set(RedisOptions.TTL_KEY_NOT_ABSENT, ttlKeyNotAbsent);
        return this;
    }

    public RedisSinkFunctionBuilder<T> setExpireOnTime(String expireOnTime) {
        configuration.set(RedisOptions.EXPIRE_ON_TIME, expireOnTime);
        return this;
    }

    public RedisSinkFunctionBuilder<T> setSinkSet(String valueName) {
        if (isNotEmpty(valueName)) {
            configuration.set(CUSTOM_VALUE_NAME, valueName);
        }
        this.redisMapper = new RowRedisSinkRowMapper(RedisCommand.SET, this.configuration);
        return this;
    }

    public RedisSinkFunctionBuilder<T> setSinkHSet(String fieldName,
                                                   String valueName) {
        if (isNotEmpty(fieldName)) {
            configuration.set(CUSTOM_FIELD_NAME, fieldName);
        }
        if (isNotEmpty(valueName)) {
            configuration.set(CUSTOM_VALUE_NAME, valueName);
        }
        this.redisMapper = new RowRedisSinkRowMapper(RedisCommand.HSET, this.configuration);
        return this;
    }

    public RedisSinkFunctionBuilder<T> setSinkList(String valueName) {
        if (isNotEmpty(valueName)) {
            configuration.set(CUSTOM_VALUE_NAME, valueName);
        }
        this.redisMapper = new RowRedisSinkRowMapper(RedisCommand.RPUSH, this.configuration);
        return this;
    }

    public RedisSinkFunctionBuilder<T> setSinkListToHead(String valueName) {
        if (isNotEmpty(valueName)) {
            configuration.set(CUSTOM_VALUE_NAME, valueName);
        }
        this.redisMapper = new RowRedisSinkRowMapper(RedisCommand.LPUSH, this.configuration);
        return this;
    }

    public RedisSinkFunctionBuilder<T> setSinkSAdd(String valueName) {
        if (isNotEmpty(valueName)) {
            configuration.set(CUSTOM_VALUE_NAME, valueName);
        }
        this.redisMapper = new RowRedisSinkRowMapper(RedisCommand.SADD, this.configuration);
        return this;
    }

    public RedisSinkFunctionBuilder<T> setSinkZAdd(String scoreName,
                                                   String valueName) {
        if (isNotEmpty(scoreName)) {
            configuration.set(CUSTOM_SCORE_NAME, scoreName);
        }
        if (isNotEmpty(valueName)) {
            configuration.set(CUSTOM_VALUE_NAME, valueName);
        }
        this.redisMapper = new RowRedisSinkRowMapper(RedisCommand.ZADD, this.configuration);
        return this;
    }

    public RedisSinkFunction<T> build() {
        if (redisMapper == null) {
            throw new IllegalStateException("RedisMapper is required");
        }
        if (flinkConfigBase == null) {
            throw new IllegalStateException("FlinkConfigBase is required");
        }

        if (flinkConfigBase instanceof FlinkSingleConfig) {
            configuration.setString(REDIS_MODE, REDIS_SINGLE);
        } else if (flinkConfigBase instanceof FlinkSentinelConfig) {
            configuration.setString(REDIS_MODE, REDIS_SENTINEL);
        } else if (flinkConfigBase instanceof FlinkClusterConfig) {
            configuration.setString(REDIS_MODE, REDIS_CLUSTER);
        }

        return new RedisSinkFunction<>(flinkConfigBase, redisMapper, columnDataTypes, configuration);
    }
}