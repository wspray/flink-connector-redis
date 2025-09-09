package org.apache.flink.streaming.connectors.redis.stream;

import org.apache.commons.lang3.Validate;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.redis.command.RedisCommand;
import org.apache.flink.streaming.connectors.redis.config.FlinkClusterConfig;
import org.apache.flink.streaming.connectors.redis.config.FlinkConfigBase;
import org.apache.flink.streaming.connectors.redis.config.FlinkSentinelConfig;
import org.apache.flink.streaming.connectors.redis.config.FlinkSingleConfig;
import org.apache.flink.streaming.connectors.redis.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.config.RedisValueDataStructure;
import org.apache.flink.streaming.connectors.redis.mapper.RowRedisQueryMapper;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.flink.streaming.connectors.redis.command.RedisCommand.ZRANGEWITHSCORES;
import static org.apache.flink.streaming.connectors.redis.command.RedisCommand.ZSCORE;
import static org.apache.flink.streaming.connectors.redis.config.RedisOptions.FIELD;
import static org.apache.flink.streaming.connectors.redis.config.RedisOptions.KEY;
import static org.apache.flink.streaming.connectors.redis.config.RedisOptions.SCAN_ADDITION_KEY;
import static org.apache.flink.streaming.connectors.redis.config.RedisOptions.SCORE;
import static org.apache.flink.streaming.connectors.redis.config.RedisOptions.VALUE;
import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.REDIS_CLUSTER;
import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.REDIS_MODE;
import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.REDIS_SENTINEL;
import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.REDIS_SINGLE;

public class RedisSourceFunctionBuilder<T> {

    private RowRedisQueryMapper redisMapper;
    private Configuration configuration = new Configuration();
    private FlinkConfigBase flinkConfigBase;
    private Map<String, TypeInformation> map;

    private RowTypeInfo rowTypeInfo;

    public static RedisSourceFunctionBuilder<Row> builder() {
        return new RedisSourceFunctionBuilder<>();
    }

    public RedisSourceFunctionBuilder<T> setRedisMapper(RowRedisQueryMapper redisMapper) {
        this.redisMapper = redisMapper;
        return this;
    }

    public RedisSourceFunctionBuilder<T> setFlinkConfigBase(FlinkConfigBase flinkConfigBase) {
        this.flinkConfigBase = flinkConfigBase;
        return this;
    }

    public RedisSourceFunctionBuilder<T> setResolvedSchema(Map<String, TypeInformation> map) {
        this.map = map;
        return this;
    }

    public RedisSourceFunctionBuilder<T> setValueType(String valueType) {
        configuration.set(RedisOptions.VALUE_TYPE, valueType);
        return this;
    }

    public RedisSourceFunctionBuilder<T> setConfiguration(Configuration configuration) {
        this.configuration = configuration;
        return this;
    }

    public RedisSourceFunctionBuilder<T> setConnectionTimeout(int timeout) {
        configuration.set(RedisOptions.TIMEOUT, timeout);
        return this;
    }

    public RedisSourceFunctionBuilder<T> setKey(String key) {
        configuration.set(RedisOptions.SCAN_KEY, key);
        return this;
    }

    public RedisSourceFunctionBuilder<T> setScanCount(long scanCount) {
        configuration.set(RedisOptions.SCAN_COUNT, scanCount);
        return this;
    }

    public RedisSourceFunctionBuilder<T> setValueOnly(boolean valueOnly) {
        if (valueOnly) {
            configuration.set(RedisOptions.VALUE_DATA_STRUCTURE, RedisValueDataStructure.row);
        }
        return this;
    }

    public RedisSourceFunctionBuilder<T> setMaxRetries(int maxRetries) {
        configuration.set(RedisOptions.MAX_RETRIES, maxRetries);
        return this;
    }

    public RedisSourceFunctionBuilder<T> setQueryGet() {
        this.redisMapper = new RowRedisQueryMapper(RedisCommand.MGET);
        return this;
    }

    public RedisSourceFunctionBuilder<T> setQueryHGet(String field) {
        Validate.isTrue(isNotEmpty(field), "empty field");
        this.redisMapper = new RowRedisQueryMapper(RedisCommand.HGET);
        configuration.set(SCAN_ADDITION_KEY, field);
        return this;
    }

    public RedisSourceFunctionBuilder<T> setQueryHGetAll() {
        this.redisMapper = new RowRedisQueryMapper(RedisCommand.HGETALL);
        return this;
    }

    public RedisSourceFunctionBuilder<T> setQueryRange() {
        this.redisMapper = new RowRedisQueryMapper(RedisCommand.LRANGE);
        return this;
    }

    public RedisSourceFunctionBuilder<T> setQueryRange(int start, int stop) {
        this.redisMapper = new RowRedisQueryMapper(RedisCommand.LRANGE);
        configuration.set(RedisOptions.SCAN_RANGE_START, start);
        configuration.set(RedisOptions.SCAN_RANGE_STOP, stop);
        return this;
    }

    public RedisSourceFunctionBuilder<T> setQuerySMembers() {
        this.redisMapper = new RowRedisQueryMapper(RedisCommand.SMEMBERS);
        return this;
    }

    public RedisSourceFunctionBuilder<T> setQuerySRandMember(int count) {
        this.redisMapper = new RowRedisQueryMapper(RedisCommand.SADD);
        configuration.set(RedisOptions.SCAN_SRANDMEMBER_COUNT, count);
        return this;
    }

    public RedisSourceFunctionBuilder<T> setQueryZRangeWithScores() {
        this.redisMapper = new RowRedisQueryMapper(ZRANGEWITHSCORES);
        return this;
    }

    public RedisSourceFunctionBuilder<T> setQueryZScore(String member) {
        Validate.isTrue(isNotEmpty(member), "empty member");
        this.redisMapper = new RowRedisQueryMapper(ZSCORE);
        configuration.set(SCAN_ADDITION_KEY, member);
        return this;
    }

    public RedisSourceFunctionBuilder<T> checkAndInferType() {
        if (redisMapper == null) {
            throw new IllegalStateException("RedisMapper is required");
        }
        if (flinkConfigBase == null) {
            throw new IllegalStateException("FlinkConfigBase is required");
        }
        if (configuration.get(RedisOptions.SCAN_KEY) == null) {
            throw new IllegalStateException("SCAN_KEY is required");
        }
        if (flinkConfigBase instanceof FlinkSingleConfig) {
            configuration.setString(REDIS_MODE, REDIS_SINGLE);
        } else if (flinkConfigBase instanceof FlinkSentinelConfig) {
            configuration.setString(REDIS_MODE, REDIS_SENTINEL);
        } else if (flinkConfigBase instanceof FlinkClusterConfig) {
            configuration.setString(REDIS_MODE, REDIS_CLUSTER);
        }

        RedisCommand redisCommand = redisMapper.getRedisCommand();
        inferRowTypeInfo(redisCommand);

        return this;
    }

    public RedisSourceFunction<T> build() {
        return new RedisSourceFunction<>(redisMapper, configuration, flinkConfigBase, map);
    }

    public RowTypeInfo getRowTypeInfo() {
        return rowTypeInfo;
    }

    private void inferRowTypeInfo(RedisCommand redisCommand) {
        Map<String, TypeInformation> typeMap = new HashMap<>();
        typeMap.put(VALUE, Types.STRING);
        if (configuration.get(RedisOptions.VALUE_DATA_STRUCTURE) != RedisValueDataStructure.row) {
            typeMap.put(KEY, Types.STRING);

            if (redisCommand.name().contains("HGET")) {
                typeMap.put(FIELD, Types.STRING);

            } else if (redisCommand == ZSCORE || redisCommand == ZRANGEWITHSCORES) {
                typeMap.put(SCORE, Types.DOUBLE);
            }
        }

        if (map != null && !map.isEmpty()) {
            List<String> valueFieldNames = new ArrayList<>(map.keySet());
            for (String fieldName : valueFieldNames) {
                typeMap.put(fieldName, map.get(fieldName));
            }
        }
        rowTypeInfo = new RowTypeInfo(
                typeMap.values().toArray(new TypeInformation[0]),
                typeMap.keySet().toArray(new String[0])
        );
    }
}
