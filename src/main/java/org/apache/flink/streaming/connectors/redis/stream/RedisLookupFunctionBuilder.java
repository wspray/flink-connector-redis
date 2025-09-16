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
import org.apache.flink.streaming.connectors.redis.config.RedisJoinConfig;
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
import static org.apache.flink.streaming.connectors.redis.config.RedisOptions.MERGE_BY_OVERWRITE;
import static org.apache.flink.streaming.connectors.redis.config.RedisOptions.SCORE;
import static org.apache.flink.streaming.connectors.redis.config.RedisOptions.VALUE;
import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.REDIS_CLUSTER;
import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.REDIS_MODE;
import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.REDIS_SENTINEL;
import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.REDIS_SINGLE;

public class RedisLookupFunctionBuilder<T> {

    private RowRedisQueryMapper redisMapper;
    private Configuration configuration = new Configuration();
    private FlinkConfigBase flinkConfigBase;
    private Map<String, TypeInformation> map;
    private RedisJoinConfig joinConfig;
    private RowTypeInfo rowTypeInfo;

    public static RedisLookupFunctionBuilder<Row> builder() {
        return new RedisLookupFunctionBuilder<>();
    }

    public RedisLookupFunctionBuilder<T> setRedisMapper(RowRedisQueryMapper redisMapper) {
        this.redisMapper = redisMapper;
        return this;
    }

    public RedisLookupFunctionBuilder<T> setFlinkConfigBase(FlinkConfigBase flinkConfigBase) {
        this.flinkConfigBase = flinkConfigBase;
        return this;
    }

    public RedisLookupFunctionBuilder<T> setResolvedSchema(Map<String, TypeInformation> map) {
        this.map = map;
        return this;
    }

    public RedisLookupFunctionBuilder<T> setValueType(String valueType) {
        configuration.set(RedisOptions.VALUE_TYPE, valueType);
        return this;
    }

    public RedisLookupFunctionBuilder<T> setRedisJoinConfig(RedisJoinConfig joinConfig) {
        this.joinConfig = joinConfig;
        return this;
    }

    public RedisLookupFunctionBuilder<T> setConfiguration(Configuration configuration) {
        this.configuration = configuration;
        return this;
    }

    public RedisLookupFunctionBuilder<T> setPassword(String password) {
        configuration.set(RedisOptions.PASSWORD, password);
        return this;
    }

    public RedisLookupFunctionBuilder<T> setConnectionTimeout(int timeout) {
        configuration.set(RedisOptions.TIMEOUT, timeout);
        return this;
    }

    public RedisLookupFunctionBuilder<T> setKeyName(String keyName) {
        configuration.set(RedisOptions.CUSTOM_KEY_NAME, keyName);
        return this;
    }

    public RedisLookupFunctionBuilder<T> setValueName(String valueName) {
        configuration.set(RedisOptions.CUSTOM_VALUE_NAME, valueName);
        return this;
    }

    public RedisLookupFunctionBuilder<T> setValueOnly(boolean valueOnly) {
        if (valueOnly) {
            configuration.set(RedisOptions.VALUE_DATA_STRUCTURE, RedisValueDataStructure.row);
        }
        return this;
    }

    public RedisLookupFunctionBuilder<T> setMaxRetries(int maxRetries) {
        configuration.set(RedisOptions.MAX_RETRIES, maxRetries);
        return this;
    }

    public RedisLookupFunctionBuilder<T> setMergeByOverwrite(boolean mergeByOverwrite) {
        configuration.set(MERGE_BY_OVERWRITE, mergeByOverwrite);
        return this;
    }

    public RedisLookupFunctionBuilder<T> setQueryGet() {
        this.redisMapper = new RowRedisQueryMapper(RedisCommand.GET);
        return this;
    }

    public RedisLookupFunctionBuilder<T> setQueryHGet(String field) {
        Validate.isTrue(isNotEmpty(field), "empty field");
        this.redisMapper = new RowRedisQueryMapper(RedisCommand.HGET);
        configuration.set(RedisOptions.CUSTOM_FIELD_NAME, field);
        return this;
    }

    public RedisLookupFunctionBuilder<T> setQueryHGetAll(String field) {
        Validate.isTrue(isNotEmpty(field), "empty field");
        this.redisMapper = new RowRedisQueryMapper(RedisCommand.HGETALL);
        configuration.set(RedisOptions.CUSTOM_FIELD_NAME, field);
        return this;
    }

    public RedisLookupFunctionBuilder<T> setQueryZScore(String member) {
        Validate.isTrue(isNotEmpty(member), "empty member");
        this.redisMapper = new RowRedisQueryMapper(ZSCORE);
        configuration.set(RedisOptions.CUSTOM_VALUE_NAME, member);
        return this;
    }

    public RedisLookupFunctionBuilder<T> checkAndInferType(RowTypeInfo inputRowTypeInfo) {
        if (redisMapper == null) {
            throw new IllegalStateException("RedisMapper is required");
        }
        if (flinkConfigBase == null) {
            throw new IllegalStateException("FlinkConfigBase is required");
        }
        if (configuration.get(RedisOptions.CUSTOM_KEY_NAME) == null) {
            throw new IllegalStateException("custom_key_name is required");
        }
        if (flinkConfigBase instanceof FlinkSingleConfig) {
            configuration.setString(REDIS_MODE, REDIS_SINGLE);
        } else if (flinkConfigBase instanceof FlinkSentinelConfig) {
            configuration.setString(REDIS_MODE, REDIS_SENTINEL);
        } else if (flinkConfigBase instanceof FlinkClusterConfig) {
            configuration.setString(REDIS_MODE, REDIS_CLUSTER);
        }

        RedisCommand redisCommand = redisMapper.getRedisCommand();
        inferRowTypeInfo(redisCommand, inputRowTypeInfo);

        return this;
    }

    public RedisLookupFunction build() {
        if (joinConfig == null) {
            joinConfig = new RedisJoinConfig.Builder().build(); // no cache
        }
        return new RedisLookupFunction(redisMapper, configuration, flinkConfigBase, map, joinConfig);
    }

    public RowTypeInfo getRowTypeInfo() {
        return rowTypeInfo;
    }

    private void inferRowTypeInfo(RedisCommand redisCommand, RowTypeInfo inputRowTypeInfo) {
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

        RowTypeInfo valueTypeInfo = new RowTypeInfo(
                typeMap.values().toArray(new TypeInformation[0]),
                typeMap.keySet().toArray(new String[0])
        );
        rowTypeInfo = mergeTypeInfo(inputRowTypeInfo, valueTypeInfo);
    }

    private RowTypeInfo mergeTypeInfo(RowTypeInfo rowTypeInfo,
                                      RowTypeInfo valueTypeInfo) {
        Map<String, TypeInformation<?>> rowFields = new HashMap<>();
        for (int i = 0; i < rowTypeInfo.getFieldNames().length; i++) {
            rowFields.put(rowTypeInfo.getFieldNames()[i], rowTypeInfo.getFieldTypes()[i]);
        }

        Map<String, TypeInformation<?>> valueFields = new HashMap<>();
        for (int i = 0; i < valueTypeInfo.getFieldNames().length; i++) {
            String fieldName = valueTypeInfo.getFieldNames()[i];
            if (!configuration.get(RedisOptions.MERGE_BY_OVERWRITE) && rowFields.containsKey(fieldName)) {
                continue;
            }
            valueFields.put(fieldName, valueTypeInfo.getFieldTypes()[i]);
        }

        Map<String, TypeInformation<?>> allFields = new HashMap<>();
        allFields.putAll(rowFields);
        allFields.putAll(valueFields);

        return new RowTypeInfo(
                allFields.values().toArray(new TypeInformation[0]),
                allFields.keySet().toArray(new String[0])
        );
    }
}
