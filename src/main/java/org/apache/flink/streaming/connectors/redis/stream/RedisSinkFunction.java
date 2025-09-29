/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.redis.stream;

import io.lettuce.core.Range;
import io.lettuce.core.RedisFuture;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.redis.command.RedisCommand;
import org.apache.flink.streaming.connectors.redis.command.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.command.RedisInsertCommand;
import org.apache.flink.streaming.connectors.redis.config.FlinkConfigBase;
import org.apache.flink.streaming.connectors.redis.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.config.RedisValueDataStructure;
import org.apache.flink.streaming.connectors.redis.config.ZremType;
import org.apache.flink.streaming.connectors.redis.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.container.RedisCommandsContainerBuilder;
import org.apache.flink.streaming.connectors.redis.mapper.RedisSinkRowMapper;
import org.apache.flink.streaming.connectors.redis.stream.converter.RedisRowConverter;
import org.apache.flink.streaming.connectors.redis.table.RedisDynamicTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.streaming.connectors.redis.command.RedisInsertCommand.HSET;
import static org.apache.flink.streaming.connectors.redis.command.RedisInsertCommand.ZADD;
import static org.apache.flink.streaming.connectors.redis.config.RedisOptions.END;
import static org.apache.flink.streaming.connectors.redis.config.RedisOptions.FIELD;
import static org.apache.flink.streaming.connectors.redis.config.RedisOptions.KEY;
import static org.apache.flink.streaming.connectors.redis.config.RedisOptions.SCORE;
import static org.apache.flink.streaming.connectors.redis.config.RedisOptions.START;
import static org.apache.flink.streaming.connectors.redis.config.RedisOptions.VALUE;
import static org.apache.flink.streaming.connectors.redis.stream.PlaceholderReplacer.replaceByTag;

/**
 * A Redis sink function for stream API.
 */
public class RedisSinkFunction<IN> extends RichSinkFunction<IN> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisSinkFunction.class);
    private final int maxRetryTimes;
    private final boolean setIfAbsent;
    private final boolean ttlKeyNotAbsent;
    private final RedisSinkRowMapper<IN> redisSinkMapper;
    private final RedisCommand redisCommand;
    private final ReadableConfig readableConfig;
    private final FlinkConfigBase flinkConfigBase;
    private final List<TypeInformation> columnDataTypes;
    private final RedisValueDataStructure redisValueDataStructure;
    private final String zremrangeby;
    private final boolean auditLog;
    protected Integer ttl;
    protected int expireTimeSeconds = -1;
    private transient RedisCommandsContainer redisCommandsContainer;
    private static final ObjectMapper mapper = new ObjectMapper()
            .setDefaultPropertyInclusion(JsonInclude.Include.ALWAYS);

    /**
     * Creates a new {@link RedisSinkFunction} that connects to the Redis server.
     *
     * @param flinkConfigBase The configuration of {@link FlinkConfigBase}
     * @param redisSinkMapper This is used to generate Redis command and key value from incoming
     *                        elements.
     */
    public RedisSinkFunction(
            FlinkConfigBase flinkConfigBase,
            RedisSinkRowMapper<IN> redisSinkMapper,
            List<TypeInformation> columnDataTypes,
            ReadableConfig readableConfig) {
        Objects.requireNonNull(flinkConfigBase, "Redis connection pool config should not be null");
        Objects.requireNonNull(redisSinkMapper, "Redis Mapper can not be null");

        this.flinkConfigBase = flinkConfigBase;
        this.readableConfig = readableConfig;
        this.maxRetryTimes = readableConfig.get(RedisOptions.MAX_RETRIES);
        this.redisSinkMapper = redisSinkMapper;
        RedisCommandDescription redisCommandDescription =
                (RedisCommandDescription) redisSinkMapper.getCommandDescription();
        Preconditions.checkNotNull(
                redisCommandDescription, "Redis Mapper data type description can not be null");

        this.redisCommand = redisCommandDescription.getRedisCommand();
        this.ttl = redisCommandDescription.getTTL();
        this.ttlKeyNotAbsent = redisCommandDescription.getTtlKeyNotAbsent();
        this.setIfAbsent = redisCommandDescription.getSetIfAbsent();
        this.auditLog = redisCommandDescription.isAuditLog();
        if (redisCommandDescription.getExpireTime() != null) {
            this.expireTimeSeconds = redisCommandDescription.getExpireTime().toSecondOfDay();
        }

        this.columnDataTypes = columnDataTypes;
        this.redisValueDataStructure = readableConfig.get(RedisOptions.VALUE_DATA_STRUCTURE);
        this.zremrangeby = readableConfig.get(RedisOptions.ZREM_RANGEBY);
    }

    /**
     * Called when new data arrives to the sink, and forwards it to Redis channel. Depending on the
     * specified Redis data type, a different Redis command will be applied. Available commands are
     * RPUSH, LPUSH, SADD, PUBLISH, SET, SETEX, PFADD, HSET, ZADD.
     *
     * @param input The incoming data
     */
    @Override
    public void invoke(IN input, Context context) throws Exception {
        Row row = (Row) input;
        RowKind kind = row.getKind();
        if (kind == RowKind.UPDATE_BEFORE) {
            return;
        }

        String[] params = getParamsFrom(row);

        // the value is taken from the entire row when redisValueFromType is row, and columns
        // separated by '\01'
        if (redisValueDataStructure == RedisValueDataStructure.row) {
            params[params.length - 1] = serializeWholeRow(row);
        }

        startSink(params, kind);
        if (auditLog) {
            LOG.info("{}", row);
        }
    }

    private String[] getParamsFrom(Row row) {
        Boolean rowByNames = this.readableConfig.get(RedisOptions.ROW_BY_NAMES);
        if (rowByNames) {
            return calcParamByCommand(row);
        }

        String[] params = new String[calcParamNumByCommand(row.getArity())];
        for (int i = 0; i < params.length; i++) {
            params[i] =
                    redisSinkMapper.getKeyFromData(
                            row, columnDataTypes.get(i), i);
        }
        return params;
    }


    /**
     * It will try many times which less than {@code maxRetryTimes} until execute success.
     *
     * @param params
     * @throws Exception
     */
    private void startSink(String[] params, RowKind kind) throws Exception {
        for (int i = 0; i <= maxRetryTimes; i++) {
            try {
                // 设置了ttl并且开启了 仅ttl不存在时设置ttl时，需提前获取下原始ttl
                AtomicReference<Long> originalTtl = new AtomicReference<>();
                if (ttl != null && ttlKeyNotAbsent) {
                    // set ttl when key not absent
                    this.redisCommandsContainer
                            .getTTL(params[0])
                            .whenComplete(
                                    (t, h) -> {
                                        if (t > -1) {
                                            originalTtl.set(t);
                                        }
                                    });
                } else {
                    originalTtl.set(null);
                }

                RedisFuture redisFuture;
                if (kind == RowKind.DELETE) {
                    redisFuture = rowKindDelete(params);
                } else {
                    redisFuture = sink(params);
                }

//                if (redisFuture != null) {
//                    redisFuture.whenComplete((r, t) -> setTtl(params[0]));
//                }

                // sync process for instant job caused shut down server
                Object result = redisFuture.get();
                if (result != null) {
                    setTtl(params[0], originalTtl.get());
                }

                break;
            } catch (UnsupportedOperationException e) {
                throw e;
            } catch (Exception e1) {
                LOG.error("sink redis error, retry times:{}", i, e1);
                if (i >= this.maxRetryTimes) {
                    throw new RuntimeException("sink redis error ", e1);
                }
                Thread.sleep(500L * i);
            }
        }
    }

    /**
     * process redis command.
     *
     * @param params
     */
    private RedisFuture sink(String[] params) {
        RedisFuture redisFuture = null;
        switch (redisCommand.getInsertCommand()) {
            case RPUSH:
                redisFuture = this.redisCommandsContainer.rpush(params[0], params[1]);
                break;
            case LPUSH:
                redisFuture = this.redisCommandsContainer.lpush(params[0], params[1]);
                break;
            case SADD:
                redisFuture = this.redisCommandsContainer.sadd(params[0], params[1]);
                break;
            case SET: {
                if (!this.setIfAbsent) {
                    redisFuture = this.redisCommandsContainer.set(params[0], params[1]);
                } else {
                    redisFuture = this.redisCommandsContainer.exists(params[0]);
                    redisFuture.whenComplete(
                            (existsVal, throwable) -> {
                                if ((int) existsVal == 0) {
                                    this.redisCommandsContainer.set(params[0], params[1]);
                                }
                            });
                }
            }
            break;
            case PFADD:
                redisFuture = this.redisCommandsContainer.pfadd(params[0], params[1]);
                break;
            case PUBLISH:
                redisFuture = this.redisCommandsContainer.publish(params[0], params[1]);
                break;
            case ZADD:
                redisFuture =
                        this.redisCommandsContainer.zadd(
                                params[0], Double.parseDouble(params[1]), params[2]);
                if (zremrangeby != null) {
                    redisFuture.whenComplete(
                            (ignore, throwable) -> {
                                try {
                                    if (zremrangeby.equalsIgnoreCase(ZremType.SCORE.name())) {
                                        Range<Double> range =
                                                Range.create(
                                                        Double.parseDouble(params[3]),
                                                        Double.parseDouble(params[4]));
                                        this.redisCommandsContainer.zremRangeByScore(
                                                params[0], range);
                                    } else if (zremrangeby.equalsIgnoreCase(ZremType.LEX.name())) {
                                        Range<String> range = Range.create(params[3], params[4]);
                                        this.redisCommandsContainer.zremRangeByLex(
                                                params[0], range);
                                    } else if (zremrangeby.equalsIgnoreCase(ZremType.RANK.name())) {
                                        this.redisCommandsContainer.zremRangeByRank(
                                                params[0],
                                                Long.parseLong(params[3]),
                                                Long.parseLong(params[4]));
                                    } else {
                                        LOG.warn("Unrecognized zrem type:{}", zremrangeby);
                                    }
                                } catch (Exception e) {
                                    LOG.error("{} zremRangeBy failed.", params[0], e);
                                }
                            });
                }
                break;
            case ZINCRBY:
                redisFuture =
                        this.redisCommandsContainer.zincrBy(
                                params[0], Double.valueOf(params[1]), params[2]);
                break;
            case ZREM:
                redisFuture = this.redisCommandsContainer.zrem(params[0], params[1]);
                break;
            case SREM:
                redisFuture = this.redisCommandsContainer.srem(params[0], params[1]);
                break;
            case HSET: {
                if (!this.setIfAbsent) {
                    redisFuture =
                            this.redisCommandsContainer.hset(params[0], params[1], params[2]);
                } else {
                    redisFuture = this.redisCommandsContainer.hexists(params[0], params[1]);
                    redisFuture.whenComplete(
                            (exist, throwable) -> {
                                if (!(Boolean) exist) {
                                    this.redisCommandsContainer.hset(
                                            params[0], params[1], params[2]);
                                }
                            });
                }
            }
            break;
            case HMSET: {
                if (params.length < 2) {
                    throw new RuntimeException("params length must be greater than 2");
                }
                if (params.length % 2 != 1) {
                    throw new RuntimeException("params length must be odd");
                }
                // 遍历把params第一个下标作为key，从第二个下标作为value，存进map中
                Map<String, String> hashField = new HashMap<>();
                for (int i = 1; i < params.length; i++) {
                    hashField.put(params[i], params[++i]);
                }
                if (!this.setIfAbsent) {
                    redisFuture = this.redisCommandsContainer.hmset(params[0], hashField);
                } else {
                    redisFuture = this.redisCommandsContainer.exists(params[0]);
                    redisFuture.whenComplete(
                            (exist, throwable) -> {
                                if (!(Boolean) exist) {
                                    this.redisCommandsContainer.hmset(params[0], hashField);
                                }
                            });
                }
            }
            break;
            case HINCRBY:
                redisFuture =
                        this.redisCommandsContainer.hincrBy(
                                params[0], params[1], Long.valueOf(params[2]));
                break;
            case HINCRBYFLOAT:
                redisFuture =
                        this.redisCommandsContainer.hincrByFloat(
                                params[0], params[1], Double.valueOf(params[2]));
                break;
            case INCRBY:
                redisFuture =
                        this.redisCommandsContainer.incrBy(params[0], Long.valueOf(params[1]));
                break;
            case INCRBYFLOAT:
                redisFuture =
                        this.redisCommandsContainer.incrByFloat(
                                params[0], Double.valueOf(params[1]));
                break;
            case DECRBY:
                redisFuture =
                        this.redisCommandsContainer.decrBy(params[0], Long.valueOf(params[1]));
                break;
            case DEL:
                redisFuture = this.redisCommandsContainer.del(params[0]);
                break;
            case HDEL:
                redisFuture = this.redisCommandsContainer.hdel(params[0], params[1]);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Cannot process such data type: " + redisCommand);
        }
        return redisFuture;
    }

    /**
     * process redis command when RowKind == DELETE.
     *
     * @param params
     */
    private RedisFuture rowKindDelete(String[] params) {
        RedisFuture redisFuture = null;
        switch (redisCommand.getDeleteCommand()) {
            case SREM:
                redisFuture = this.redisCommandsContainer.srem(params[0], params[1]);
                break;
            case DEL:
                redisFuture = this.redisCommandsContainer.del(params[0]);
                break;
            case ZREM:
                redisFuture = this.redisCommandsContainer.zrem(params[0], params[2]);
                break;
            case ZINCRBY:
                Double d = -Double.valueOf(params[1]);
                redisFuture = this.redisCommandsContainer.zincrBy(params[0], d, params[2]);
                break;
            case HDEL: {
                redisFuture = this.redisCommandsContainer.hdel(params[0], params[1]);
            }
            break;
            case HINCRBY:
                redisFuture =
                        this.redisCommandsContainer.hincrBy(
                                params[0], params[1], -Long.valueOf(params[2]));
                break;
            case HINCRBYFLOAT:
                redisFuture =
                        this.redisCommandsContainer.hincrByFloat(
                                params[0], params[1], -Double.valueOf(params[2]));
                break;
            case INCRBY:
                redisFuture =
                        this.redisCommandsContainer.incrBy(params[0], -Long.valueOf(params[1]));
                break;
            case INCRBYFLOAT:
                redisFuture =
                        this.redisCommandsContainer.incrByFloat(
                                params[0], -Double.valueOf(params[1]));
                break;
        }
        return redisFuture;
    }

    /**
     * set ttl for key.
     *
     * @param key
     * @param remainTtl
     */
    private void setTtl(String key, Long remainTtl) {
        if (redisCommand == RedisCommand.DEL) {
            return;
        }

        if (ttl != null) {
            if (ttlKeyNotAbsent && remainTtl != null) {
                this.redisCommandsContainer.expire(key, remainTtl.intValue());
            } else {
                // set ttl every sink
                this.redisCommandsContainer.expire(key, ttl);
            }
        } else if (expireTimeSeconds != -1) {
            this.redisCommandsContainer
                    .getTTL(key)
                    .whenComplete(
                            (t, h) -> {
                                if (t < 0) {
                                    int now = LocalTime.now().toSecondOfDay();
                                    this.redisCommandsContainer.expire(
                                            key,
                                            expireTimeSeconds > now
                                                    ? expireTimeSeconds - now
                                                    : 86400 + expireTimeSeconds - now);
                                }
                            });
        } else if (ttl != null) {
            this.redisCommandsContainer.expire(key, ttl);
        }
    }

    /**
     * serialize whole row.
     *
     * @param row
     * @return
     */
    private String serializeWholeRow(Row row) {
        Boolean rowByNames = this.readableConfig.get(RedisOptions.ROW_BY_NAMES);
        if (rowByNames) {
            return getAllValueStr(row);
        }

        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < columnDataTypes.size(); i++) {
            stringBuilder.append(
                    RedisRowConverter.rowDataToString(
                            columnDataTypes.get(i), row, i));
            if (i != columnDataTypes.size() - 1) {
                stringBuilder.append(RedisDynamicTableFactory.CACHE_SEPERATOR);
            }
        }
        return stringBuilder.toString();
    }

    private String getAllValueStr(Row row) {
        Map<String, Object> map = new LinkedHashMap<>(row.getArity());
        Set<String> fieldNames = row.getFieldNames(true);
        if (!CollectionUtil.isNullOrEmpty(fieldNames)) {
            fieldNames.forEach(fieldName -> {
                map.put(fieldName, row.getField(fieldName));
            });
        }
        try {
            return mapper.writeValueAsString(map);
        } catch (Exception e) {
            LOG.error("JsonProcessing Exception when getAllValueStr", e);
        }
        return row.toString();
    }

    /**
     * calculate the number of redis command's param
     *
     * @return
     */
    private int calcParamNumByCommand(int rowArity) {
        if (redisCommand == RedisCommand.DEL) {
            return 1;
        }

        if (redisCommand.getInsertCommand() == ZADD && zremrangeby != null) {
            return 5;
        } else if (redisCommand.getInsertCommand() == HSET
                || redisCommand.getInsertCommand() == ZADD
                || redisCommand.getInsertCommand() == RedisInsertCommand.HINCRBY
                || redisCommand.getInsertCommand() == RedisInsertCommand.HINCRBYFLOAT
                || redisCommand.getInsertCommand() == RedisInsertCommand.ZINCRBY) {
            return 3;
        } else if (redisCommand.getInsertCommand() == RedisInsertCommand.HMSET) {
            return rowArity;
        }

        return 2;
    }

    private String[] calcParamByCommand(Row row) {
        List<String> params = new ArrayList<>();
        String keyField = this.readableConfig.get(RedisOptions.CUSTOM_KEY_NAME);
        String realKeyField = replaceByTag(row, keyField, KEY);

        params.add(realKeyField);

        if (redisCommand == RedisCommand.DEL) {
            return params.toArray(new String[0]);
        }

        if (redisCommand.getInsertCommand() == ZADD) {
            String scoreField = this.readableConfig.get(RedisOptions.CUSTOM_SCORE_NAME);
            String realScoreField = replaceByTag(row, scoreField, SCORE);
            params.add(realScoreField);

            String valueField = this.readableConfig.get(RedisOptions.CUSTOM_VALUE_NAME);
            String realValueField = replaceByTag(row, valueField, VALUE);
            params.add(realValueField);

            if (zremrangeby != null) {
                String startField = this.readableConfig.get(RedisOptions.CUSTOM_START);
                String realStartField = replaceByTag(row, startField, START);
                params.add(realStartField);

                String endField = this.readableConfig.get(RedisOptions.CUSTOM_END);
                String realEndField = replaceByTag(row, endField, END);
                params.add(realEndField);
            }
            return params.toArray(new String[0]);
        } else if (redisCommand.getInsertCommand() == HSET
                || redisCommand.getInsertCommand() == RedisInsertCommand.HINCRBY
                || redisCommand.getInsertCommand() == RedisInsertCommand.HINCRBYFLOAT) {
            String fieldField = this.readableConfig.get(RedisOptions.CUSTOM_FIELD_NAME);
            String realFieldField = replaceByTag(row, fieldField, FIELD);
            params.add(realFieldField);

            String valueField = this.readableConfig.get(RedisOptions.CUSTOM_VALUE_NAME);
            String realValueField = replaceByTag(row, valueField, VALUE);
            params.add(realValueField);
            return params.toArray(new String[0]);

        } else if (redisCommand.getInsertCommand() == RedisInsertCommand.HMSET) {
            Set<String> fieldNames = row.getFieldNames(true);
            for (String fieldName : fieldNames) {
                params.add(replaceByTag(row, fieldName, fieldName));
            }
            return params.toArray(new String[0]);
        }

        String valueField = this.readableConfig.get(RedisOptions.CUSTOM_VALUE_NAME);
        String realValueField = replaceByTag(row, valueField, VALUE);
        params.add(realValueField);
        return params.toArray(new String[0]);
    }

    /**
     * Initializes the connection to Redis by either cluster or sentinels or single server.
     *
     * @throws IllegalArgumentException if PoolConfig, ClusterConfig and SentinelConfig are all null
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        Preconditions.checkArgument(
                redisCommand.getInsertCommand() != RedisInsertCommand.NONE,
                "the command %s do not support insert.",
                redisCommand.name());

        try {
            this.redisCommandsContainer = RedisCommandsContainerBuilder.build(this.flinkConfigBase);
            this.redisCommandsContainer.open();
            LOG.info("success to create redis container for sink");
        } catch (Exception e) {
            LOG.error("Redis has not been properly initialized: ", e);
            throw e;
        }
    }

    /**
     * Closes commands container.
     *
     * @throws IOException if command container is unable to close.
     */
    @Override
    public void close() throws IOException {
        if (redisCommandsContainer != null) {
            redisCommandsContainer.close();
        }
    }
}
