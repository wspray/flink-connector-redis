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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.calcite.shaded.com.google.common.cache.Cache;
import org.apache.flink.calcite.shaded.com.google.common.cache.CacheBuilder;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.connectors.redis.command.RedisCommand;
import org.apache.flink.streaming.connectors.redis.command.RedisCommandBaseDescription;
import org.apache.flink.streaming.connectors.redis.command.RedisJoinCommand;
import org.apache.flink.streaming.connectors.redis.command.RedisSelectCommand;
import org.apache.flink.streaming.connectors.redis.config.FlinkConfigBase;
import org.apache.flink.streaming.connectors.redis.config.RedisJoinConfig;
import org.apache.flink.streaming.connectors.redis.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.config.RedisValueDataStructure;
import org.apache.flink.streaming.connectors.redis.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.container.RedisCommandsContainerBuilder;
import org.apache.flink.streaming.connectors.redis.mapper.RedisMapper;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.streaming.connectors.redis.table.RedisDynamicTableFactory.CACHE_SEPERATOR;

/**
 * redis lookup function for DataStream API.
 */
public class RedisLookupFunction extends RichAsyncFunction<Row, Row> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisLookupFunction.class);

    private RedisCommand redisCommand;
    private ReadableConfig readableConfig;
    private FlinkConfigBase flinkConfigBase;
    private RedisCommandsContainer redisCommandsContainer;

    private final long cacheMaxSize;
    private final long cacheTtl;
    private final int maxRetryTimes;
    private final Map<String, TypeInformation> dataTypes;
    private final RedisValueDataStructure redisValueDataStructure;
    private Cache<String, Object> cache;
    private String[] queryParameter;

    public RedisLookupFunction(
            FlinkConfigBase flinkConfigBase,
            RedisMapper redisMapper,
            RedisJoinConfig redisJoinConfig,
            Map<String, TypeInformation> dataTypes,
            ReadableConfig readableConfig) {
        Preconditions.checkNotNull(
                flinkConfigBase, "Redis connection pool config should not be null");
        Preconditions.checkNotNull(redisMapper, "Redis Mapper can not be null");

        this.flinkConfigBase = flinkConfigBase;
        this.readableConfig = readableConfig;
        this.cacheTtl = redisJoinConfig.getCacheTtl();
        this.cacheMaxSize = redisJoinConfig.getCacheMaxSize();
        this.maxRetryTimes = readableConfig.get(RedisOptions.MAX_RETRIES);
        this.redisValueDataStructure = readableConfig.get(RedisOptions.VALUE_DATA_STRUCTURE);

        RedisCommandBaseDescription redisCommandDescription = redisMapper.getCommandDescription();
        Preconditions.checkNotNull(
                redisCommandDescription, "Redis Mapper data type description can not be null");
        this.redisCommand = redisCommandDescription.getRedisCommand();

        this.dataTypes = dataTypes;
    }

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) throws Exception {
        Object[] keys = new Object[input.getArity()];
        for (int i = 0; i < input.getArity(); i++) {
            keys[i] = input.getField(i);
        }

        // when use cache.
        if (cache != null) {
            Row rowData = null;
            switch (redisCommand.getJoinCommand()) {
                case GET:
                    rowData = (Row) cache.getIfPresent(String.valueOf(keys[0]));
                    break;
                case HGET:
                    String key =
                            new StringBuilder(String.valueOf(keys[0]))
                                    .append(CACHE_SEPERATOR)
                                    .append(String.valueOf(keys[1]))
                                    .toString();
                    rowData = (Row) cache.getIfPresent(key);
                    break;
                case HGETALL:
                    Map<String, String> map =
                            (Map<String, String>) cache.getIfPresent(String.valueOf(keys[0]));
                    if (map != null) {
                        resultFuture.complete(
                                Collections.singleton(
                                        RedisResultWrapper.createRowDataForHash(
                                                keys,
                                                map.get(String.valueOf(keys[1])),
                                                redisValueDataStructure,
                                                dataTypes)));
                        return;
                    }
                    break;
                case ZSCORE: {
                    String keyForZScore =
                            new StringBuilder(String.valueOf(keys[0]))
                                    .append(CACHE_SEPERATOR)
                                    .append(String.valueOf(keys[1]))
                                    .toString();
                    rowData = (Row) cache.getIfPresent(keyForZScore);
                    break;
                }
                default:
            }

            // when cache is not null.
            if (rowData != null) {
                resultFuture.complete(Collections.singleton(rowData));
                return;
            }
        }

        // It will try many times which less than {@code maxRetryTimes} until execute success.
        for (int i = 0; i <= maxRetryTimes; i++) {
            try {
                query(resultFuture, keys);
                break;
            } catch (Exception e) {
                LOG.error("query redis error, retry times:{}", i, e);
                if (i >= maxRetryTimes) {
                    throw new RuntimeException("query redis error ", e);
                }
                Thread.sleep(500 * i);
            }
        }
    }

    /**
     * query redis.
     *
     * @param keys
     * @throws Exception
     */
    private void query(ResultFuture<Row> resultFuture, Object... keys) {
        switch (redisCommand.getJoinCommand()) {
            case GET: {
                this.redisCommandsContainer
                        .get(String.valueOf(keys[0]))
                        .thenAccept(
                                result -> {
                                    Row rowData =
                                            RedisResultWrapper.createRowDataForString(
                                                    keys,
                                                    result,
                                                    redisValueDataStructure,
                                                    dataTypes);
                                    resultFuture.complete(Collections.singleton(rowData));
                                    if (cache != null && result != null) {
                                        cache.put(String.valueOf(keys[0]), rowData);
                                    }
                                });

                break;
            }
            case HGET: {
                this.redisCommandsContainer
                        .hget(String.valueOf(keys[0]), String.valueOf(keys[1]))
                        .thenAccept(
                                result -> {
                                    Row rowData =
                                            RedisResultWrapper.createRowDataForHash(
                                                    keys,
                                                    result,
                                                    redisValueDataStructure,
                                                    dataTypes);
                                    resultFuture.complete(Collections.singleton(rowData));
                                    if (cache != null && result != null) {
                                        String key =
                                                new StringBuilder(String.valueOf(keys[0]))
                                                        .append(CACHE_SEPERATOR)
                                                        .append(String.valueOf(keys[1]))
                                                        .toString();
                                        cache.put(key, rowData);
                                    }
                                });

                break;
            }
            case HGETALL:
                loadAllElementsForMap(resultFuture, keys);
                return;
            case ZSCORE: {
                this.redisCommandsContainer
                        .zscore(String.valueOf(keys[0]), String.valueOf(keys[1]))
                        .thenAccept(
                                result -> {
                                    Row rowData =
                                            RedisResultWrapper.createRowDataForSortedSet(
                                                    keys, result, redisValueDataStructure);
                                    resultFuture.complete(Collections.singleton(rowData));
                                    if (cache != null && result != null) {
                                        String key =
                                                new StringBuilder(String.valueOf(keys[0]))
                                                        .append(CACHE_SEPERATOR)
                                                        .append(String.valueOf(keys[1]))
                                                        .toString();
                                        cache.put(key, rowData);
                                    }
                                });
                break;
            }
            default:
        }
    }

    /**
     * load all element in memory from map.
     *
     * @param keys
     */
    private void loadAllElementsForMap(
            ResultFuture<Row> resultFuture, Object... keys) {
        this.redisCommandsContainer
                .hgetAll(String.valueOf(keys[0]))
                .thenAccept(
                        map -> {
                            cache.put(String.valueOf(keys[0]), map);
                            resultFuture.complete(
                                    Collections.singleton(
                                            RedisResultWrapper.createRowDataForHash(
                                                    keys,
                                                    map.get(String.valueOf(keys[1])),
                                                    redisValueDataStructure,
                                                    dataTypes)));
                        });
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);

        Preconditions.checkArgument(
                redisCommand.getJoinCommand() != RedisJoinCommand.NONE,
                String.format("the command %s do not support join.", redisCommand.name()));

        if (redisCommand.getJoinCommand() == RedisJoinCommand.HGETALL) {
            Preconditions.checkArgument(
                    cacheMaxSize != -1 && cacheTtl != -1,
                    "cache must be opened by cacheMaxSize and cacheTtl when you want to load all elements to cache.");
        }

        validator();
        this.queryParameter = new String[2];
        this.queryParameter[0] = this.readableConfig.get(RedisOptions.SCAN_KEY);

        if (redisCommand.getJoinCommand() == RedisJoinCommand.HGET) {
            this.queryParameter[1] = this.readableConfig.get(RedisOptions.SCAN_ADDITION_KEY);
        } else if (redisCommand.getJoinCommand() == RedisJoinCommand.ZSCORE) {
            this.queryParameter[1] = this.readableConfig.get(RedisOptions.SCAN_ADDITION_KEY);
        }

        try {
            this.redisCommandsContainer = RedisCommandsContainerBuilder.build(this.flinkConfigBase);
            this.redisCommandsContainer.open();
            LOG.info("success to create redis container.");
        } catch (Exception e) {
            LOG.error("Redis has not been properly initialized: ", e);
            throw e;
        }

        this.cache =
                cacheMaxSize == -1 || cacheTtl == -1
                        ? null
                        : CacheBuilder.newBuilder()
                        .expireAfterWrite(cacheTtl, TimeUnit.SECONDS)
                        .maximumSize(cacheMaxSize)
                        .build();
    }

    @Override
    public void close() throws Exception {
        if (redisCommandsContainer != null) {
            redisCommandsContainer.close();
        }

        if (cache != null) {
            cache.cleanUp();
            cache = null;
        }
    }

    private void validator() {
        Preconditions.checkNotNull(
                this.readableConfig.get(RedisOptions.SCAN_KEY),
                "the %s for source can not be null",
                RedisOptions.SCAN_KEY.key());

        Preconditions.checkArgument(
                redisCommand.getSelectCommand() != RedisSelectCommand.NONE,
                String.format("the command %s do not support query.", redisCommand.name()));

        if (redisCommand.getSelectCommand() == RedisSelectCommand.HGET) {
            Preconditions.checkNotNull(
                    this.readableConfig.get(RedisOptions.SCAN_ADDITION_KEY),
                    "must set field value of Map to %s",
                    RedisOptions.SCAN_ADDITION_KEY.key());
        } else if (redisCommand.getSelectCommand() == RedisSelectCommand.ZSCORE) {
            Preconditions.checkNotNull(
                    this.readableConfig.get(RedisOptions.SCAN_ADDITION_KEY),
                    "must set member value of SortedSet to %s",
                    RedisOptions.SCAN_ADDITION_KEY.key());
//        } else if (redisCommand.getSelectCommand() == RedisSelectCommand.LRANGE) {
//            Preconditions.checkNotNull(
//                    this.readableConfig.get(RedisOptions.SCAN_RANGE_START),
//                    "the %s must not be null when query list",
//                    RedisOptions.SCAN_RANGE_START.key());
//
//            Preconditions.checkNotNull(
//                    this.readableConfig.get(RedisOptions.SCAN_RANGE_STOP),
//                    "the %s must not be null when query list",
//                    RedisOptions.SCAN_RANGE_STOP.key());
        } else if (redisCommand.getSelectCommand() == RedisSelectCommand.SRANDMEMBER) {
            Preconditions.checkNotNull(
                    this.readableConfig.get(RedisOptions.SCAN_SRANDMEMBER_COUNT),
                    "the %s must not be null when query set",
                    RedisOptions.SCAN_SRANDMEMBER_COUNT.key());
        }
    }
}
