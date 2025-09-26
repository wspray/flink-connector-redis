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

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.guava31.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheBuilder;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.connectors.redis.command.RedisCommand;
import org.apache.flink.streaming.connectors.redis.command.RedisCommandBaseDescription;
import org.apache.flink.streaming.connectors.redis.command.RedisJoinCommand;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.BooleanUtils.TRUE;
import static org.apache.flink.streaming.connectors.redis.command.RedisCommand.ZRANGEWITHSCORES;
import static org.apache.flink.streaming.connectors.redis.command.RedisCommand.ZSCORE;
import static org.apache.flink.streaming.connectors.redis.config.RedisOptions.FIELD;
import static org.apache.flink.streaming.connectors.redis.config.RedisOptions.KEY;
import static org.apache.flink.streaming.connectors.redis.config.RedisOptions.SCORE;
import static org.apache.flink.streaming.connectors.redis.config.RedisOptions.VALUE;
import static org.apache.flink.streaming.connectors.redis.stream.PlaceholderReplacer.replaceByTag;
import static org.apache.flink.streaming.connectors.redis.stream.RedisResultArrayWrapper.isJsonArray;
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
    private final Map<String, String> valueOverwriteMap;
    private final boolean valueTypeJson;
    private final Map<String, TypeInformation> dataTypes;
    private final RedisValueDataStructure redisValueDataStructure;
    private Cache<String, Object> cache;

    public RedisLookupFunction(
            RedisMapper redisMapper,
            ReadableConfig readableConfig,
            FlinkConfigBase flinkConfigBase,
            Map<String, TypeInformation> dataTypes,
            RedisJoinConfig redisJoinConfig) {
        Preconditions.checkNotNull(
                flinkConfigBase, "Redis connection pool config should not be null");
        Preconditions.checkNotNull(redisMapper, "Redis Mapper can not be null");

        this.flinkConfigBase = flinkConfigBase;
        this.readableConfig = readableConfig;
        this.cacheTtl = redisJoinConfig.getCacheTtl();
        this.cacheMaxSize = redisJoinConfig.getCacheMaxSize();
        this.maxRetryTimes = readableConfig.get(RedisOptions.MAX_RETRIES);
        this.valueOverwriteMap = readableConfig.get(RedisOptions.MERGE_BY_OVERWRITE);
        this.redisValueDataStructure = readableConfig.get(RedisOptions.VALUE_DATA_STRUCTURE);
        this.valueTypeJson = readableConfig.get(RedisOptions.VALUE_TYPE).equalsIgnoreCase("json");

        RedisCommandBaseDescription redisCommandDescription = redisMapper.getCommandDescription();
        Preconditions.checkNotNull(
                redisCommandDescription, "Redis Mapper data type description can not be null");
        this.redisCommand = redisCommandDescription.getRedisCommand();

        this.dataTypes = dataTypes;
    }

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) {
        try {
            asyncInvokeRow(input, resultFuture);
        } catch (Exception e) {
            resultFuture.complete(Collections.singleton(expandDefaultRow(input)));
        }
    }

    @Override
    public void timeout(Row input, ResultFuture<Row> resultFuture) throws Exception {
        resultFuture.complete(Collections.singleton(expandDefaultRow(input)));
    }

    public void asyncInvokeRow(Row input, ResultFuture<Row> resultFuture) throws Exception {
        String[] queryParameter = calcParamByCommand(input);

        // when use cache.
        if (cache != null) {
            Object rowObj = null;
            switch (redisCommand.getJoinCommand()) {
                case GET:
                    rowObj = cache.getIfPresent(queryParameter[0]);
                    break;
                case HGET:
                    String key =
                            new StringBuilder(queryParameter[0])
                                    .append(CACHE_SEPERATOR)
                                    .append(queryParameter[1])
                                    .toString();
                    rowObj = cache.getIfPresent(key);
                    break;
                case HGETALL:
                    Map<String, String> map =
                            (Map<String, String>) cache.getIfPresent(queryParameter[0]);
                    if (map != null) {
                        String result = map.get(queryParameter[1]);
                        if (valueTypeJson && isJsonArray(result)) {
                            List<Row> rows = RedisResultArrayWrapper.createRowDataForHash(
                                    queryParameter,
                                    result,
                                    redisValueDataStructure,
                                    dataTypes);
                            resultFuture.complete(rows.stream()
                                    .map(r -> mergeRow(input, r))
                                    .collect(Collectors.toList()));
                            return;
                        }
                        resultFuture.complete(Collections.singleton(
                                mergeRow(input, RedisResultWrapper.createRowDataForHash(
                                        queryParameter,
                                        result,
                                        redisValueDataStructure,
                                        dataTypes))));
                        return;
                    }
                    break;
                case ZSCORE: {
                    String keyForZScore =
                            new StringBuilder(queryParameter[0])
                                    .append(CACHE_SEPERATOR)
                                    .append(queryParameter[1])
                                    .toString();
                    rowObj = cache.getIfPresent(keyForZScore);
                    break;
                }
                default:
            }

            // when cache is not null.
            if (rowObj != null) {
                if (rowObj instanceof Row) {
                    resultFuture.complete(Collections.singleton(mergeRow(input, (Row) rowObj)));
                }
                if (rowObj instanceof List) {
                    List<Row> rows = (List) rowObj;
                    resultFuture.complete(rows.stream()
                            .map(r -> mergeRow(input, r))
                            .collect(Collectors.toList()));
                }
                return;
            }
        }

        // It will try many times which less than {@code maxRetryTimes} until execute success.
        for (int i = 0; i <= maxRetryTimes; i++) {
            try {
                query(input, resultFuture, queryParameter);
                Thread.sleep(3000L);
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
     * @param queryParameter
     * @throws Exception
     */
    private void query(Row input, ResultFuture<Row> resultFuture, String... queryParameter) {
        switch (redisCommand.getJoinCommand()) {
            case GET: {
                this.redisCommandsContainer
                        .get(queryParameter[0])
                        .thenAccept(
                                result -> {
                                    if (valueTypeJson && isJsonArray(String.valueOf(result))) {
                                        List<Row> rows =
                                                RedisResultArrayWrapper.createRowDataForString(
                                                        queryParameter,
                                                        result,
                                                        redisValueDataStructure,
                                                        dataTypes);

                                        resultFuture.complete(rows.stream()
                                                .map(r -> mergeRow(input, r))
                                                .collect(Collectors.toList()));
                                        if (cache != null && result != null) {
                                            cache.put(queryParameter[0], rows);
                                        }
                                        return;
                                    }
                                    Row row =
                                            RedisResultWrapper.createRowDataForString(
                                                    queryParameter,
                                                    result,
                                                    redisValueDataStructure,
                                                    dataTypes);
                                    resultFuture.complete(Collections.singleton(mergeRow(input, row)));
                                    if (cache != null && result != null) {
                                        cache.put(queryParameter[0], row);
                                    }
                                });

                break;
            }
            case HGET: {
                this.redisCommandsContainer
                        .hget(queryParameter[0], queryParameter[1])
                        .thenAccept(
                                result -> {
                                    if (valueTypeJson && isJsonArray(String.valueOf(result))) {
                                        List<Row> rows =
                                                RedisResultArrayWrapper.createRowDataForHash(
                                                        queryParameter,
                                                        result,
                                                        redisValueDataStructure,
                                                        dataTypes);
                                        resultFuture.complete(rows.stream()
                                                .map(r -> mergeRow(input, r))
                                                .collect(Collectors.toList()));
                                        if (cache != null && result != null) {
                                            String keyForHash =
                                                    new StringBuilder(queryParameter[0])
                                                            .append(CACHE_SEPERATOR)
                                                            .append(queryParameter[1])
                                                            .toString();
                                            cache.put(keyForHash, rows);
                                        }
                                        return;
                                    }
                                    Row row =
                                            RedisResultWrapper.createRowDataForHash(
                                                    queryParameter,
                                                    result,
                                                    redisValueDataStructure,
                                                    dataTypes);
                                    resultFuture.complete(Collections.singleton(mergeRow(input, row)));
                                    if (cache != null && result != null) {
                                        String keyForHash =
                                                new StringBuilder(queryParameter[0])
                                                        .append(CACHE_SEPERATOR)
                                                        .append(queryParameter[1])
                                                        .toString();
                                        cache.put(keyForHash, row);
                                    }
                                });

                break;
            }
            case HGETALL:
                this.redisCommandsContainer
                        .hgetAll(queryParameter[0])
                        .thenAccept(
                                map -> {
                                    cache.put(queryParameter[0], map);
                                    String result = map.get(queryParameter[1]);
                                    if (valueTypeJson && isJsonArray(result)) {
                                        List<Row> rows = RedisResultArrayWrapper.createRowDataForHash(
                                                queryParameter,
                                                result,
                                                redisValueDataStructure,
                                                dataTypes);
                                        resultFuture.complete(rows.stream()
                                                .map(r -> mergeRow(input, r))
                                                .collect(Collectors.toList()));
                                        return;
                                    }
                                    Row row = RedisResultWrapper.createRowDataForHash(
                                            queryParameter,
                                            result,
                                            redisValueDataStructure,
                                            dataTypes);
                                    resultFuture.complete(Collections.singleton(mergeRow(input, row)));
                                });
                return;
            case ZSCORE: {
                this.redisCommandsContainer
                        .zscore(queryParameter[0], queryParameter[1])
                        .thenAccept(
                                result -> {
                                    Row row =
                                            RedisResultWrapper.createRowDataForSortedSet(
                                                    queryParameter, result, redisValueDataStructure);
                                    resultFuture.complete(Collections.singleton(mergeRow(input, row)));
                                    if (cache != null && result != null) {
                                        String keyForZSore =
                                                new StringBuilder(queryParameter[0])
                                                        .append(CACHE_SEPERATOR)
                                                        .append(queryParameter[1])
                                                        .toString();
                                        cache.put(keyForZSore, row);
                                    }
                                });
                break;
            }
            default:
        }
    }

    private String[] calcParamByCommand(Row row) {
        List<String> params = new ArrayList<>();
        String keyField = this.readableConfig.get(RedisOptions.CUSTOM_KEY_NAME);
        String realKeyField = replaceByTag(row, keyField);

        params.add(realKeyField);

        if (redisCommand.getJoinCommand() == RedisJoinCommand.HGET) {
            String fieldField = this.readableConfig.get(RedisOptions.CUSTOM_FIELD_NAME);
            String realValueField = replaceByTag(row, fieldField);
            params.add(realValueField);
        } else if (redisCommand.getJoinCommand() == RedisJoinCommand.ZSCORE) {
            String valueField = this.readableConfig.get(RedisOptions.CUSTOM_VALUE_NAME);
            String realValueField = replaceByTag(row, valueField);
            params.add(realValueField);
        }
        return params.toArray(new String[0]);
    }

    private Row mergeRow(Row left, Row right) {
        if (left == null || right == null) {
            return left;
        }
        Set<String> rightFieldNames = right.getFieldNames(true);
        if (CollectionUtils.isEmpty(rightFieldNames)) {
            return left;
        }
        Set<String> leftFieldNames = left.getFieldNames(true);
        if (CollectionUtils.isEmpty(leftFieldNames)) {
            return left;
        }

        Row outRow = Row.withNames();
        for (String fieldName : leftFieldNames) {
            outRow.setField(fieldName, left.getField(fieldName));
        }
        for (String fieldName : rightFieldNames) {
            String overwrite = valueOverwriteMap.get(fieldName);
            if (overwrite != null
                    && !Objects.equals(overwrite.toLowerCase(), TRUE)
                    && leftFieldNames.contains(fieldName)) {
                continue;
            }
            outRow.setField(fieldName, right.getField(fieldName));
        }
        return outRow;
    }

    private Row expandDefaultRow(Row input) {
        Row outRow = Row.withNames();
        outRow.setField(VALUE, null);
        if (redisValueDataStructure != RedisValueDataStructure.row) {
            outRow.setField(KEY, null);

            if (redisCommand.name().contains("HGET")) {
                outRow.setField(FIELD, null);
            } else if (redisCommand == ZSCORE || redisCommand == ZRANGEWITHSCORES) {
                outRow.setField(SCORE, null);
            }
        }

        if (dataTypes != null && !dataTypes.isEmpty()) {
            List<String> valueFieldNames = new ArrayList<>(dataTypes.keySet());
            for (String fieldName : valueFieldNames) {
                outRow.setField(fieldName, null);
            }
        }
        return mergeRow(input, outRow);
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);

        Preconditions.checkArgument(
                redisCommand.getJoinCommand() != RedisJoinCommand.NONE,
                String.format("the command %s do not support join.", redisCommand.name()));

        // permit hget all to cache without cache limit
        /*if (redisCommand.getJoinCommand() == RedisJoinCommand.HGETALL) {
            Preconditions.checkArgument(
                    cacheMaxSize != -1 && cacheTtl != -1,
                    "cache must be opened by cacheMaxSize and cacheTtl when you want to load all elements to cache.");
        }*/

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
}
