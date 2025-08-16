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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.redis.command.RedisCommand;
import org.apache.flink.streaming.connectors.redis.command.RedisCommandBaseDescription;
import org.apache.flink.streaming.connectors.redis.command.RedisSelectCommand;
import org.apache.flink.streaming.connectors.redis.config.FlinkConfigBase;
import org.apache.flink.streaming.connectors.redis.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.config.RedisValueDataStructure;
import org.apache.flink.streaming.connectors.redis.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.container.RedisCommandsContainerBuilder;
import org.apache.flink.streaming.connectors.redis.mapper.RedisMapper;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RedisSourceFunction<T> extends RichSourceFunction<T> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisSourceFunction.class);

    ReadableConfig readableConfig;

    private FlinkConfigBase flinkConfigBase;
    private transient RedisCommandsContainer redisCommandsContainer;

    private final int maxRetryTimes;

    private RedisCommand redisCommand;

    private final RedisValueDataStructure redisValueDataStructure;

    private final List<TypeInformation> dataTypes;

    private String[] queryParameter;

    public RedisSourceFunction(
            RedisMapper redisMapper,
            ReadableConfig readableConfig,
            FlinkConfigBase flinkConfigBase,
            List<TypeInformation> dataTypes) {
        this.readableConfig = readableConfig;
        this.flinkConfigBase = flinkConfigBase;
        this.maxRetryTimes = readableConfig.get(RedisOptions.MAX_RETRIES);
        this.redisValueDataStructure = readableConfig.get(RedisOptions.VALUE_DATA_STRUCTURE);

        RedisCommandBaseDescription redisCommandDescription = redisMapper.getCommandDescription();
        Preconditions.checkNotNull(
                redisCommandDescription, "Redis Mapper data type description can not be null");
        this.redisCommand = redisCommandDescription.getRedisCommand();
        this.dataTypes = dataTypes;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        validator();
        this.queryParameter = new String[2];
        this.queryParameter[0] = this.readableConfig.get(RedisOptions.SCAN_KEY);

        if (redisCommand.getSelectCommand() == RedisSelectCommand.HGET) {
            this.queryParameter[1] = this.readableConfig.get(RedisOptions.SCAN_ADDITION_KEY);
        } else if (redisCommand.getSelectCommand() == RedisSelectCommand.ZSCORE) {
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
        super.open(parameters);
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        // It will try many times which less than {@code maxRetryTimes} until execute success.
        for (int i = 0; i <= maxRetryTimes; i++) {
            try {
                query(ctx);
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

    private void query(SourceContext ctx) throws Exception {
        switch (redisCommand.getSelectCommand()) {
            case GET: {
                String result = this.redisCommandsContainer.get(queryParameter[0]).get();
                Row rowData =
                        RedisResultWrapper.createRowDataForString(
                                queryParameter, result, redisValueDataStructure, dataTypes);
                ctx.collect(rowData);
                break;
            }
            case HGET: {
                String result =
                        this.redisCommandsContainer
                                .hget(queryParameter[0], queryParameter[1])
                                .get();
                Row rowData =
                        RedisResultWrapper.createRowDataForHash(
                                queryParameter, result, redisValueDataStructure, dataTypes);
                ctx.collect(rowData);
                break;
            }
            case ZSCORE: {
                Double result =
                        this.redisCommandsContainer
                                .zscore(queryParameter[0], queryParameter[1])
                                .get();
                Row rowData =
                        RedisResultWrapper.createRowDataForSortedSet(
                                queryParameter, result, dataTypes);
                ctx.collect(rowData);
                break;
            }
            case LRANGE: {
                List list =
                        this.redisCommandsContainer
                                .lRange(
                                        queryParameter[0],
                                        this.readableConfig.get(RedisOptions.SCAN_RANGE_START),
                                        this.readableConfig.get(RedisOptions.SCAN_RANGE_STOP))
                                .get();
                list.forEach(
                        result -> {
                            Row rowData =
                                    RedisResultWrapper.createRowDataForString(
                                            queryParameter,
                                            String.valueOf(result),
                                            redisValueDataStructure,
                                            dataTypes);
                            ctx.collect(rowData);
                        });

                break;
            }
            case SRANDMEMBER: {
                List list =
                        this.redisCommandsContainer
                                .srandmember(
                                        String.valueOf(queryParameter[0]),
                                        readableConfig.get(RedisOptions.SCAN_COUNT))
                                .get();

                list.forEach(
                        result -> {
                            Row rowData =
                                    RedisResultWrapper.createRowDataForString(
                                            queryParameter,
                                            String.valueOf(result),
                                            redisValueDataStructure,
                                            dataTypes);
                            ctx.collect(rowData);
                        });
                break;
            }
            case SUBSCRIBE: {
            }
            default:
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (redisCommandsContainer != null) {
            redisCommandsContainer.close();
        }
    }

    @Override
    public void cancel() {
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
            Preconditions.checkArgument(
                    dataTypes.get(1).equals(BasicTypeInfo.DOUBLE_TYPE_INFO),
                    "the second column's type of source table must be double. the type of score is double when the data structure in redis is SortedSet.");
        } else if (redisCommand.getSelectCommand() == RedisSelectCommand.LRANGE) {
            Preconditions.checkNotNull(
                    this.readableConfig.get(RedisOptions.SCAN_RANGE_START),
                    "the %s must not be null when query list",
                    RedisOptions.SCAN_RANGE_START.key());

            Preconditions.checkNotNull(
                    this.readableConfig.get(RedisOptions.SCAN_RANGE_STOP),
                    "the %s must not be null when query list",
                    RedisOptions.SCAN_RANGE_STOP.key());
        } else if (redisCommand.getSelectCommand() == RedisSelectCommand.SRANDMEMBER) {
            Preconditions.checkNotNull(
                    this.readableConfig.get(RedisOptions.SCAN_COUNT),
                    "the %s must not be null when query set",
                    RedisOptions.SCAN_COUNT.key());
        }
    }
}
