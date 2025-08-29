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

package org.apache.flink.streaming.connectors.redis.datastream;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.connectors.redis.config.FlinkClusterConfig;
import org.apache.flink.streaming.connectors.redis.config.FlinkConfigBase;
import org.apache.flink.streaming.connectors.redis.config.RedisJoinConfig;
import org.apache.flink.streaming.connectors.redis.stream.RedisLookupFunction;
import org.apache.flink.streaming.connectors.redis.stream.RedisLookupFunctionBuilder;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo.INT_ARRAY_TYPE_INFO;
import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.REDIS_CLUSTER;
import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.REDIS_MODE;

/**
 * Created by jeff.zou on 2021/2/26.
 */
public class DataLookUpBuilderTest /*extends TestRedisConfigBase */ {

    @Test
    public void testDateStreamInsert() throws Exception {
//        FlinkConfigBase conf =
//                new FlinkSingleConfig.Builder()
//                        .setHost(REDIS_HOST)
//                        .setPort(REDIS_PORT)
//                        .setPassword(REDIS_PASSWORD)
//                        .build();

        FlinkConfigBase conf = new FlinkClusterConfig.Builder()
                .setNodesInfo("10.130.18.76:6381,10.130.18.76:6379,10.130.18.76:6383")
                .build();

//        FlinkConfigBase conf = new FlinkSentinelConfig.Builder()
//                .setSentinelsInfo("")
//                .setSentinelsPassword("")
//                .setMasterName("")
//                .setDatabase("")
//                .build();

//        System.out.println(singleRedisCommands.get("test*"));
        Configuration configuration = new Configuration();
//        configuration.setString(REDIS_MODE, REDIS_SINGLE);
//        configuration.setString(REDIS_MODE, REDIS_SENTINEL);
        configuration.setString(REDIS_MODE, REDIS_CLUSTER);

//        configuration.set(VALUE_DATA_STRUCTURE, RedisValueDataStructure.row1);


//        configuration.set(RedisOptions.CUSTOM_KEY_NAME, "student:{id}");
//        RowRedisQueryMapper redisMapper = new RowRedisQueryMapper(RedisCommand.GET);

//        configuration.set(RedisOptions.CUSTOM_KEY_NAME, "student:{id}");
//        RowRedisQueryMapper redisMapper = new RowRedisQueryMapper(RedisCommand.MGET);

//        configuration.setString(SCAN_KEY, "user:1");
//        configuration.setString(SCAN_ADDITION_KEY, "name");
//        RowRedisQueryMapper redisMapper = new RowRedisQueryMapper(RedisCommand.HGET);

//        configuration.setString(SCAN_KEY, "user:*");
//        RowRedisQueryMapper redisMapper = new RowRedisQueryMapper(RedisCommand.HGETALL);

        LinkedHashMap<String, TypeInformation> map = new LinkedHashMap<>();
        map.put("name", Types.STRING);
        map.put("gender", Types.INT);
        map.put("hobbies", Types.STRING);
        map.put("eyes", DOUBLE_ARRAY_TYPE_INFO);
        RedisJoinConfig.Builder joinConfigBuilder = new RedisJoinConfig.Builder();
//        joinConfigBuilder
//                .setCacheTTL(10)
//                .setCacheMaxSize(500)
//                .setLoadAll(true);
        RedisJoinConfig joinConfig = joinConfigBuilder.build();
//        RedisLookupFunction lookupFunction =
//                new RedisLookupFunction(redisMapper, configuration, conf, map, joinConfig);

        RedisLookupFunctionBuilder<Row> builder = RedisLookupFunctionBuilder.builder()
                .setFlinkConfigBase(conf)
                .setResolvedSchema(map)
//                .setMaxRetries(1)
                .setRedisJoinConfig(joinConfig)
                .setKeyName("student:/{id}")
                .setQueryGet();


//        configuration.set(RedisOptions.CUSTOM_FIELD_NAME, "subject");
//        configuration.set(RedisOptions.CUSTOM_VALUE_NAME, "scope");

        Row row1 = Row.withNames();
        row1.setField("id", "1");
        row1.setField("subject", "math");
        row1.setField("scores", new Integer[]{1, 2, 3});
        Row row2 = Row.withNames();
        row2.setField("id", "2");
        row2.setField("subject", "english");
        Row row3 = Row.withNames();
        row3.setField("id", "1");
        row3.setField("subject", "science");

        TypeInformation<Row> rowTypeInformation = Types.ROW_NAMED(
                new String[]{"id", "subject", "scores"},
                Types.STRING, Types.STRING, INT_ARRAY_TYPE_INFO);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Row> inputStream = env.fromData(rowTypeInformation, row1, row2, row3);

        RowTypeInfo inputRowTypeInfo = (RowTypeInfo) inputStream.getType();
        builder.checkAndInferType(inputRowTypeInfo);
        RowTypeInfo typeInfo = builder.getRowTypeInfo();

        RedisLookupFunction redisLookupFunction = builder.build();

        DataStream<Row> orderedResult = AsyncDataStream
                //保证顺序：异步返回的结果保证顺序，超时时间1秒，最大容量2，超出容量触发反压
                .orderedWait(inputStream,
                        redisLookupFunction,
                        -1, TimeUnit.MILLISECONDS)
                .setParallelism(1);

        orderedResult.addSink(new PrintSinkFunction<>("@@@", false));
        env.execute("RedisSinkTest");

    }
}
