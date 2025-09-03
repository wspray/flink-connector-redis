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
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.connectors.redis.config.FlinkClusterConfig;
import org.apache.flink.streaming.connectors.redis.config.FlinkConfigBase;
import org.apache.flink.streaming.connectors.redis.config.FlinkSingleConfig;
import org.apache.flink.streaming.connectors.redis.stream.RedisSourceFunction;
import org.apache.flink.streaming.connectors.redis.stream.RedisSourceFunctionBuilder;
import org.apache.flink.streaming.connectors.redis.table.base.TestRedisConfigBase;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jeff.zou on 2021/2/26.
 */
public class DataSourceBuilderTest /*extends TestRedisConfigBase*/ {

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
//                .setNodesInfo("127.0.0.1:6379")
                .build();

//        FlinkConfigBase conf = new FlinkSentinelConfig.Builder()
//                .setSentinelsInfo("")
//                .setSentinelsPassword("")
//                .setMasterName("")
//                .setDatabase("")
//                .build();

        Map<String, TypeInformation> map = new HashMap<>();
        map.put("gender", Types.INT);
        map.put("name", Types.STRING);
        map.put("hobbies", Types.STRING);

        RedisSourceFunctionBuilder<Row> builder = RedisSourceFunctionBuilder.builder()
                .setFlinkConfigBase(conf)
//                        .setKey("BASIC_CRYPTION*")   // cluster
//                        .setQueryGet()
//                        .setKey("user:1")            // single
//                        .setQueryHGet("name")
//                        .setKey("user:*")
//                        .setQueryHGetAll()
//                        .setKey("mylist*")
//                        .setQueryRange()
//                        .setKey("myset*")
//                        .setQuerySMembers()
//                        .setKey("myzset*")
//                        .setQueryZRangeWithScores()
//                        .setKey("myzset")
//                        .setValueOnly()
//                        .setQueryZScore("item1")
                .setKey("test:hash*")
//                .setResolvedSchema(map)
//                .setValueOnly(true)
                .setQueryHGetAll()
                .checkAndInferType();
        RowTypeInfo rowTypeInfo = builder.getRowTypeInfo();
        RedisSourceFunction<Row> sourceFunction = builder.build();


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Row> source = env.addSource(sourceFunction, TypeInformation.of(Row.class));
        source.addSink(new PrintSinkFunction<>("@@@", false));
        env.execute("RedisSinkTest");

    }
}
