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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.config.FlinkClusterConfig;
import org.apache.flink.streaming.connectors.redis.config.FlinkConfigBase;
import org.apache.flink.streaming.connectors.redis.stream.RedisSinkFunction;
import org.apache.flink.streaming.connectors.redis.stream.RedisSinkFunctionBuilder;
import org.apache.flink.streaming.connectors.redis.table.base.TestRedisConfigBase;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

/**
 * Created by jeff.zou on 2021/2/26.
 */
public class DataSinkBuilderTest extends TestRedisConfigBase {

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

        // hset example
        Row row = Row.withNames();
        row.setField("name", "tom");
        row.setField("subject", "math");
        row.setField("scope", null);

        TypeInformation<Row> rowTypeInfo = Types.ROW_NAMED(
                new String[]{"name", "subject", "scope"},
                Types.STRING, Types.STRING, Types.STRING
        );

        RedisSinkFunction<Row> redisSinkFunction = RedisSinkFunctionBuilder.builder()
                .setFlinkConfigBase(conf)
                .setKeyName("name")
//                .setTTL(60)
//                .setTTLKeyNotAbsent(true)
//                .setExpireOnTime("14:22")
                .setAllRowOutPut(true)
                .setSinkHSet("subject", "scope")
                .build();

        // set example
//        Row row = Row.withNames();
//        row.setField("key", "100");
//        row.setField("value", "math");
//        RedisSinkFunction<Row> redisSinkFunction = RedisSinkFunctionBuilder.builder()
//                .setFlinkConfigBase(conf)
//                .setKeyName("test:101")
////                .setIfAbsent(true)
////                .setTTL(60)
////                .setTTLKeyNotAbsent(true)
////                .setExpireOnTime("14:22")
////                .setAllRowOutPut()
//                .setSinkSet(null)
//                .build();
//        TypeInformation<Row> rowTypeInfo = Types.ROW_NAMED(
//                new String[]{"key", "value"},
//                Types.STRING, Types.STRING
//        );

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Row> dataStream = env.fromData(
                Arrays.asList(row, row),
                rowTypeInfo
        );

        dataStream.addSink(redisSinkFunction).setParallelism(1);
        env.execute("RedisSinkTest");

    }
}
