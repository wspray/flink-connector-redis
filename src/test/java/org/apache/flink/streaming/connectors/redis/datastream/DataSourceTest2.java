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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.connectors.redis.command.RedisCommand;
import org.apache.flink.streaming.connectors.redis.config.FlinkClusterConfig;
import org.apache.flink.streaming.connectors.redis.config.FlinkConfigBase;
import org.apache.flink.streaming.connectors.redis.config.FlinkSentinelConfig;
import org.apache.flink.streaming.connectors.redis.config.RedisValueDataStructure;
import org.apache.flink.streaming.connectors.redis.mapper.RowRedisQueryMapper;
import org.apache.flink.streaming.connectors.redis.stream.RedisSourceFunction;
import org.apache.flink.streaming.connectors.redis.table.base.TestRedisConfigBase;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;

import static org.apache.flink.streaming.connectors.redis.config.RedisOptions.SCAN_KEY;
import static org.apache.flink.streaming.connectors.redis.config.RedisOptions.VALUE_DATA_STRUCTURE;
import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.REDIS_CLUSTER;
import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.REDIS_MODE;
import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.REDIS_SENTINEL;

/**
 * Created by jeff.zou on 2021/2/26.
 */
public class DataSourceTest2 extends TestRedisConfigBase {

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

        configuration.set(VALUE_DATA_STRUCTURE, RedisValueDataStructure.row);

//        configuration.setString(REDIS_COMMAND, RedisCommand.HGET.name());

//        configuration.setString(SCAN_KEY, "test*");
//        RowRedisQueryMapper redisMapper = new RowRedisQueryMapper(RedisCommand.MGET);
        configuration.setString(SCAN_KEY, "BASIC_CRYPTION*");
        RowRedisQueryMapper redisMapper = new RowRedisQueryMapper(RedisCommand.MGET);

//        configuration.setString(SCAN_KEY, "student:*");
//        RowRedisQueryMapper redisMapper = new RowRedisQueryMapper(RedisCommand.MGET);

//        configuration.setString(SCAN_KEY, "user:1");
//        configuration.setString(SCAN_ADDITION_KEY, "name");
//        RowRedisQueryMapper redisMapper = new RowRedisQueryMapper(RedisCommand.HGET);

//        configuration.setString(SCAN_KEY, "user:*");
//        RowRedisQueryMapper redisMapper = new RowRedisQueryMapper(RedisCommand.HGETALL);

//        configuration.setString(SCAN_KEY, "mylist*");
//        RowRedisQueryMapper redisMapper = new RowRedisQueryMapper(RedisCommand.LRANGE);

//        configuration.setString(SCAN_KEY, "myset*");
//        RowRedisQueryMapper redisMapper = new RowRedisQueryMapper(RedisCommand.SMEMBERS);

//        configuration.setString(SCAN_KEY, "myzset*");
//        RowRedisQueryMapper redisMapper = new RowRedisQueryMapper(RedisCommand.ZRANGEWITHSCORES);

        LinkedHashMap<String, TypeInformation> map = new LinkedHashMap<>();
        map.put("name", Types.STRING);
        map.put("gender", Types.INT);
        map.put("hobbies", Types.STRING);

        RedisSourceFunction<Row> sourceFunction =
                new RedisSourceFunction<>(redisMapper, configuration, conf, map);


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Row> source = env.addSource(sourceFunction, TypeInformation.of(Row.class));
        source.addSink(new PrintSinkFunction<>("@@@", false));
        env.execute("RedisSinkTest");

    }
}
