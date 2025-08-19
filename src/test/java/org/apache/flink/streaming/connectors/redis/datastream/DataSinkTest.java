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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.command.RedisCommand;
import org.apache.flink.streaming.connectors.redis.config.FlinkConfigBase;
import org.apache.flink.streaming.connectors.redis.config.FlinkSingleConfig;
import org.apache.flink.streaming.connectors.redis.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.config.RedisValueDataStructure;
import org.apache.flink.streaming.connectors.redis.mapper.RedisSinkRowMapper;
import org.apache.flink.streaming.connectors.redis.mapper.RowRedisSinkRowMapper;
import org.apache.flink.streaming.connectors.redis.stream.RedisSinkFunction;
import org.apache.flink.streaming.connectors.redis.table.base.TestRedisConfigBase;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.Preconditions;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.streaming.connectors.redis.config.RedisOptions.TTL;
import static org.apache.flink.streaming.connectors.redis.config.RedisValidator.*;

/**
 * Created by jeff.zou on 2021/2/26.
 */
public class DataSinkTest extends TestRedisConfigBase {

    @Test
    public void testDateStreamInsert() throws Exception {

//        System.out.println(singleRedisCommands.hget("tom", "math"));
//        singleRedisCommands.del("tom");
        Configuration configuration = new Configuration();
        configuration.setString(REDIS_MODE, REDIS_SINGLE);
//        configuration.setString(REDIS_COMMAND, RedisCommand.HSET.name());

//        configuration.set(RedisOptions.ROW_BY_NAMES, false);
        configuration.set(RedisOptions.SET_IF_ABSENT, false);// SET、HST、HMSET 有效
//        configuration.setInteger(TTL, 10);
//        configuration.set(RedisOptions.TTL_KEY_NOT_ABSENT, true); // 设置了TTL
//        configuration.set(RedisOptions.EXPIRE_ON_TIME, "12:12:01"); // 未设置TTL 10:00 12:12:01



//        configuration.set(RedisOptions.VALUE_DATA_STRUCTURE, RedisValueDataStructure.row);


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Row row = Row.withNames();
        row.setField("name", "tom");
        row.setField("subject", "math");
        row.setField("scope", "1312");

        configuration.set(RedisOptions.CUSTOM_KEY_NAME, "name");
        configuration.set(RedisOptions.CUSTOM_FIELD_NAME, "subject");
        configuration.set(RedisOptions.CUSTOM_VALUE_NAME, "scope");
//        configuration.set(RedisOptions.CUSTOM_SCORE_NAME, "score");

        RedisSinkRowMapper redisMapper = new RowRedisSinkRowMapper(RedisCommand.HSET, configuration);

        List<TypeInformation> columnDataTypes =
                Arrays.asList(Types.STRING, Types.STRING, Types.STRING);
        TypeInformation<Row> rowTypeInfo = Types.ROW_NAMED(
                new String[]{"name", "subject", "scope"},
                Types.STRING, Types.STRING, Types.STRING
        );

        DataStream<Row> dataStream = env.fromData(
                Arrays.asList(row, row),
                rowTypeInfo
        );
        FlinkConfigBase conf =
                new FlinkSingleConfig.Builder()
                        .setHost(REDIS_HOST)
                        .setPort(REDIS_PORT)
                        .setPassword(REDIS_PASSWORD)
                        .build();

        RedisSinkFunction redisSinkFunction =
                new RedisSinkFunction<>(conf, redisMapper, columnDataTypes, configuration);

        dataStream.addSink(redisSinkFunction).setParallelism(1);
        env.execute("RedisSinkTest");

        Object hget = singleRedisCommands.hget("tom", "math");
        System.out.println(hget.toString());
//        Preconditions.condition(hget.equals("222"), "");
    }
}
