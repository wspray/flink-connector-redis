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

package org.apache.flink.streaming.connectors.redis.mapper;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.command.RedisCommand;
import org.apache.flink.streaming.connectors.redis.command.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.stream.converter.RedisRowConverter;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;

import java.time.LocalTime;

/**
 * base row redis mapper implement.
 */
public class RowRedisSinkRowMapper implements RedisSinkRowMapper<Row> {

    private final Integer ttl;

    private LocalTime expireTime;

    private final RedisCommand redisCommand;

    private final Boolean setIfAbsent;

    private final Boolean ttlKeyNotAbsent;

    private final Boolean auditLog;

    public RowRedisSinkRowMapper(RedisCommand redisCommand, ReadableConfig config) {
        this.redisCommand = redisCommand;
        this.ttl = config.get(RedisOptions.TTL);
        this.setIfAbsent = config.get(RedisOptions.SET_IF_ABSENT);
        this.ttlKeyNotAbsent = config.get(RedisOptions.TTL_KEY_NOT_ABSENT);
        this.auditLog = config.get(RedisOptions.AUDIT_LOG);
        String expireOnTime = config.get(RedisOptions.EXPIRE_ON_TIME);
        if (!StringUtils.isNullOrWhitespaceOnly(expireOnTime)) {
            this.expireTime = LocalTime.parse(expireOnTime);
        }
    }

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(
                redisCommand, ttl, expireTime, setIfAbsent, ttlKeyNotAbsent, auditLog);
    }

    @Override
    public String getKeyFromData(Row rowData, TypeInformation logicalType, Integer keyIndex) {
        return RedisRowConverter.rowDataToString(logicalType, rowData, keyIndex);
    }

    @Override
    public String getValueFromData(Row rowData, TypeInformation logicalType, Integer valueIndex) {
        return RedisRowConverter.rowDataToString(logicalType, rowData, valueIndex);
    }

    @Override
    public String getFieldFromData(Row rowData, TypeInformation logicalType, Integer fieldIndex) {
        return RedisRowConverter.rowDataToString(logicalType, rowData, fieldIndex);
    }

    public RedisCommand getRedisCommand() {
        return redisCommand;
    }

    @Override
    public boolean equals(Object obj) {
        RedisCommand redisCommand = ((RowRedisSinkRowMapper) obj).redisCommand;
        return this.redisCommand == redisCommand;
    }
}
