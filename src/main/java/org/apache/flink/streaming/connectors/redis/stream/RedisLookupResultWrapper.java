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

import io.lettuce.core.ScoredValue;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.redis.config.RedisValueDataStructure;
import org.apache.flink.streaming.connectors.redis.stream.converter.RedisRowConverter;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.apache.flink.streaming.connectors.redis.config.RedisOptions.FIELD;
import static org.apache.flink.streaming.connectors.redis.config.RedisOptions.KEY;
import static org.apache.flink.streaming.connectors.redis.config.RedisOptions.SCORE;
import static org.apache.flink.streaming.connectors.redis.config.RedisOptions.VALUE;

public class RedisLookupResultWrapper {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * create row data for string.
     *
     * @param keys
     * @param value
     */
    public static Row createRowDataForString(
            Object[] keys,
            String value,
            RedisValueDataStructure redisValueDataStructure,
            Map<String, TypeInformation> dataTypes) {
        if (redisValueDataStructure == RedisValueDataStructure.column) {
            Row row = createRowDataForRow(value, dataTypes);
            row.setField(
                    KEY,
                    RedisRowConverter.dataTypeFromStringWithException(
                            Types.STRING, String.valueOf(keys[0])));
            return row;
        }

        return createRowDataForRow(value, dataTypes);
    }

    public static Row createRowDataForString(
            String key,
            String value,
            RedisValueDataStructure redisValueDataStructure,
            Map<String, TypeInformation> dataTypes) {
        if (redisValueDataStructure == RedisValueDataStructure.column) {
            Row row = createRowDataForRow(value, dataTypes);
            row.setField(
                    KEY,
                    RedisRowConverter.dataTypeFromStringWithException(
                            Types.STRING, String.valueOf(key)));
            return row;
        }

        return createRowDataForRow(value, dataTypes);
    }

    /**
     * create row data for whole row.
     *
     * @param value
     * @return
     */
    public static Row createRowDataForRow(String value,
                                          Map<String, TypeInformation> dataTypes) {
        Row row = Row.withNames();
        row.setField(VALUE, value);
        if (dataTypes == null || dataTypes.isEmpty()) {
            return row;
        }

        Map<String, Object> jsonMap = deserializeString(value);
        boolean successDeserialize = jsonMap != null && !jsonMap.isEmpty();
        if (successDeserialize) {
            row.setField(VALUE, value); // unify typeInfo
        }

        dataTypes.forEach((key, type) -> {
            Object result = successDeserialize ? jsonMap.get(key) : null;
            row.setField(
                    key,
                    result == null ?
                            null : RedisRowConverter.dataTypeFromStringWithException(
                            type, String.valueOf(result)));
        });
        return row;
    }

    /**
     * create row data for hash.
     *
     * @param keys
     * @param value
     */
    public static Row createRowDataForHash(
            Object[] keys,
            String value,
            RedisValueDataStructure redisValueDataStructure,
            Map<String, TypeInformation> dataTypes) {
        if (redisValueDataStructure == RedisValueDataStructure.column) {
            Row row = createRowDataForRow(value, dataTypes);
            row.setField(
                    KEY,
                    RedisRowConverter.dataTypeFromStringWithException(
                            Types.STRING, String.valueOf(keys[0])));
            row.setField(
                    FIELD,
                    RedisRowConverter.dataTypeFromStringWithException(
                            Types.STRING, String.valueOf(keys[1])));
            return row;
        }

        return createRowDataForRow(value, dataTypes);
    }

    public static List<Row> createRowDataForHashAll(
            Object[] keys,
            Map<String, String> map,
            RedisValueDataStructure redisValueDataStructure,
            Map<String, TypeInformation> dataTypes) {
        List<Row> list = new ArrayList<>();

        if (redisValueDataStructure == RedisValueDataStructure.column) {
            map.forEach((field, value) -> {
                Row row = createRowDataForRow(value, dataTypes);
                row.setField(
                        KEY,
                        RedisRowConverter.dataTypeFromStringWithException(
                                Types.STRING, String.valueOf(keys[0])));
                row.setField(
                        FIELD,
                        RedisRowConverter.dataTypeFromStringWithException(
                                Types.STRING, field));
                list.add(row);
            });
            return list;
        }

        Collection<String> values = map.values();
        for (String value : values) {
            Row row = createRowDataForRow(value, dataTypes);
            list.add(row);
        }
        return list;
    }

    public static Row createRowDataForSortedSet(
            Object[] keys,
            Double value,
            RedisValueDataStructure redisValueDataStructure) {
        Row row = Row.withNames();
        if (redisValueDataStructure == RedisValueDataStructure.column) {
            row.setField(
                    KEY,
                    RedisRowConverter.dataTypeFromStringWithException(
                            Types.STRING, String.valueOf(keys[0])));
            row.setField(
                    VALUE,
                    RedisRowConverter.dataTypeFromStringWithException(
                            Types.STRING, String.valueOf(keys[1])));

            row.setField(SCORE, value);
        } else {
            row.setField(VALUE, value);
        }
        return row;
    }

    public static Row createRowDataForSortedSet(
            Object[] keys,
            ScoredValue<String> scoredValue,
            RedisValueDataStructure redisValueDataStructure,
            Map<String, TypeInformation> dataTypes) {
        if (redisValueDataStructure == RedisValueDataStructure.column) {
            Row row = createRowDataForRow(scoredValue.getValue(), dataTypes);
            row.setField(
                    KEY,
                    RedisRowConverter.dataTypeFromStringWithException(
                            Types.STRING, String.valueOf(keys[0])));
            row.setField(
                    SCORE,
                    RedisRowConverter.dataTypeFromStringWithException(
                            Types.DOUBLE, String.valueOf(scoredValue.getScore())));
            return row;
        }

        return createRowDataForRow(scoredValue.getValue(), dataTypes);
    }

    public static String serializeObject(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (Exception e) {
            throw new RuntimeException("serialize object exception:" + e.getMessage());
        }
    }

    public static Map<String, Object> deserializeString(String str) {
        try {
            return objectMapper.readValue(str, Map.class);
        } catch (Exception e) {
            throw new RuntimeException("deserialize string exception:" + e.getMessage());
        }
    }
}
