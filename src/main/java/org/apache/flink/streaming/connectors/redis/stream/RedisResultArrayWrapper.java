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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.redis.config.RedisValueDataStructure;
import org.apache.flink.streaming.connectors.redis.stream.converter.RedisRowConverter;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.streaming.connectors.redis.config.RedisOptions.FIELD;
import static org.apache.flink.streaming.connectors.redis.config.RedisOptions.KEY;
import static org.apache.flink.streaming.connectors.redis.config.RedisOptions.SCORE;
import static org.apache.flink.streaming.connectors.redis.config.RedisOptions.VALUE;

public class RedisResultArrayWrapper {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * create row data for string.
     *
     * @param keys
     * @param value
     */
    public static List<Row> createRowDataForString(
            Object[] keys,
            String value,
            RedisValueDataStructure redisValueDataStructure,
            Map<String, TypeInformation> dataTypes) {
        if (redisValueDataStructure == RedisValueDataStructure.column) {
            List<Row> rows = createRowDataForRow(value, dataTypes);
            for (Row row : rows) {
                row.setField(
                        KEY,
                        RedisRowConverter.dataTypeFromString(
                                Types.STRING, String.valueOf(keys[0])));
            }
            return rows;
        }

        return createRowDataForRow(value, dataTypes);
    }

    public static List<Row> createRowDataForString(
            String key,
            String value,
            RedisValueDataStructure redisValueDataStructure,
            Map<String, TypeInformation> dataTypes) {
        if (redisValueDataStructure == RedisValueDataStructure.column) {
            List<Row> rows = createRowDataForRow(value, dataTypes);
            for (Row row : rows) {
                row.setField(
                        KEY,
                        RedisRowConverter.dataTypeFromString(
                                Types.STRING, String.valueOf(key)));
            }
            return rows;
        }

        return createRowDataForRow(value, dataTypes);
    }

    /**
     * create row data for whole row.
     *
     * @param value
     * @return
     */
    public static List<Row> createRowDataForRow(String value,
                                                Map<String, TypeInformation> dataTypes) {
        Row row = Row.withNames();
        row.setField(VALUE, value);
        if (dataTypes == null || dataTypes.isEmpty()) {
            return Collections.singletonList(row);
        }

        List<Row> rows = new ArrayList<>();
        List<Map<String, Object>> jsonMapList = deserializeString(value);
        for (Map<String, Object> jsonMap : jsonMapList) {
            String jsonValue = toJson(jsonMap);
            rows.add(RedisResultWrapper.createRowDataForRow(jsonValue, dataTypes));
        }
        return rows;
    }

    /**
     * create row data for hash.
     *
     * @param keys
     * @param value
     */
    public static List<Row> createRowDataForHash(
            Object[] keys,
            String value,
            RedisValueDataStructure redisValueDataStructure,
            Map<String, TypeInformation> dataTypes) {
        if (redisValueDataStructure == RedisValueDataStructure.column) {
            List<Row> rows = createRowDataForRow(value, dataTypes);
            for (Row row : rows) {
                row.setField(
                        KEY,
                        RedisRowConverter.dataTypeFromString(
                                Types.STRING, String.valueOf(keys[0])));
                row.setField(
                        FIELD,
                        RedisRowConverter.dataTypeFromString(
                                Types.STRING, String.valueOf(keys[1])));
            }
            return rows;
        }

        return createRowDataForRow(value, dataTypes);
    }

    public static List<Row> createRowDataForHashAll(
            Object[] keys,
            boolean valueTypeJson,
            Map<String, String> map,
            RedisValueDataStructure redisValueDataStructure,
            Map<String, TypeInformation> dataTypes) {
        List<Row> list = new ArrayList<>();

        if (redisValueDataStructure == RedisValueDataStructure.column) {
            map.forEach((field, value) -> {
                if (valueTypeJson && isJsonArray(value)) {
                    List<Row> rows = createRowDataForRow(value, dataTypes);
                    for (Row row : rows) {
                        row.setField(
                                KEY,
                                RedisRowConverter.dataTypeFromString(
                                        Types.STRING, String.valueOf(keys[0])));
                        row.setField(
                                FIELD,
                                RedisRowConverter.dataTypeFromString(
                                        Types.STRING, field));
                    }
                    list.addAll(rows);
                } else {
                    Row row = RedisResultWrapper.createRowDataForRow(value, dataTypes);
                    row.setField(
                            KEY,
                            RedisRowConverter.dataTypeFromString(
                                    Types.STRING, String.valueOf(keys[0])));
                    row.setField(
                            FIELD,
                            RedisRowConverter.dataTypeFromString(
                                    Types.STRING, field));
                    list.add(row);
                }
            });
            return list;
        }

        Collection<String> values = map.values();
        for (String value : values) {
            List<Row> rows = createRowDataForRow(value, dataTypes);
            list.addAll(rows);
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
                    RedisRowConverter.dataTypeFromString(
                            Types.STRING, String.valueOf(keys[0])));
            row.setField(
                    VALUE,
                    RedisRowConverter.dataTypeFromString(
                            Types.STRING, String.valueOf(keys[1])));

            row.setField(SCORE, value);
        } else {
            row.setField(VALUE, value);
        }
        return row;
    }

    public static List<Row> createRowDataForSortedSet(
            Object[] keys,
            ScoredValue<String> scoredValue,
            RedisValueDataStructure redisValueDataStructure,
            Map<String, TypeInformation> dataTypes) {
        if (redisValueDataStructure == RedisValueDataStructure.column) {
            List<Row> rows = createRowDataForRow(scoredValue.getValue(), dataTypes);
            for (Row row : rows) {
                row.setField(
                        KEY,
                        RedisRowConverter.dataTypeFromString(
                                Types.STRING, String.valueOf(keys[0])));
                row.setField(
                        SCORE,
                        RedisRowConverter.dataTypeFromString(
                                Types.DOUBLE, String.valueOf(scoredValue.getScore())));
            }
            return rows;
        }

        return createRowDataForRow(scoredValue.getValue(), dataTypes);
    }

    public static String serializeObject(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (Exception e) {
            // ignore
        }
        return String.valueOf(object);
    }

    public static List<Map<String, Object>> deserializeString(String str) {
        try {
            return objectMapper.readValue(str, new TypeReference<List<Map<String, Object>>>() {
            });
        } catch (Exception e) {
            // ignore
        }
        return Collections.emptyList();
    }

    public static boolean isJsonArray(String str) {
        if (str == null || str.isEmpty()) return false;
        try {
            JsonNode node = objectMapper.readTree(str);
            return node != null && node.isArray();
        } catch (Exception ignored) {
            return false;
        }
    }

    public static String toJson(Map<String, Object> map) {
        if (map == null) return "{}";
        try {
            return objectMapper.writeValueAsString(map);
        } catch (Exception e) {
            throw new RuntimeException("Map转JSON失败", e);
        }
    }
}
