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

package org.apache.flink.streaming.connectors.redis.stream.converter;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Base64;
import java.util.List;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.lang.Boolean.parseBoolean;

/**
 * redis serialize for stream API.
 */
public class RedisRowConverter {

    private static final Logger LOG = LoggerFactory.getLogger(RedisRowConverter.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static Object dataTypeFromStringWithException(TypeInformation<?> fieldType, String result) {
        try {
            return createDeserializer(fieldType).deserialize(result);
        } catch (Exception e) {
            LOG.error("deserialize result exception:{}", e.getMessage());
            throw new FlinkRuntimeException("deserialize result exception:" + e.getMessage(), e);
        }
    }

    public static Object dataTypeFromString(TypeInformation<?> fieldType, String result) {
        try {
            return createDeserializer(fieldType).deserialize(result);
        } catch (Exception e) {
            LOG.error("deserialize result exception:{}", e.getMessage());
        }
        return null;
    }

    public static String rowDataToString(TypeInformation<?> fieldType, Row row, Integer index) {
        if (row.getField(index) == null) {
            return null;
        }
        return createSerializer(fieldType).serialize(row, index);
    }

    public static RedisDeserializationConverter createDeserializer(TypeInformation<?> fieldType) {
        if (fieldType.equals(Types.BIG_DEC)) {
            return BigDecimal::new;
        } else if (fieldType.equals(Types.FLOAT)) {
            return Float::valueOf;
        } else if (fieldType.equals(Types.DOUBLE)) {
            return Double::valueOf;
        } else if (fieldType.equals(Types.CHAR)) {
            return result -> result.charAt(0);
        } else if (fieldType.equals(Types.STRING)) {
            return result -> result;
        } else if (fieldType.equals(Types.BOOLEAN)) {
            return result -> parseBoolean(result) || "1".equals(result) ? TRUE : FALSE;
        } else if (fieldType.equals(Types.BYTE)) {
            return Byte::valueOf;
        } else if (fieldType.equals(Types.SHORT)) {
            return Short::valueOf;
        } else if (fieldType.equals(Types.INT)) {
            return Integer::valueOf;
        } else if (fieldType.equals(Types.LONG) || fieldType.equals(Types.BIG_INT)) {
            return Long::valueOf;
        } else if (fieldType.equals(Types.SQL_DATE)) {
            return Date::valueOf;
        } else if (fieldType.equals(Types.SQL_TIME)) {
            return Time::valueOf;
        } else if (fieldType.equals(Types.SQL_TIMESTAMP)) {
            return Timestamp::valueOf;
        } else if (fieldType.equals(Types.LOCAL_DATE)) {
            return LocalDate::parse;
        } else if (fieldType.equals(Types.LOCAL_TIME)) {
            return LocalTime::parse;
        } else if (fieldType.equals(Types.LOCAL_DATE_TIME)) {
            return LocalDateTime::parse;
        } else if (fieldType.equals(Types.INSTANT)) {
            return Instant::parse;
        } else if (fieldType.equals(Types.PRIMITIVE_ARRAY(Types.BYTE))) {
            return result -> Base64.getDecoder().decode(result);
        } else if (fieldType.equals(BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO)) {
            return result -> {
                try {
                    return objectMapper.readValue(result, new TypeReference<List<String>>() {
                    }).toArray(new String[0]);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to parse JSON array", e);
                }
            };
        } else if (fieldType.equals(BasicArrayTypeInfo.BOOLEAN_ARRAY_TYPE_INFO)) {
            return result -> {
                try {
                    return objectMapper.readValue(result, new TypeReference<List<Boolean>>() {
                    }).toArray(new Boolean[0]);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to parse JSON array", e);
                }
            };
        } else if (fieldType.equals(BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO)) {
            return result -> {
                try {
                    return objectMapper.readValue(result, new TypeReference<List<Byte>>() {
                    }).toArray(new Byte[0]);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to parse JSON array", e);
                }
            };
        } else if (fieldType.equals(BasicArrayTypeInfo.SHORT_ARRAY_TYPE_INFO)) {
            return result -> {
                try {
                    return objectMapper.readValue(result, new TypeReference<List<Short>>() {
                    }).toArray(new Short[0]);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to parse JSON array", e);
                }
            };
        } else if (fieldType.equals(BasicArrayTypeInfo.INT_ARRAY_TYPE_INFO)) {
            return result -> {
                try {
                    return objectMapper.readValue(result, new TypeReference<List<Integer>>() {
                    }).toArray(new Integer[0]);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to parse JSON array", e);
                }
            };
        } else if (fieldType.equals(BasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO)) {
            return result -> {
                try {
                    return objectMapper.readValue(result, new TypeReference<List<Long>>() {
                    }).toArray(new Long[0]);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to parse JSON array", e);
                }
            };
        } else if (fieldType.equals(BasicArrayTypeInfo.FLOAT_ARRAY_TYPE_INFO)) {
            return result -> {
                try {
                    return objectMapper.readValue(result, new TypeReference<List<Float>>() {
                    }).toArray(new Float[0]);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to parse JSON array", e);
                }
            };
        } else if (fieldType.equals(BasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO)) {
            return result -> {
                try {
                    return objectMapper.readValue(result, new TypeReference<List<Double>>() {
                    }).toArray(new Double[0]);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to parse JSON array", e);
                }
            };
        } else if (fieldType.equals(BasicArrayTypeInfo.CHAR_ARRAY_TYPE_INFO)) {
            return result -> {
                try {
                    return objectMapper.readValue(result, new TypeReference<List<Character>>() {
                    }).toArray(new Character[0]);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to parse JSON array", e);
                }
            };
        } else {
            throw new UnsupportedOperationException("Unsupported field type: " + fieldType);
        }
    }

    private static RedisSerializationConverter createSerializer(TypeInformation<?> fieldType) {
        if (fieldType.equals(Types.STRING)) {
            return (row, index) -> String.valueOf(row.getField(index));
        } else if (fieldType.equals(Types.PRIMITIVE_ARRAY(Types.BYTE))) {
            return (row, index) -> {
                byte[] bytes = (byte[]) row.getField(index);
                return bytes == null ? null : Base64.getEncoder().encodeToString(bytes);
            };
        } else {
            return (row, index) -> String.valueOf(row.getField(index));
        }
    }

    @FunctionalInterface
    interface RedisDeserializationConverter extends Serializable {

        Object deserialize(String field);
    }

    @FunctionalInterface
    interface RedisSerializationConverter extends Serializable {

        String serialize(Row row, Integer index);
    }
}
