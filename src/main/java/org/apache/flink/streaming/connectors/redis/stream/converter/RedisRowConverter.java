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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Types;

/** redis serialize for stream API. */
public class RedisRowConverter {

    private static final int TIMESTAMP_PRECISION_MIN = 0;
    private static final int TIMESTAMP_PRECISION_MAX = 3;

    public static Object dataTypeFromString(TypeInformation<?> fieldType, String result) {
        return createDeserializer(fieldType).deserialize(result);
    }

    public static String rowToString(TypeInformation<?> fieldType, Row row, Integer index) {
        if (row.getField(index) == null) {
            return null;
        }
        return createSerializer(fieldType).serialize(row, index);
    }

    public static RedisDeserializationConverter createDeserializer(TypeInformation<?> fieldType) {
        if (fieldType.equals(BasicTypeInfo.BIG_INT_TYPE_INFO)) {
            return result -> new BigDecimal(result);
        } else if (fieldType.equals(BasicTypeInfo.FLOAT_TYPE_INFO)) {
            return Float::valueOf;
        } else if (fieldType.equals(BasicTypeInfo.DOUBLE_TYPE_INFO)) {
            return Double::valueOf;
        } else if (fieldType.equals(BasicTypeInfo.STRING_TYPE_INFO)) {
            return result -> result;
        } else if (fieldType.equals(BasicTypeInfo.BOOLEAN_TYPE_INFO)) {
            return Boolean::valueOf;
        } else if (fieldType.equals(BasicTypeInfo.BYTE_TYPE_INFO)) {
            return Byte::valueOf;
        } else if (fieldType.equals(BasicTypeInfo.SHORT_TYPE_INFO)) {
            return Short::valueOf;
        } else if (fieldType.equals(BasicTypeInfo.INT_TYPE_INFO)) {
            return Integer::valueOf;
        } else if (fieldType.equals(BasicTypeInfo.LONG_TYPE_INFO)) {
            return Long::valueOf;
        } else if (fieldType.equals(BasicTypeInfo.DATE_TYPE_INFO)) {
            return Date::valueOf;
        } else if (fieldType.equals(Types.TIME)) {
            return Time::valueOf;
//        } else if (fieldType.equals(BasicTypeInfo.TIMESTAMP_TYPE_INFO)) {
//            return Timestamp::valueOf;
//        } else if (fieldType.equals(BasicTypeInfo.BINARY_TYPE_INFO)) {
//            return result -> Base64.getDecoder().decode(result);
        } else {
            throw new UnsupportedOperationException("Unsupported field type: " + fieldType);
        }
    }

    private static RedisSerializationConverter createSerializer(TypeInformation<?> fieldType) {
        if (fieldType.equals(BasicTypeInfo.STRING_TYPE_INFO)) {
            return (row, index) -> row.getField(index).toString();
//        } else if (fieldType.equals(BasicTypeInfo.BINARY_TYPE_INFO)) {
//            return (row, index) -> Base64.getEncoder().encodeToString((byte[]) row.getField(index));
        } else {
            return (row, index) -> row.getField(index).toString();
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
