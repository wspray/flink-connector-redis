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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.redis.config.RedisValueDataStructure;
import org.apache.flink.streaming.connectors.redis.stream.converter.RedisRowConverter;
import org.apache.flink.types.Row;

import java.util.List;

import static org.apache.flink.streaming.connectors.redis.table.RedisDynamicTableFactory.CACHE_SEPERATOR;

public class RedisResultWrapper {

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
            List<TypeInformation> dataTypes) {
        if (redisValueDataStructure == RedisValueDataStructure.column) {
            Row row = new Row(2);
            row.setField(
                    0,
                    RedisRowConverter.dataTypeFromString(
                            dataTypes.get(0), String.valueOf(keys[0])));
            if (value == null) {
                row.setField(0, null);
                return row;
            }

            row.setField(
                    1,
                    RedisRowConverter.dataTypeFromString(dataTypes.get(1), value));
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
    public static Row createRowDataForRow(String value, List<TypeInformation> dataTypes) {
        Row row = new Row(dataTypes.size());
        if (value == null) {
            return row;
        }

        String[] values = value.split(CACHE_SEPERATOR);
        for (int i = 0; i < dataTypes.size(); i++) {
            if (i < values.length) {
                row.setField(
                        i,
                        RedisRowConverter.dataTypeFromString(
                                dataTypes.get(i), values[i]));
            } else {
                row.setField(i, null);
            }
        }
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
            List<TypeInformation> dataTypes) {
        if (redisValueDataStructure == RedisValueDataStructure.column) {
            Row row = new Row(3);
            row.setField(
                    0,
                    RedisRowConverter.dataTypeFromString(
                            dataTypes.get(0), String.valueOf(keys[0])));
            row.setField(
                    1,
                    RedisRowConverter.dataTypeFromString(
                            dataTypes.get(1), String.valueOf(keys[1])));

            if (value == null) {
                return row;
            }
            row.setField(
                    2,
                    RedisRowConverter.dataTypeFromString(dataTypes.get(2), value));
            return row;
        }
        return createRowDataForRow(value, dataTypes);
    }

    public static Row createRowDataForSortedSet(
            Object[] keys, Double value, List<TypeInformation> dataTypes) {
        Row row = new Row(3);
        row.setField(
                0,
                RedisRowConverter.dataTypeFromString(
                        dataTypes.get(0), String.valueOf(keys[0])));
        row.setField(
                2,
                RedisRowConverter.dataTypeFromString(
                        dataTypes.get(2), String.valueOf(keys[1])));

        if (value == null) {
            return row;
        }
        row.setField(1, value);
        return row;
    }
}
