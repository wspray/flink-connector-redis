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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;

/**
 * @param <T>
 */
public interface RedisSinkRowMapper<T> extends RedisMapper<T> {

    /**
     * Extracts key from data.
     *
     * @param row source data
     * @return key
     */
    String getKeyFromData(Row row, TypeInformation typeInformation, Integer keyIndex);

    /**
     * Extracts value from data.
     *
     * @param row source data
     * @return value
     */
    String getValueFromData(Row row, TypeInformation typeInformation, Integer valueIndex);

    /**
     * @param row
     * @param fieldIndex
     * @return
     */
    String getFieldFromData(Row row, TypeInformation typeInformation, Integer fieldIndex);
}
