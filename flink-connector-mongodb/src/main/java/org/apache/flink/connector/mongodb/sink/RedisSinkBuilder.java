/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.mongodb.sink;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.mongodb.common.config.RedisConnectionOptions;
import org.apache.flink.connector.mongodb.sink.config.RedisWriteOptions;
import org.apache.flink.connector.mongodb.sink.writer.serializer.RedisSerializationSchema;
import org.apache.flink.util.InstantiationUtil;

/**
 * Base builder to construct a {@link RedisSink}.
 *
 * @param <IN> type of the records converted to MongoDB bulk request
 */
@PublicEvolving
public class RedisSinkBuilder<IN> {

    private final RedisConnectionOptions.RedisConnectionOptionsBuilder connectionOptionsBuilder;
    private final RedisWriteOptions.RedisWriteOptionsBuilder writeOptionsBuilder;

    private RedisSerializationSchema<IN> serializationSchema;

    RedisSinkBuilder() {
        this.connectionOptionsBuilder = RedisConnectionOptions.builder();
        this.writeOptionsBuilder = RedisWriteOptions.builder();
    }

    /**
     * Sets the connection string of Redis.
     *
     * @param uri connection string of Redis
     * @return this builder
     */
    public RedisSinkBuilder<IN> setUri(String uri) {
        connectionOptionsBuilder.setUri(uri);
        return this;
    }

    /**
     * Sets the maximum number of actions to buffer for each batch request. You can pass -1 to
     * disable batching.
     *
     * @param batchSize the maximum number of actions to buffer for each batch request.
     * @return this builder
     */
    public RedisSinkBuilder<IN> setBatchSize(int batchSize) {
        writeOptionsBuilder.setBatchSize(batchSize);
        return this;
    }

    /**
     * Sets the batch flush interval, in milliseconds. You can pass -1 to disable it.
     *
     * @param batchIntervalMs the batch flush interval, in milliseconds.
     * @return this builder
     */
    public RedisSinkBuilder<IN> setBatchIntervalMs(long batchIntervalMs) {
        writeOptionsBuilder.setBatchIntervalMs(batchIntervalMs);
        return this;
    }

    /**
     * Sets the max retry times if writing records failed.
     *
     * @param maxRetries the max retry times.
     * @return this builder
     */
    public RedisSinkBuilder<IN> setMaxRetries(int maxRetries) {
        writeOptionsBuilder.setMaxRetries(maxRetries);
        return this;
    }

    /**
     * Sets the wanted {@link DeliveryGuarantee}. The default delivery guarantee is {@link
     * DeliveryGuarantee#AT_LEAST_ONCE}
     *
     * @param deliveryGuarantee which describes the record emission behaviour
     * @return this builder
     */
    public RedisSinkBuilder<IN> setDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
        writeOptionsBuilder.setDeliveryGuarantee(deliveryGuarantee);
        return this;
    }

    /**
     * Sets the serialization schema which is invoked on every record to convert it to MongoDB bulk
     * request.
     *
     * @param serializationSchema to process records into MongoDB bulk {@link WriteModel}.
     * @return this builder
     */
    public RedisSinkBuilder<IN> setSerializationSchema(
            RedisSerializationSchema<IN> serializationSchema) {
        checkNotNull(serializationSchema);
        checkState(
                InstantiationUtil.isSerializable(serializationSchema),
                "The mongo serialization schema must be serializable.");
        this.serializationSchema = serializationSchema;
        return this;
    }

    /**
     * Constructs the {@link RedisSink} with the properties configured this builder.
     *
     * @return {@link RedisSink}
     */
    public RedisSink<IN> build() {
        checkNotNull(serializationSchema, "The serialization schema must be supplied");
        return new RedisSink<>(
                connectionOptionsBuilder.build(), writeOptionsBuilder.build(), serializationSchema);
    }
}
