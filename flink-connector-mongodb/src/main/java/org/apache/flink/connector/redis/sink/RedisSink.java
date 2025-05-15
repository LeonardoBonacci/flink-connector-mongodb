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

package org.apache.flink.connector.redis.sink;

import static org.apache.flink.util.Preconditions.checkNotNull;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.redis.common.config.RedisConnectionOptions;
import org.apache.flink.connector.redis.sink.config.RedisWriteOptions;
import org.apache.flink.connector.redis.sink.writer.RedisWriter;
import org.apache.flink.connector.redis.sink.writer.serializer.RedisSerializationSchema;

/**
 * Mongo sink converts each incoming element into Redis Hash (bulk write action) and
 * bulk writes to redis when the number of actions is greater than batchSize or the flush interval
 * is greater than batchIntervalMs.
 *
 * <p>The following example shows how to create a RedisSink sending records of a Redis Hash
 * type.
 *
 * <pre>{@code
 * RedisSink<Document> sink = RedisSink.<Document>builder()
 *     .setUri("mongodb://user:password@127.0.0.1:27017")
 *     .setDatabase("db")
 *     .setBatchSize(5)
 *     .setSerializationSchema(
 *         (doc, context) -> new InsertOneModel<>(doc.toBsonDocument()))
 *     .build();
 * }</pre>
 *
 * @param <IN> Type of the elements handled by this sink
 */
@PublicEvolving
public class RedisSink<IN> implements Sink<IN> {

    private static final long serialVersionUID = 1L;

    private final RedisConnectionOptions connectionOptions;
    private final RedisWriteOptions writeOptions;
    private final RedisSerializationSchema<IN> serializationSchema;

    RedisSink(
            RedisConnectionOptions connectionOptions,
            RedisWriteOptions writeOptions,
            RedisSerializationSchema<IN> serializationSchema) {
        this.connectionOptions = checkNotNull(connectionOptions);
        this.writeOptions = checkNotNull(writeOptions);
        this.serializationSchema = checkNotNull(serializationSchema);
        ClosureCleaner.clean(
                serializationSchema, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
    }

    public static <IN> RedisSinkBuilder<IN> builder() {
        return new RedisSinkBuilder<>();
    }

    @Override
    public SinkWriter<IN> createWriter(InitContext context) {
        return new RedisWriter<>(
                connectionOptions,
                writeOptions,
                writeOptions.getDeliveryGuarantee() == DeliveryGuarantee.AT_LEAST_ONCE,
                context,
                serializationSchema);
    }
}
