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

package org.apache.flink.connector.redis.sink.writer.serializer;

import java.io.Serializable;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.redis.common.utils.RedisHash;
import org.apache.flink.connector.redis.sink.config.RedisWriteOptions;
import org.apache.flink.connector.redis.sink.writer.context.RedisSinkContext;

/**
 * The serialization schema for how to serialize records into Redis.
 *
 * @param <IN> The message type send to Redis.
 */
@PublicEvolving
public interface RedisSerializationSchema<IN> extends Serializable {

    /**
     * Initialization method for the schema. It is called before the actual working methods {@link
     * #serialize(Object, RedisSinkContext)} and thus suitable for one-time setup work.
     *
     * <p>The provided {@link SerializationSchema.InitializationContext} can be used to access
     * additional features such as registering user metrics.
     *
     * @param initializationContext Contextual information that can be used during initialization.
     * @param sinkContext Runtime information i.e. partitions, subtaskId.
     * @param sinkConfiguration All the configure options for the MongoDB sink.
     */
    default void open(
            SerializationSchema.InitializationContext initializationContext,
            RedisSinkContext sinkContext,
            RedisWriteOptions sinkConfiguration)
            throws Exception {
        // Nothing to do by default.
    }

    /**
     * Serializes the given element into {@link RedisHash}.
     *
     * @param element Element to be serialized.
     * @param sinkContext Context to provide extra information.
     */
    RedisHash serialize(IN element, RedisSinkContext sinkContext);
}
