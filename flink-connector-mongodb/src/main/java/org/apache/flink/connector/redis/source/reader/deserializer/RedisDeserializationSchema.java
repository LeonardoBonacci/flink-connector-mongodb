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

package org.apache.flink.connector.redis.source.reader.deserializer;

import java.io.IOException;
import java.io.Serializable;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.redis.common.utils.RedisHash;
import org.apache.flink.util.Collector;

/**
 * A schema bridge for deserializing the MongoDB's {@code BsonDocument} into a flink managed
 * instance.
 *
 * @param <T> The output message type for sinking to downstream flink operator.
 */
@PublicEvolving
public interface RedisDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {

    /**
     * Deserializes the kv pair.
     *
     * @param document The kv pair to deserialize.
     * @return The deserialized message as an object (null if the message cannot be deserialized).
     */
    T deserialize(RedisHash document) throws IOException;

    /**
     * Deserializes the kv pair.
     *
     * <p>Can output multiple records through the {@link Collector}. Note that number and size of
     * the produced records should be relatively small. Depending on the source implementation
     * records can be buffered in memory or collecting records might delay emitting checkpoint
     * barrier.
     *
     * @param document The kv pair to deserialize.
     * @param out The collector to put the resulting messages.
     */
    default void deserialize(RedisHash readObject, Collector<T> out) throws IOException {
        T deserialize = deserialize(readObject);
        if (deserialize != null) {
            out.collect(deserialize);
        }
    }
}