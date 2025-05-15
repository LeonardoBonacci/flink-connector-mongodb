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

package org.apache.flink.connector.redis.source.reader.emitter;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.redis.common.utils.RedisHash;
import org.apache.flink.connector.redis.source.reader.RedisSourceReader;
import org.apache.flink.connector.redis.source.reader.deserializer.RedisDeserializationSchema;
import org.apache.flink.connector.redis.source.split.RedisSourceSplitState;
import org.apache.flink.util.Collector;

/**
 * The {@link RecordEmitter} implementation for {@link RedisSourceReader} . We would always update
 * the last consumed message id in this emitter.
 */
@Internal
public class RedisRecordEmitter<T>
        implements RecordEmitter<RedisHash, T, RedisSourceSplitState> {

    private final RedisDeserializationSchema<T> deserializationSchema;
    private final SourceOutputWrapper<T> sourceOutputWrapper;

    public RedisRecordEmitter(RedisDeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
        this.sourceOutputWrapper = new SourceOutputWrapper<>();
    }

    @Override
    public void emitRecord(
    			RedisHash readObject, SourceOutput<T> output, RedisSourceSplitState splitState)
            throws Exception {
        // Update current offset.
        splitState.updateOffset(readObject);
        // Sink the record to source output.
        sourceOutputWrapper.setSourceOutput(output);
        deserializationSchema.deserialize(readObject, sourceOutputWrapper);
    }

    private static class SourceOutputWrapper<T> implements Collector<T> {
        private SourceOutput<T> sourceOutput;

        @Override
        public void collect(T record) {
            sourceOutput.collect(record);
        }

        @Override
        public void close() {}

        private void setSourceOutput(SourceOutput<T> sourceOutput) {
            this.sourceOutput = sourceOutput;
        }
    }
}