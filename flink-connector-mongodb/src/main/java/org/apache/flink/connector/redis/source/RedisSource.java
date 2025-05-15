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

package org.apache.flink.connector.redis.source;

import static org.apache.flink.util.Preconditions.checkNotNull;

import java.util.List;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.redis.common.config.RedisConnectionOptions;
import org.apache.flink.connector.redis.common.utils.RedisHash;
import org.apache.flink.connector.redis.source.config.RedisReadOptions;
import org.apache.flink.connector.redis.source.enumerator.MongoSourceEnumStateSerializer;
import org.apache.flink.connector.redis.source.enumerator.RedisSourceEnumState;
import org.apache.flink.connector.redis.source.enumerator.RedisSourceEnumerator;
import org.apache.flink.connector.redis.source.enumerator.assigner.MongoScanSplitAssigner;
import org.apache.flink.connector.redis.source.enumerator.assigner.RedisSplitAssigner;
import org.apache.flink.connector.redis.source.reader.RedisSourceReader;
import org.apache.flink.connector.redis.source.reader.RedisSourceReaderContext;
import org.apache.flink.connector.redis.source.reader.deserializer.RedisDeserializationSchema;
import org.apache.flink.connector.redis.source.reader.emitter.RedisRecordEmitter;
import org.apache.flink.connector.redis.source.reader.split.MongoScanSourceSplitReader;
import org.apache.flink.connector.redis.source.split.MongoSourceSplitSerializer;
import org.apache.flink.connector.redis.source.split.RedisSourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * The Source implementation of MongoDB. Use a {@link MongoSourceBuilder} to construct a {@link
 * RedisSource}. The following example shows how to create a MongoSource emitting records of <code>
 * String</code> type.
 *
 * <pre>{@code
 * MongoSource<String> source = MongoSource.<String>builder()
 *      .setUri("mongodb://user:password@127.0.0.1:27017")
 *      .setDeserializationSchema(new RedisJsonDeserializationSchema())
 *      .build();
 * }</pre>
 *
 * <p>See {@link MongoSourceBuilder} for more details.
 *
 * @param <OUT> The output type of the source.
 */
@PublicEvolving
public class RedisSource<OUT>
        implements Source<OUT, RedisSourceSplit, RedisSourceEnumState>, ResultTypeQueryable<OUT> {

    private static final long serialVersionUID = 1L;

    /** The connection options for Redis source. */
    private final RedisConnectionOptions connectionOptions;

    /** The read options for Redis source. */
    private final RedisReadOptions readOptions;

    /** The limit for MongoDB source. */
    private final int limit;

    /** The boundedness for MongoDB source. */
    private final Boundedness boundedness;

    /** The mongo deserialization schema used for deserializing message. */
    private final RedisDeserializationSchema<OUT> deserializationSchema;

    RedisSource(
            RedisConnectionOptions connectionOptions,
            RedisReadOptions readOptions,
            @Nullable List<String> projectedFields,
            int limit,
            RedisDeserializationSchema<OUT> deserializationSchema) {
        this.connectionOptions = checkNotNull(connectionOptions);
        this.readOptions = checkNotNull(readOptions);
        this.limit = limit;
        // Only support bounded mode for now.
        // We can implement unbounded mode by ChangeStream future.
        this.boundedness = Boundedness.BOUNDED;
        this.deserializationSchema = checkNotNull(deserializationSchema);
    }

    /**
     * Get a MongoSourceBuilder to builder a {@link RedisSource}.
     *
     * @return a Mongo source builder.
     */
    public static <OUT> MongoSourceBuilder<OUT> builder() {
        return new MongoSourceBuilder<>();
    }

    @Override
    public Boundedness getBoundedness() {
        return boundedness;
    }

    @Override
    public SourceReader<OUT, RedisSourceSplit> createReader(SourceReaderContext readerContext) {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<RedisHash>> elementsQueue =
                new FutureCompletingBlockingQueue<>();

        RedisSourceReaderContext mongoReaderContext =
                new RedisSourceReaderContext(readerContext, limit);

        Supplier<SplitReader<RedisHash, RedisSourceSplit>> splitReaderSupplier =
                () ->
                        new MongoScanSourceSplitReader(
                                connectionOptions,
                                readOptions,
                                mongoReaderContext);

        return new RedisSourceReader<>(
                elementsQueue,
                splitReaderSupplier,
                new RedisRecordEmitter<>(deserializationSchema),
                mongoReaderContext);
    }

    @Override
    public SplitEnumerator<RedisSourceSplit, RedisSourceEnumState> createEnumerator(
            SplitEnumeratorContext<RedisSourceSplit> enumContext) {
        RedisSourceEnumState initialState = RedisSourceEnumState.initialState();
        RedisSplitAssigner splitAssigner =
                new MongoScanSplitAssigner(connectionOptions, readOptions, initialState);
        return new RedisSourceEnumerator(boundedness, enumContext, splitAssigner);
    }

    @Override
    public SplitEnumerator<RedisSourceSplit, RedisSourceEnumState> restoreEnumerator(
            SplitEnumeratorContext<RedisSourceSplit> enumContext, RedisSourceEnumState checkpoint) {
        RedisSplitAssigner splitAssigner =
                new MongoScanSplitAssigner(connectionOptions, readOptions, checkpoint);
        return new RedisSourceEnumerator(boundedness, enumContext, splitAssigner);
    }

    @Override
    public SimpleVersionedSerializer<RedisSourceSplit> getSplitSerializer() {
        return MongoSourceSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<RedisSourceEnumState> getEnumeratorCheckpointSerializer() {
        return MongoSourceEnumStateSerializer.INSTANCE;
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return deserializationSchema.getProducedType();
    }
}