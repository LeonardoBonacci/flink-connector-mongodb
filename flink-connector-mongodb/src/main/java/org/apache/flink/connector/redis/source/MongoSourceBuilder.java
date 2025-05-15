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

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

import java.util.List;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.redis.common.config.RedisConnectionOptions;
import org.apache.flink.connector.redis.common.utils.RedisHash;
import org.apache.flink.connector.redis.source.config.RedisReadOptions;
import org.apache.flink.connector.redis.source.enumerator.splitter.PartitionStrategy;
import org.apache.flink.connector.redis.source.reader.deserializer.RedisDeserializationSchema;
import org.apache.flink.connector.redis.source.reader.split.MongoScanSourceSplitReader;

/**
 * The builder class for {@link RedisSource} to make it easier for the users to construct a {@link
 * RedisSource}.
 *
 * @param <OUT> The output type of the source.
 */
@PublicEvolving
public class MongoSourceBuilder<OUT> {

    private final RedisConnectionOptions.RedisConnectionOptionsBuilder connectionOptionsBuilder;
    private final RedisReadOptions.MongoReadOptionsBuilder readOptionsBuilder;

    private List<String> projectedFields;

    private int limit = -1;
    private RedisDeserializationSchema<OUT> deserializationSchema;

    MongoSourceBuilder() {
        this.connectionOptionsBuilder = RedisConnectionOptions.builder();
        this.readOptionsBuilder = RedisReadOptions.builder();
    }

    /**
     * Sets the connection string of MongoDB.
     *
     * @param uri connection string of MongoDB
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setUri(String uri) {
        connectionOptionsBuilder.setUri(uri);
        return this;
    }

    /**
     * Sets the number of documents should be fetched per round-trip when reading.
     *
     * @param fetchSize the number of documents should be fetched per round-trip when reading.
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setFetchSize(int fetchSize) {
        readOptionsBuilder.setFetchSize(fetchSize);
        return this;
    }

    /**
     * The MongoDB server normally times out idle cursors after an inactivity period (10 minutes) to
     * prevent excess memory use. Set this option to prevent that. If a session is idle for longer
     * than 30 minutes, the MongoDB server marks that session as expired and may close it at any
     * time. When the MongoDB server closes the session, it also kills any in-progress operations
     * and open cursors associated with the session. This includes cursors configured with {@code
     * noCursorTimeout()} or a {@code maxTimeMS()} greater than 30 minutes.
     *
     * @param noCursorTimeout Set this option to true to prevent cursor timeout (10 minutes)
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setNoCursorTimeout(boolean noCursorTimeout) {
        readOptionsBuilder.setNoCursorTimeout(noCursorTimeout);
        return this;
    }

    /**
     * Sets the partition strategy. Available partition strategies are single, sample, split-vector,
     * sharded and default. You can see {@link PartitionStrategy} for detail.
     *
     * @param partitionStrategy the strategy of a partition.
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setPartitionStrategy(PartitionStrategy partitionStrategy) {
        readOptionsBuilder.setPartitionStrategy(partitionStrategy);
        return this;
    }

    /**
     * Sets the partition memory size of MongoDB split. Split a MongoDB collection into multiple
     * partitions according to the partition memory size. Partitions can be read in parallel by
     * multiple {@link MongoScanSourceSplitReader} to speed up the overall read time.
     *
     * @param partitionSize the memory size of a partition.
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setPartitionSize(MemorySize partitionSize) {
        readOptionsBuilder.setPartitionSize(partitionSize);
        return this;
    }

    /**
     * Sets the number of samples to take per partition which is only used for the sample partition
     * strategy {@link PartitionStrategy#SAMPLE}. The sample partitioner samples the collection,
     * projects and sorts by the partition fields. Then uses every {@code samplesPerPartition} as
     * the value to use to calculate the partition boundaries. The total number of samples taken is:
     * samples per partition * ( count of documents / number of documents per partition).
     *
     * @param samplesPerPartition number of samples per partition.
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setSamplesPerPartition(int samplesPerPartition) {
        readOptionsBuilder.setSamplesPerPartition(samplesPerPartition);
        return this;
    }

    /**
     * Sets the limit of documents to read. If limit is not set or set to -1, the documents of the
     * entire collection will be read.
     *
     * @param limit the limit of documents to read.
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setLimit(int limit) {
        checkArgument(limit == -1 || limit > 0, "The limit must be larger than 0");
        this.limit = limit;
        return this;
    }

    /**
     * Sets the deserialization schema for MongoDB {@link RedisHash}.
     *
     * @param deserializationSchema the deserialization schema to deserialize {@link RedisHash}.
     * @return this builder
     */
    public MongoSourceBuilder<OUT> setDeserializationSchema(
            RedisDeserializationSchema<OUT> deserializationSchema) {
        checkNotNull(deserializationSchema, "The deserialization schema must not be null");
        this.deserializationSchema = deserializationSchema;
        return this;
    }

    /**
     * Build the {@link RedisSource}.
     *
     * @return a MongoSource with the settings made for this builder.
     */
    public RedisSource<OUT> build() {
        checkNotNull(deserializationSchema, "The deserialization schema must be supplied");
        return new RedisSource<>(
                connectionOptionsBuilder.build(),
                readOptionsBuilder.build(),
                projectedFields,
                limit,
                deserializationSchema);
    }
}