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

package org.apache.flink.connector.redis.source.enumerator.assigner;

import static org.apache.flink.util.Preconditions.checkState;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.redis.common.config.RedisConnectionOptions;
import org.apache.flink.connector.redis.source.config.RedisReadOptions;
import org.apache.flink.connector.redis.source.enumerator.RedisSourceEnumState;
import org.apache.flink.connector.redis.source.split.RedisScanSourceSplit;
import org.apache.flink.connector.redis.source.split.RedisSourceSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.lettuce.core.RedisClient;

/** The split assigner for {@link MongoScanSourceSplit}. */
@Internal
public class MongoScanSplitAssigner implements RedisSplitAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(MongoScanSplitAssigner.class);

    private final RedisConnectionOptions connectionOptions;

    private final List<String> alreadyProcessedCollections;
    private final LinkedList<RedisScanSourceSplit> remainingScanSplits;
    private final Map<String, RedisScanSourceSplit> assignedScanSplits;
    private boolean initialized;

    private RedisClient redisClient;

    public MongoScanSplitAssigner(
            RedisConnectionOptions connectionOptions,
            RedisReadOptions readOptions,
            RedisSourceEnumState sourceEnumState) {
        this.connectionOptions = connectionOptions;
        this.alreadyProcessedCollections = sourceEnumState.getAlreadyProcessedCollections();
        this.remainingScanSplits = new LinkedList<>(sourceEnumState.getRemainingScanSplits());
        this.assignedScanSplits = sourceEnumState.getAssignedScanSplits();
        this.initialized = sourceEnumState.isInitialized();
    }

    @Override
    public void open() {
        LOG.info("Mongo scan split assigner is opening.");
        if (!initialized) {
            redisClient = RedisClient.create(connectionOptions.getUri());
            initialized = true;
        }
    }

    @Override
    public Optional<RedisSourceSplit> getNext() {
        if (!remainingScanSplits.isEmpty()) {
            // return remaining splits firstly
            RedisScanSourceSplit split = remainingScanSplits.poll();
            assignedScanSplits.put(split.splitId(), split);
            return Optional.of(split);
        } else {
        		return Optional.empty();
        }
    }

    @Override
    public void addSplitsBack(Collection<RedisSourceSplit> splits) {
        for (RedisSourceSplit split : splits) {
            if (split instanceof RedisScanSourceSplit) {
                remainingScanSplits.add((RedisScanSourceSplit) split);
                // we should remove the add-backed splits from the assigned list,
                // because they are failed
                assignedScanSplits.remove(split.splitId());
            }
        }
    }

    @Override
    public RedisSourceEnumState snapshotState(long checkpointId) {
        return new RedisSourceEnumState(
                alreadyProcessedCollections,
                remainingScanSplits,
                assignedScanSplits,
                initialized);
    }

    @Override
    public boolean noMoreSplits() {
        checkState(initialized, "The noMoreSplits method was called but not initialized.");
        return remainingScanSplits.isEmpty();
    }

    @Override
    public void close() throws IOException {
        if (redisClient != null) {
        		redisClient.close();
            LOG.info("Mongo scan split assigner is closed.");
        }
    }
}