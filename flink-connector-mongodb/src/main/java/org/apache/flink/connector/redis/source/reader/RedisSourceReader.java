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

package org.apache.flink.connector.redis.source.reader;

import java.util.Map;
import java.util.function.Supplier;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.redis.common.utils.RedisHash;
import org.apache.flink.connector.redis.source.split.RedisScanSourceSplit;
import org.apache.flink.connector.redis.source.split.RedisScanSourceSplitState;
import org.apache.flink.connector.redis.source.split.RedisSourceSplit;
import org.apache.flink.connector.redis.source.split.RedisSourceSplitState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The common mongo source reader for both ordered & unordered message consuming.
 *
 * @param <OUT> The output message type for flink.
 */
@Internal
public class RedisSourceReader<OUT>
        extends SingleThreadMultiplexSourceReaderBase<
                RedisHash, OUT, RedisSourceSplit, RedisSourceSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSourceReader.class);

    public RedisSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<RedisHash>> elementQueue,
            Supplier<SplitReader<RedisHash, RedisSourceSplit>> splitReaderSupplier,
            RecordEmitter<RedisHash, OUT, RedisSourceSplitState> recordEmitter,
            RedisSourceReaderContext readerContext) {
        super(
                elementQueue,
                new SingleThreadFetcherManager<>(elementQueue, splitReaderSupplier),
                recordEmitter,
                readerContext.getConfiguration(),
                readerContext);
    }

    @Override
    public void start() {
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected void onSplitFinished(Map<String, RedisSourceSplitState> finishedSplitIds) {
        for (RedisSourceSplitState splitState : finishedSplitIds.values()) {
            RedisSourceSplit sourceSplit = splitState.toRedisSourceSplit();
            LOG.info("Split {} is finished.", sourceSplit.splitId());
        }
        context.sendSplitRequest();
    }

    @Override
    protected RedisSourceSplitState initializedState(RedisSourceSplit split) {
        if (split instanceof RedisScanSourceSplit) {
            return new RedisScanSourceSplitState((RedisScanSourceSplit) split);
        } else {
            throw new IllegalArgumentException("Unknown split type.");
        }
    }

    @Override
    protected RedisSourceSplit toSplitType(String splitId, RedisSourceSplitState splitState) {
        return splitState.toRedisSourceSplit();
    }
}