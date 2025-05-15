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

package org.apache.flink.connector.redis.source.reader.split;

import java.io.IOException;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.redis.common.config.RedisConnectionOptions;
import org.apache.flink.connector.redis.common.utils.RedisHash;
import org.apache.flink.connector.redis.source.config.RedisReadOptions;
import org.apache.flink.connector.redis.source.reader.RedisSourceReaderContext;
import org.apache.flink.connector.redis.source.split.RedisScanSourceSplit;
import org.apache.flink.connector.redis.source.split.RedisSourceSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.lettuce.core.RedisClient;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.output.KeyValueStreamingChannel;

/** A split reader implements {@link SplitReader} for {@link RedisScanSourceSplit}. */
@Internal
public class MongoScanSourceSplitReader implements RedisSourceSplitReader<RedisSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoScanSourceSplitReader.class);

    private final RedisConnectionOptions connectionOptions;
    private final RedisReadOptions readOptions;
    private final RedisSourceReaderContext readerContext;

    private boolean closed = false;
    private boolean finished = false;
    private RedisClient redisClient;
    private ScanCursor currentCursor;
    private RedisScanSourceSplit currentSplit;

    public MongoScanSourceSplitReader(
            RedisConnectionOptions connectionOptions,
            RedisReadOptions readOptions,
            RedisSourceReaderContext readerContext) {
        this.connectionOptions = connectionOptions;
        this.readOptions = readOptions;
        this.readerContext = readerContext;
    }

    @Override
    public RecordsWithSplitIds<RedisHash> fetch() throws IOException {
        if (closed) {
            throw new IllegalStateException("Cannot fetch records from a closed split reader");
        }

        RecordsBySplits.Builder<RedisHash> builder = new RecordsBySplits.Builder<>();

        // Return when no split registered to this reader.
        if (currentSplit == null) {
            return builder.build();
        }

        currentCursor = getOrCreateCursor();
        int fetchSize = readOptions.getFetchSize();

        try {
            for (int recordNum = 0; recordNum < fetchSize; recordNum++) {
                if (currentCursor.hasNext()) {
                    builder.add(currentSplit, currentCursor.next());
                    readerContext.getReadCount().incrementAndGet();
                } else {
                    builder.addFinishedSplit(currentSplit.splitId());
                    finished = true;
                    break;
                }
            }
            return builder.build();
        } catch (RuntimeException e) {
            throw new IOException("Scan records form Redis failed", e);
        } finally {
            if (finished) {
                closeCursor();
            }
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<RedisSourceSplit> splitsChanges) {
        LOG.debug("Handle split changes {}", splitsChanges);

        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChanges.getClass()));
        }

        RedisSourceSplit sourceSplit = splitsChanges.splits().get(0);
        if (!(sourceSplit instanceof RedisScanSourceSplit)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SourceSplit type of %s is not supported.",
                            sourceSplit.getClass()));
        }

        this.currentSplit = (RedisScanSourceSplit) sourceSplit;
        this.finished = false;
    }

    @Override
    public void wakeUp() {
        // Close current cursor to cancel blocked hasNext(), next().
        closeCursor();
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            closeCursor();
        }
    }

    private ScanCursor getOrCreateCursor() {
        if (currentCursor == null) {
            LOG.debug("Opened cursor for split: {}", currentSplit);
            redisClient = RedisClient.create(connectionOptions.getUri());

            StatefulRedisConnection<String, String> connection = redisClient.connect();
            RedisCommands<String, String> commands = connection.sync();

            ScanArgs scanArgs = new ScanArgs().limit(100); // COUNT 100

            
            KeyValueStreamingChannel<String, String> channel = new KeyValueStreamingChannel<String, String>() {
                @Override
                public void onKeyValue(String key, String value) {
                    System.out.println("Key: " + key + ", Value: " + value);
                }
            };
            
            ScanCursor cursor = ScanCursor.INITIAL;
            do {
                cursor = commands.hscan(channel, "my-hash", cursor, scanArgs);
            } while (!cursor.isFinished());

            
            // Using MongoDB's cursor.min() and cursor.max() to limit an index bound.
            // When the index range is the primary key, the bound is (min <= _id < max).
            // Compound indexes and hash indexes bounds can also be supported in this way.
            // Please refer to https://www.mongodb.com/docs/manual/reference/method/cursor.min/
            FindIterable<RedisHash> findIterable =
                    redisClient
                            .min(currentSplit.getMin())
                            .max(currentSplit.getMax());

            // Current split was partially read and recovered from checkpoint
            if (currentSplit.getOffset() > 0) {
                findIterable.skip(currentSplit.getOffset());
            }

            currentCursor = findIterable.cursor();
        }
        return currentCursor;
    }

    private void closeCursor() {
        if (currentCursor != null) {
            LOG.debug("Closing cursor for split: {}", currentSplit);
            try {
              redisClient.close();
            } finally {
              redisClient = null;
            }
        }
    }
}