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

package org.apache.flink.connector.mongodb.sink.writer;

import static org.apache.flink.util.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.mongodb.common.config.RedisConnectionOptions;
import org.apache.flink.connector.mongodb.common.utils.RedisHash;
import org.apache.flink.connector.mongodb.sink.config.RedisWriteOptions;
import org.apache.flink.connector.mongodb.sink.writer.context.DefaultRedisSinkContext;
import org.apache.flink.connector.mongodb.sink.writer.context.RedisSinkContext;
import org.apache.flink.connector.mongodb.sink.writer.serializer.RedisSerializationSchema;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

/**
 * This class is responsible for writing records to a Redis collection.
 *
 * @param <IN> The type of the input elements.
 */
@Internal
public class RedisWriter<IN> implements SinkWriter<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisWriter.class);

    private final RedisConnectionOptions connectionOptions;
    private final RedisWriteOptions writeOptions;
    private final RedisSerializationSchema<IN> serializationSchema;
    private final RedisSinkContext sinkContext;
    private final MailboxExecutor mailboxExecutor;
    private final boolean flushOnCheckpoint;
    private final List<RedisHash> bulkRequests = new ArrayList<>();
    private final Collector<RedisHash> collector;
    private final Counter numRecordsOut;
    private final RedisClient redisClient;
    private final StatefulRedisConnection<String, String> connection;
    private final long batchIntervalMs;
    private final int batchSize;

    private boolean checkpointInProgress = false;
    private volatile long lastSendTime = 0L;
    private volatile long ackTime = Long.MAX_VALUE;

    private transient volatile boolean closed = false;
    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;
    private transient volatile Exception flushException;

    public RedisWriter(
            RedisConnectionOptions connectionOptions,
            RedisWriteOptions writeOptions,
            boolean flushOnCheckpoint,
            Sink.InitContext initContext,
            RedisSerializationSchema<IN> serializationSchema) {
        this.connectionOptions = checkNotNull(connectionOptions);
        this.writeOptions = checkNotNull(writeOptions);
        this.serializationSchema = checkNotNull(serializationSchema);
        this.flushOnCheckpoint = flushOnCheckpoint;
        this.batchIntervalMs = writeOptions.getBatchIntervalMs();
        this.batchSize = writeOptions.getBatchSize();

        checkNotNull(initContext);
        this.mailboxExecutor = checkNotNull(initContext.getMailboxExecutor());

        SinkWriterMetricGroup metricGroup = checkNotNull(initContext.metricGroup());
        metricGroup.setCurrentSendTimeGauge(() -> ackTime - lastSendTime);

        this.numRecordsOut = metricGroup.getNumRecordsSendCounter();
        this.collector = new ListCollector<>(this.bulkRequests);

        // Initialize the serialization schema.
        this.sinkContext = new DefaultRedisSinkContext(initContext, writeOptions);
        try {
            SerializationSchema.InitializationContext initializationContext =
                    initContext.asSerializationSchemaInitializationContext();
            serializationSchema.open(initializationContext, sinkContext, writeOptions);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to open the RedisEmitter", e);
        }

        // Initialize the redis client and connection.
        this.redisClient = RedisClient.create(connectionOptions.getUri());
        this.connection = redisClient.connect();
        
        boolean flushOnlyOnCheckpoint = batchIntervalMs == -1 && batchSize == -1;

        if (!flushOnlyOnCheckpoint && batchIntervalMs > 0) {
            this.scheduler =
                    Executors.newScheduledThreadPool(1, new ExecutorThreadFactory("redis-writer"));

            this.scheduledFuture =
                    this.scheduler.scheduleWithFixedDelay(
                            () -> {
                                synchronized (RedisWriter.this) {
                                    if (!closed && isOverMaxBatchIntervalLimit()) {
                                        try {
                                            doBulkWrite();
                                        } catch (Exception e) {
                                            flushException = e;
                                        }
                                    }
                                }
                            },
                            batchIntervalMs,
                            batchIntervalMs,
                            TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public synchronized void write(IN element, Context context)
            throws IOException, InterruptedException {
        checkFlushException();

        // do not allow new bulk writes until all actions are flushed
        while (checkpointInProgress) {
            mailboxExecutor.yield();
        }
        RedisHash writeObject = serializationSchema.serialize(element, sinkContext);
        numRecordsOut.inc();
        collector.collect(writeObject);
        if (isOverMaxBatchSizeLimit() || isOverMaxBatchIntervalLimit()) {
            doBulkWrite();
        }
    }

    @Override
    public synchronized void flush(boolean endOfInput) throws IOException {
        checkFlushException();

        checkpointInProgress = true;
        while (!bulkRequests.isEmpty() && (flushOnCheckpoint || endOfInput)) {
            doBulkWrite();
        }
        checkpointInProgress = false;
    }

    @Override
    public synchronized void close() throws Exception {
        if (!closed) {
            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
                scheduler.shutdown();
            }

            if (!bulkRequests.isEmpty()) {
                try {
                    doBulkWrite();
                } catch (Exception e) {
                    LOG.error("Writing records to Redis failed when closing RedisWriter", e);
                    throw new IOException("Writing records to Redis failed.", e);
                } finally {
                    redisClient.close();
                    closed = true;
                }
            } else {
            		redisClient.close();
                closed = true;
            }
        }
    }

    @VisibleForTesting
    void doBulkWrite() throws IOException {
        if (bulkRequests.isEmpty()) {
            // no records to write
            return;
        }

        int maxRetries = writeOptions.getMaxRetries();
        long retryIntervalMs = writeOptions.getRetryIntervalMs();
        for (int i = 0; i <= maxRetries; i++) {
            try {
                lastSendTime = System.currentTimeMillis();
                
                RedisCommands<String, String> commands = connection.sync();
                bulkRequests.forEach(kv -> commands.hset(kv.getKey(), kv.getValue()));
                
                ackTime = System.currentTimeMillis();
                bulkRequests.clear();
                break;
            } catch (RuntimeException e) {
                LOG.debug("Bulk Write to Redis failed, retry times = {}", i, e);
                if (i >= maxRetries) {
                    LOG.error("Bulk Write to Redis failed", e);
                    throw new IOException(e);
                }
                try {
                    Thread.sleep(retryIntervalMs * (i + 1));
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException(
                            "Unable to flush; interrupted while doing another attempt", e);
                }
            }
        }
    }

    private boolean isOverMaxBatchSizeLimit() {
        return batchSize != -1 && bulkRequests.size() >= batchSize;
    }

    private boolean isOverMaxBatchIntervalLimit() {
        long lastSentInterval = System.currentTimeMillis() - lastSendTime;
        return batchIntervalMs != -1 && lastSentInterval >= batchIntervalMs;
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new RuntimeException("Writing records to Redis failed.", flushException);
        }
    }
}
