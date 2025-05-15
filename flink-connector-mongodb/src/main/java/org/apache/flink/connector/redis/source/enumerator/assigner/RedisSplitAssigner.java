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

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.redis.source.enumerator.RedisSourceEnumerator;
import org.apache.flink.connector.redis.source.enumerator.RedisSourceEnumState;
import org.apache.flink.connector.redis.source.split.RedisSourceSplit;

/** The split assigner for {@link MongoSourceSplit}. */
@Internal
public interface RedisSplitAssigner {

    /**
     * Called to open the assigner to acquire any resources, like threads or network connections.
     */
    void open();

    /**
     * Called to close the assigner, in case it holds on to any resources, like threads or network
     * connections.
     */
    void close() throws IOException;

    /**
     * Gets the next split to assign to {@link MongoSourceSplitReader} when {@link
     * RedisSourceEnumerator} receives a split request, until there are {@link #noMoreSplits()}.
     */
    Optional<RedisSourceSplit> getNext();

    /**
     * Adds a set of splits to this assigner. This happens for example when some split processing
     * failed and the splits need to be re-added.
     */
    void addSplitsBack(Collection<RedisSourceSplit> splits);

    /** Snapshot the current assign state into checkpoint. */
    RedisSourceEnumState snapshotState(long checkpointId);

    /** Return whether there are no more splits. */
    boolean noMoreSplits();
}