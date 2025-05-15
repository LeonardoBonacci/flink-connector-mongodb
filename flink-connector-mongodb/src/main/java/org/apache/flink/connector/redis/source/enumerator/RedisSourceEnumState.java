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

package org.apache.flink.connector.redis.source.enumerator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.redis.source.split.RedisScanSourceSplit;

/**
 * The state class for Redis source enumerator, used for storing the split state. This class is
 * managed and controlled by {@link RedisSplitAssigner}.
 */
@PublicEvolving
public class RedisSourceEnumState {

    /**
     * The paths that are no longer in the enumerator checkpoint, but have been processed before.
     */
    private final List<String> alreadyProcessedCollections;

    /** The scan splits in the checkpoint. */
    private final List<RedisScanSourceSplit> remainingScanSplits;

    /**
     * The scan splits that the {@link RedisSourceEnumerator} has assigned to {@link
     * MongoSourceSplitReader}s.
     */
    private final Map<String, RedisScanSourceSplit> assignedScanSplits;

    /** The pipeline has been triggered and topic partitions have been assigned to readers. */
    private final boolean initialized;

    public RedisSourceEnumState(
            List<String> alreadyProcessedCollections,
            List<RedisScanSourceSplit> remainingScanSplits,
            Map<String, RedisScanSourceSplit> assignedScanSplits,
            boolean initialized) {
        this.alreadyProcessedCollections = alreadyProcessedCollections;
        this.remainingScanSplits = remainingScanSplits;
        this.assignedScanSplits = assignedScanSplits;
        this.initialized = initialized;
    }

    public List<String> getAlreadyProcessedCollections() {
        return alreadyProcessedCollections;
    }

    public List<RedisScanSourceSplit> getRemainingScanSplits() {
        return remainingScanSplits;
    }

    public Map<String, RedisScanSourceSplit> getAssignedScanSplits() {
        return assignedScanSplits;
    }

    public boolean isInitialized() {
        return initialized;
    }

    /** The initial assignment state for Redis. */
    public static RedisSourceEnumState initialState() {
        return new RedisSourceEnumState(
                new ArrayList<>(), new ArrayList<>(), new HashMap<>(), false);
    }
}