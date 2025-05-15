/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *   http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.connector.redis.source.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.redis.common.utils.RedisHash;

/** MongoDB source split state for {@link RedisScanSourceSplit}. */
@Internal
public class RedisScanSourceSplitState implements RedisSourceSplitState {

    private final RedisScanSourceSplit scanSplit;

    private int offset;

    public RedisScanSourceSplitState(RedisScanSourceSplit scanSplit) {
        this.scanSplit = scanSplit;
        this.offset = scanSplit.getOffset();
    }

    @Override
    public RedisScanSourceSplit toRedisSourceSplit() {
        return new RedisScanSourceSplit(
                scanSplit.splitId(),
                scanSplit.getDatabase(),
                scanSplit.getCollection(),
                scanSplit.getMin(),
                scanSplit.getMax(),
                scanSplit.getHint(),
                offset);
    }

    @Override
    public void updateOffset(RedisHash readObject) {
        offset++;
    }
}