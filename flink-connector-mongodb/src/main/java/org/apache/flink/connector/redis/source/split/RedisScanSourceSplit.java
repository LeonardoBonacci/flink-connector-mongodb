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

package org.apache.flink.connector.redis.source.split;

import java.util.Objects;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.redis.common.utils.RedisHash;

/** A {@link SourceSplit} implementation for a Redis partition. */
@PublicEvolving
public class RedisScanSourceSplit extends RedisSourceSplit {

    private static final long serialVersionUID = 1L;

    private final RedisHash min;

    private final RedisHash max;

    private final int offset;

    public RedisScanSourceSplit(
            String splitId,
            RedisHash min,
            RedisHash max) {
        this(splitId, min, max, 0);
    }

    public RedisScanSourceSplit(
            String splitId,
            RedisHash min,
            RedisHash max,
            int offset) {
        super(splitId);
        this.min = min;
        this.max = max;
        this.offset = offset;
    }

    public RedisHash getMin() {
        return min;
    }

    public RedisHash getMax() {
        return max;
    }
    
    public int getOffset() {
        return offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        RedisScanSourceSplit split = (RedisScanSourceSplit) o;
        return Objects.equals(min, split.min)
                && Objects.equals(max, split.max)
                && offset == split.offset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), min, max, offset);
    }

    @Override
    public String toString() {
        return "RedisScanSourceSplit {"
                + " splitId="
                + splitId
                + ", min="
                + min
                + ", max="
                + max
                + ", offset="
                + offset
                + " }";
    }
}