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

import static org.apache.flink.connector.redis.common.utils.RedisSerdeUtils.deserializeList;
import static org.apache.flink.connector.redis.common.utils.RedisSerdeUtils.deserializeMap;
import static org.apache.flink.connector.redis.common.utils.RedisSerdeUtils.serializeList;
import static org.apache.flink.connector.redis.common.utils.RedisSerdeUtils.serializeMap;
import static org.apache.flink.connector.redis.source.split.MongoSourceSplitSerializer.SCAN_SPLIT_FLAG;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.redis.source.split.MongoSourceSplitSerializer;
import org.apache.flink.connector.redis.source.split.RedisScanSourceSplit;
import org.apache.flink.connector.redis.source.split.RedisSourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/** The {@link SimpleVersionedSerializer Serializer} for the enumerator state of Mongo source. */
@Internal
public class MongoSourceEnumStateSerializer
        implements SimpleVersionedSerializer<RedisSourceEnumState> {

    public static final MongoSourceEnumStateSerializer INSTANCE =
            new MongoSourceEnumStateSerializer();

    private MongoSourceEnumStateSerializer() {
        // Singleton instance.
    }

    @Override
    public int getVersion() {
        // We use MongoSourceSplitSerializer's version because we use reuse this class.
        return MongoSourceSplitSerializer.CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(RedisSourceEnumState state) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            serializeList(out, state.getRemainingCollections(), DataOutputStream::writeUTF);

            serializeList(out, state.getAlreadyProcessedCollections(), DataOutputStream::writeUTF);

            serializeList(
                    out,
                    state.getRemainingScanSplits(),
                    MongoSourceSplitSerializer.INSTANCE::serializeMongoSplit);

            serializeMap(
                    out,
                    state.getAssignedScanSplits(),
                    DataOutputStream::writeUTF,
                    MongoSourceSplitSerializer.INSTANCE::serializeMongoSplit);

            out.writeBoolean(state.isInitialized());

            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public RedisSourceEnumState deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            List<String> remainingCollections = deserializeList(in, DataInput::readUTF);
            List<String> alreadyProcessedCollections = deserializeList(in, DataInput::readUTF);
            List<RedisScanSourceSplit> remainingScanSplits =
                    deserializeList(in, i -> deserializeRedisScanSourceSplit(version, i));

            Map<String, RedisScanSourceSplit> assignedScanSplits =
                    deserializeMap(
                            in,
                            DataInput::readUTF,
                            i -> deserializeRedisScanSourceSplit(version, i));

            boolean initialized = in.readBoolean();

            return new RedisSourceEnumState(
                    alreadyProcessedCollections,
                    remainingScanSplits,
                    assignedScanSplits,
                    initialized);
        }
    }

    private static RedisSourceSplit deserializeRedisScanSourceSplit(
            int version, DataInputStream in) throws IOException {
        int splitKind = in.readInt();
        if (splitKind == SCAN_SPLIT_FLAG) {
            return MongoSourceSplitSerializer.INSTANCE.deserializeMongoScanSourceSplit(version, in);
        }
        throw new IOException("Split kind mismatch expect 1 but was " + splitKind);
    }
}