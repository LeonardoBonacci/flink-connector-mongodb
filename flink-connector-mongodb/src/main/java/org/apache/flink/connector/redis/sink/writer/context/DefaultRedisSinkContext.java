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

package org.apache.flink.connector.redis.sink.writer.context;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.redis.sink.config.RedisWriteOptions;

/** Default {@link RedisSinkContext} implementation. */
@Internal
public class DefaultRedisSinkContext implements RedisSinkContext {

    private final Sink.InitContext initContext;
    private final RedisWriteOptions writeOptions;

    public DefaultRedisSinkContext(Sink.InitContext initContext, RedisWriteOptions writeOptions) {
        this.initContext = initContext;
        this.writeOptions = writeOptions;
    }

    @Override
    public Sink.InitContext getInitContext() {
        return initContext;
    }

    @Override
    public long processTime() {
        return initContext.getProcessingTimeService().getCurrentProcessingTime();
    }

    @Override
    public RedisWriteOptions getWriteOptions() {
        return writeOptions;
    }
}
