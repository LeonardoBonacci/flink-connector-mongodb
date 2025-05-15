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

package org.apache.flink.connector.redis.common.config;

import static org.apache.flink.util.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.Objects;

import org.apache.flink.annotation.PublicEvolving;

/** The connection configuration class for Redis. */
@PublicEvolving
public class RedisConnectionOptions implements Serializable {

    private final String uri;

    private RedisConnectionOptions(String uri) {
        this.uri = checkNotNull(uri);
    }

    public String getUri() {
        return uri;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RedisConnectionOptions that = (RedisConnectionOptions) o;
        return Objects.equals(uri, that.uri);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uri);
    }

    public static RedisConnectionOptionsBuilder builder() {
        return new RedisConnectionOptionsBuilder();
    }

    /** Builder for {@link RedisConnectionOptions}. */
    @PublicEvolving
    public static class RedisConnectionOptionsBuilder {
        private String uri;

        private RedisConnectionOptionsBuilder() {}

        /**
         * Sets the connection string of Redis.
         *
         * @param uri connection string of Redis
         * @return this builder
         */
        public RedisConnectionOptionsBuilder setUri(String uri) {
            this.uri = uri;
            return this;
        }

        /**
         * Build the {@link RedisConnectionOptions}.
         *
         * @return a RedisConnectionOptions with the settings made for this builder.
         */
        public RedisConnectionOptions build() {
            return new RedisConnectionOptions(uri);
        }
    }
}
