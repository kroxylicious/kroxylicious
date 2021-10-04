/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.strimzi.kproxy.codec;

import java.util.Map;
import java.util.Objects;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KafkaRequestEncoder extends KafkaMessageEncoder {
    private static final Logger LOGGER = LogManager.getLogger(KafkaRequestEncoder.class);

    public static class VersionedApi {
        public final short apiKey;
        public final short apiVersion;

        public VersionedApi(short apiKey, short apiVersion) {
            this.apiKey = apiKey;
            this.apiVersion = apiVersion;
        }

        @Override
        public String toString() {
            return "VersionedApi(" +
                    "apiKey=" + apiKey +
                    ", apiVersion=" + apiVersion +
                    ')';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            VersionedApi that = (VersionedApi) o;
            return apiKey == that.apiKey && apiVersion == that.apiVersion;
        }

        @Override
        public int hashCode() {
            return Objects.hash(apiKey, apiVersion);
        }
    }

    private final Map<Integer, VersionedApi> correlation;

    public KafkaRequestEncoder(Map<Integer, VersionedApi> correlation) {
        this.correlation = correlation;
    }

    @Override
    protected Logger log() {
        return LOGGER;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, KafkaFrame frame, ByteBuf out) throws Exception {
        super.encode(ctx, frame, out);
        if (hasResponse(frame)) {
            RequestHeaderData header = (RequestHeaderData) frame.header();
            correlation.put(header.correlationId(),
                    new VersionedApi(frame.body().apiKey(), header.requestApiVersion()));
        }
    }

    @Override
    protected short headerVersion(KafkaFrame frame) {
        return frame.requestHeaderVersion();
    }

    private boolean hasResponse(KafkaFrame frame) {
        return frame.header().apiKey() != ApiKeys.PRODUCE.id ||
                ((ProduceRequestData) frame.body()).acks() != 0;
    }
}
