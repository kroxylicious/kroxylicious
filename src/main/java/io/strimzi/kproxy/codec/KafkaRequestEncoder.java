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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KafkaRequestEncoder extends KafkaMessageEncoder {
    private static final Logger LOGGER = LogManager.getLogger(KafkaRequestEncoder.class);

    // TODO merge with the ResponseDecoder as KafkaBackendCodec
    // and merge the RequestDecoder and with ResponseEncoder as KafkaFrontendCodec

    public static class VersionedApi {
        public final short apiKey;
        public final short apiVersion;

        public VersionedApi(short apiKey, short apiVersion) {
            this.apiKey = apiKey;
            this.apiVersion = apiVersion;
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
        RequestHeaderData header = (RequestHeaderData) frame.header();
        correlation.put(header.correlationId(),
                new VersionedApi(header.apiKey(), header.requestApiVersion()));
    }
}
