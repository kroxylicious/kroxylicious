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
package io.strimzi.kproxy;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.strimzi.kproxy.codec.KafkaFrame;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class ApiVersionsResponseHandler extends ChannelInboundHandlerAdapter {
    private static final Logger LOGGER = LogManager.getLogger(ApiVersionsResponseHandler.class);
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        KafkaFrame frame = (KafkaFrame) msg;
        if (frame.apiKey() == ApiKeys.API_VERSIONS) {
            var resp = (ApiVersionsResponseData) frame.body();
            intersectApiVersions(ctx, resp);
        }
        super.channelRead(ctx, msg);
    }

    private static void intersectApiVersions(ChannelHandlerContext ctx, ApiVersionsResponseData resp) {
        for (var key : resp.apiKeys()) {
            short apiId = key.apiKey();
            if (ApiKeys.hasId(apiId)) {
                ApiKeys apiKey = ApiKeys.forId(apiId);
                intersectApiVersion(ctx, key, apiKey);
            }
        }
    }

    /**
     * Update the given {@code key}'s max and min versions so that the client uses APIs versions mutually
     * understood by both the proxy and the broker.
     * @param ctx The context.
     * @param key The key data from an upstream API_VERSIONS response.
     * @param apiKey The proxy's API key for this API.
     */
    private static void intersectApiVersion(ChannelHandlerContext ctx, ApiVersionsResponseData.ApiVersion key, ApiKeys apiKey) {
        short mutualMin = (short) Math.max(
                key.minVersion(),
                apiKey.messageType.lowestSupportedVersion());
        if (mutualMin != key.minVersion()) {
            LOGGER.trace("[{}] Use {} min version {} (was: {})", ctx.channel(), apiKey, mutualMin, key.maxVersion());
            key.setMinVersion(mutualMin);
        }

        short mutualMax = (short) Math.min(
                key.maxVersion(),
                apiKey.messageType.highestSupportedVersion());
        if (mutualMax != key.maxVersion()) {
            LOGGER.trace("[{}] Use {} max version {} (was: {})", ctx.channel(), apiKey, mutualMax, key.maxVersion());
            key.setMaxVersion(mutualMax);
        }
    }
}
