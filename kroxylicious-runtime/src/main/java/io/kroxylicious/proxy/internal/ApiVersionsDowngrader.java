/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

import io.kroxylicious.proxy.frame.DecodedFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.Frame;

import static io.kroxylicious.proxy.internal.codec.KafkaRequestDecoder.CLIENT_AHEAD_OF_PROXY;

public class ApiVersionsDowngrader extends ChannelOutboundHandlerAdapter {
    private static final Logger LOGGER = LoggerFactory.getLogger(ApiVersionsDowngrader.class);

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof Frame responseFrame && responseFrame.requestResponseState().getStateOrDefault(CLIENT_AHEAD_OF_PROXY, false)) {
            if (responseFrame instanceof DecodedFrame<?, ?> decodedFrame) {
                if (decodedFrame.body() instanceof ApiVersionsResponseData data) {
                    if (data.errorCode() == Errors.UNSUPPORTED_VERSION.code()) {
                        // broker already doesn't support, forward intersected versions to client
                        LOGGER.info("{}: forwarding error response on, proxy max version ahead of broker", ctx);
                        ctx.write(msg, promise);
                    }
                    else {
                        // broker does support max proxy version, downgrade to v0 and set error code
                        downgradeToV0Response(ctx, promise, responseFrame, decodedFrame, data);
                    }
                }
                else {
                    ctx.fireExceptionCaught(new IllegalStateException("expect to only intercept ApiVersionsResponseData"));
                }
            }
            else {
                ctx.fireExceptionCaught(new IllegalStateException("expect ApiVersions to be decoded"));
            }
        }
        else {
            ctx.write(msg, promise);
        }
    }

    private static void downgradeToV0Response(ChannelHandlerContext ctx, ChannelPromise promise, Frame responseFrame, DecodedFrame<?, ?> decodedFrame,
                                              ApiVersionsResponseData data) {
        LOGGER.info("{}: respond with v0 unsupported version response", ctx);
        ResponseHeaderData header = new ResponseHeaderData();
        header.setCorrelationId(decodedFrame.correlationId());
        ApiVersionsResponseData apiVersionsResponseData = new ApiVersionsResponseData();
        apiVersionsResponseData.setErrorCode(Errors.UNSUPPORTED_VERSION.code());
        ApiVersionsResponseData.ApiVersionCollection collection = new ApiVersionsResponseData.ApiVersionCollection();
        ApiVersionsResponseData.ApiVersion version = data.apiKeys().find(ApiKeys.API_VERSIONS.id);
        if (version != null) {
            // copy, because min and max being set on the version prevents the version being added to the collection
            ApiVersionsResponseData.ApiVersion version1 = new ApiVersionsResponseData.ApiVersion();
            version1.setApiKey(version.apiKey());
            version1.setMinVersion(version.minVersion());
            version1.setMaxVersion(version.maxVersion());
            collection.add(version1);
        }
        apiVersionsResponseData.setApiKeys(collection);
        DecodedResponseFrame<ApiMessage> downgraded = new DecodedResponseFrame<>((short) 0, decodedFrame.correlationId(), header, apiVersionsResponseData,
                responseFrame.requestResponseState());
        ctx.write(downgraded, promise);
    }
}
