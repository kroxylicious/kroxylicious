/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.codec;

import org.apache.kafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import io.kroxylicious.proxy.frame.RequestFrame;
import io.kroxylicious.proxy.internal.InternalRequestFrame;

public class KafkaRequestEncoder extends KafkaMessageEncoder<RequestFrame> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaRequestEncoder.class);

    public static final int LENGTH = 4;
    public static final int API_KEY = 2;
    public static final int API_VERSION = 2;
    private final CorrelationManager correlationManager;

    public KafkaRequestEncoder(CorrelationManager correlationManager) {
        this.correlationManager = correlationManager;
    }

    @Override
    protected Logger log() {
        return LOGGER;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, RequestFrame frame, ByteBuf out) throws Exception {
        super.encode(ctx, frame, out);
        var wi = out.writerIndex();
        short apiKey = frame.apiKeyId();
        short apiVersion = frame.apiVersion();
        boolean hasResponse = frame.hasResponse();
        boolean decodeResponse = frame.decodeResponse();
        int downstreamCorrelationId = frame.correlationId();
        int upstreamCorrelationId = correlationManager.putBrokerRequest(apiKey,
                apiVersion,
                downstreamCorrelationId,
                hasResponse,
                frame instanceof InternalRequestFrame ? ((InternalRequestFrame<?>) frame).recipient() : null,
                frame instanceof InternalRequestFrame ? ((InternalRequestFrame<?>) frame).promise() : null, decodeResponse);
        out.writerIndex(LENGTH + API_KEY + API_VERSION);
        out.writeInt(upstreamCorrelationId);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{}: {} downstream correlation id {} assigned upstream correlation id: {}",
                    ctx, ApiKeys.forId(apiKey), downstreamCorrelationId, upstreamCorrelationId);
        }
        out.writerIndex(wi);
    }

}
