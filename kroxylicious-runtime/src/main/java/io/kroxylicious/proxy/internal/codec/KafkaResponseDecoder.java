/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.codec;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Readable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.Frame;
import io.kroxylicious.proxy.frame.OpaqueFrame;
import io.kroxylicious.proxy.frame.OpaqueResponseFrame;
import io.kroxylicious.proxy.internal.InternalResponseFrame;

public class KafkaResponseDecoder extends KafkaMessageDecoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaResponseDecoder.class);

    private final CorrelationManager correlationManager;

    public KafkaResponseDecoder(CorrelationManager correlationManager, int socketRequestMaxSizeBytes) {
        super(socketRequestMaxSizeBytes);
        this.correlationManager = correlationManager;
    }

    @Override
    protected Logger log() {
        return LOGGER;
    }

    @Override
    protected Frame decodeHeaderAndBody(ChannelHandlerContext ctx, ByteBuf in, int length) {
        var wi = in.writerIndex();
        var ri = in.readerIndex();
        var upstreamCorrelationId = in.readInt();
        in.readerIndex(ri);

        CorrelationManager.Correlation correlation = this.correlationManager.getBrokerCorrelation(upstreamCorrelationId);
        if (correlation == null) {
            throw new AssertionError("Missing correlation id " + upstreamCorrelationId);
        }
        else if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{}: Recovered correlation {} for upstream correlation id {}", ctx, correlation, upstreamCorrelationId);
        }
        int correlationId = correlation.downstreamCorrelationId();
        in.writerIndex(ri);
        in.writeInt(correlationId);
        in.writerIndex(wi);

        final Frame frame;
        if (correlation.decodeResponse()) {
            ApiKeys apiKey = ApiKeys.forId(correlation.apiKey());
            short apiVersion = correlation.apiVersion();
            var accessor = new ByteBufAccessorImpl(in);
            short headerVersion = apiKey.responseHeaderVersion(apiVersion);
            log().trace("{}: Header version: {}", ctx, headerVersion);
            ResponseHeaderData header = readHeader(headerVersion, accessor);
            log().trace("{}: Header: {}", ctx, header);
            ApiMessage body = BodyDecoder.decodeResponse(apiKey, apiVersion, accessor);
            log().trace("{}: Body: {}", ctx, body);
            Filter recipient = correlation.recipient();
            if (recipient == null) {
                frame = new DecodedResponseFrame<>(apiVersion, correlationId, header, body);
            }
            else {
                frame = new InternalResponseFrame<>(recipient, apiVersion, correlationId, header, body, correlation.promise());
            }
        }
        else {
            frame = opaqueFrame(correlation.apiKey(), correlation.apiVersion(), in, correlationId, length);
        }
        log().trace("{}: Frame: {}", ctx, frame);
        return frame;
    }

    private OpaqueFrame opaqueFrame(short apiKeyId, short apiVersion, ByteBuf in, int correlationId, int length) {
        return new OpaqueResponseFrame(apiKeyId, apiVersion, in.readSlice(length).retain(), correlationId, length);
    }

    private ResponseHeaderData readHeader(short headerVersion, Readable accessor) {
        return new ResponseHeaderData(accessor, headerVersion);
    }

}