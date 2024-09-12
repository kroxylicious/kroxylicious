/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.test.codec;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Readable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import io.kroxylicious.test.client.CorrelationManager;
import io.kroxylicious.test.client.SequencedResponse;

/**
 * KafkaResponseDecoder
 */
public class KafkaResponseDecoder extends KafkaMessageDecoder {

    int i = 0;
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaResponseDecoder.class);

    private final CorrelationManager correlationManager;

    /**
     * Creates a response decoder
     * @param correlationManager the manager used to retrieve apiKey and apiVersion for the response
     */
    public KafkaResponseDecoder(CorrelationManager correlationManager) {
        super();
        this.correlationManager = correlationManager;
    }

    @Override
    protected Logger log() {
        return LOGGER;
    }

    @Override
    protected Frame decodeHeaderAndBody(ChannelHandlerContext ctx, ByteBuf in, int length) {
        var ri = in.readerIndex();
        var correlationId = in.readInt();
        in.readerIndex(ri);

        CorrelationManager.Correlation correlation = this.correlationManager.getBrokerCorrelation(correlationId);
        if (correlation == null) {
            throw new AssertionError("Missing correlation id " + correlationId);
        } else if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{}: Recovered correlation {} for upstream correlation id {}", ctx, correlation, correlationId);
        }

        final DecodedResponseFrame<?> frame;
        ApiKeys apiKey = ApiKeys.forId(correlation.apiKey());
        short apiVersion = correlation.apiVersion();
        var accessor = new ByteBufAccessorImpl(in);
        short headerVersion = apiKey.responseHeaderVersion(apiVersion);
        log().trace("{}: Header version: {}", ctx, headerVersion);
        ResponseHeaderData header = readHeader(headerVersion, accessor);
        log().trace("{}: Header: {}", ctx, header);
        ApiMessage body = BodyDecoder.decodeResponse(apiKey, apiVersion, accessor);
        log().trace("{}: Body: {}", ctx, body);
        frame = new DecodedResponseFrame<>(apiVersion, correlationId, header, body);
        correlation.responseFuture().complete(new SequencedResponse(frame, i++));
        return frame;
    }

    private ResponseHeaderData readHeader(short headerVersion, Readable accessor) {
        return new ResponseHeaderData(accessor, headerVersion);
    }

}
