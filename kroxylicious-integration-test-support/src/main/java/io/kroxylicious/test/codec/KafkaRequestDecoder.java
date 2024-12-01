/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.test.codec;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Readable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/**
 * Decoder for Kafka Requests
 */
public class KafkaRequestDecoder extends KafkaMessageDecoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaRequestDecoder.class);

    /**
     * Create KafkaRequestDecoder
     */
    public KafkaRequestDecoder() {
        super();
    }

    @Override
    protected Logger log() {
        return LOGGER;
    }

    @Override
    protected Frame decodeHeaderAndBody(ChannelHandlerContext ctx, ByteBuf in, final int length) {
        // Read the api key and version to determine the header api version
        final int sof = in.readerIndex();
        var apiId = in.readShort();
        ApiKeys apiKey = ApiKeys.forId(apiId);
        if (log().isTraceEnabled()) { // avoid boxing
            log().trace("{}: apiKey: {} {}", ctx, apiId, apiKey);
        }
        short apiVersion = in.readShort();
        if (log().isTraceEnabled()) { // avoid boxing
            log().trace("{}: apiVersion: {}", ctx, apiVersion);
        }
        int correlationId = in.readInt();
        LOGGER.debug("{}: {} downstream correlation id: {}", ctx, apiKey, correlationId);

        RequestHeaderData header;
        var decodeRequest = true;
        LOGGER.debug("Decode {}/v{} request? {}", apiKey, apiVersion, decodeRequest);
        boolean decodeResponse = true;
        LOGGER.debug("Decode {}/v{} response? {}", apiKey, apiVersion, decodeResponse);
        short headerVersion = apiKey.requestHeaderVersion(apiVersion);
        if (log().isTraceEnabled()) { // avoid boxing
            log().trace("{}: headerVersion {}", ctx, headerVersion);
        }
        in.readerIndex(sof);
        ByteBufAccessorImpl acc = new ByteBufAccessorImpl(in);
        header = readHeader(headerVersion, acc);
        if (log().isTraceEnabled()) {
            log().trace("{}: header: {}", ctx, header);
        }
        final Frame frame;
        ApiMessage body = BodyDecoder.decodeRequest(apiKey, apiVersion, acc);
        if (log().isTraceEnabled()) {
            log().trace("{}: body {}", ctx, body);
        }

        frame = new DecodedRequestFrame<>(apiVersion, correlationId, header, body);
        if (log().isTraceEnabled()) {
            log().trace("{}: frame {}", ctx, frame);
        }

        return frame;
    }

    private RequestHeaderData readHeader(short headerVersion, Readable accessor) {
        return new RequestHeaderData(accessor, headerVersion);
    }

}
