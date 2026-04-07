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

import io.kroxylicious.test.support.TestSupportLoggingKeys;

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
            log().atTrace()
                    .addKeyValue(TestSupportLoggingKeys.API_ID, apiId)
                    .addKeyValue(TestSupportLoggingKeys.API_KEY, apiKey)
                    .addKeyValue(TestSupportLoggingKeys.CTX, ctx)
                    .log("Read");
        }
        short apiVersion = in.readShort();
        if (log().isTraceEnabled()) { // avoid boxing
            log().atTrace()
                    .addKeyValue(TestSupportLoggingKeys.CTX, ctx)
                    .addKeyValue(TestSupportLoggingKeys.API_VERSION, apiVersion)
                    .log("Read");
        }
        int correlationId = in.readInt();
        LOGGER.atDebug()
                .addKeyValue(TestSupportLoggingKeys.CTX, ctx)
                .addKeyValue(TestSupportLoggingKeys.API_KEY, apiKey)
                .addKeyValue(TestSupportLoggingKeys.CORRELATION_ID, correlationId)
                .log("downstream", ctx, apiKey, correlationId);

        RequestHeaderData header;
        var decodeRequest = true;
        LOGGER.atDebug()
                .addKeyValue(TestSupportLoggingKeys.API_KEY, apiKey)
                .addKeyValue(TestSupportLoggingKeys.API_VERSION, apiVersion)
                .addKeyValue(TestSupportLoggingKeys.DECODE_REQUEST, decodeRequest)
                .log("Decode request?", apiKey, apiVersion, decodeRequest);
        boolean decodeResponse = true;
        LOGGER.atDebug()
                .addKeyValue(TestSupportLoggingKeys.API_KEY, apiKey)
                .addKeyValue(TestSupportLoggingKeys.API_VERSION, apiVersion)
                .addKeyValue(TestSupportLoggingKeys.DECODE_RESPONSE, decodeResponse)
                .log("Decode response? {}", apiKey, apiVersion, decodeResponse);
        short headerVersion = apiKey.requestHeaderVersion(apiVersion);
        if (log().isTraceEnabled()) { // avoid boxing
            log().atTrace()
                    .addKeyValue(TestSupportLoggingKeys.CTX, ctx)
                    .addKeyValue(TestSupportLoggingKeys.HEADER_VERSION, headerVersion)
                    .log("headerVersion");
        }
        in.readerIndex(sof);
        ByteBufAccessorImpl acc = new ByteBufAccessorImpl(in);
        header = readHeader(headerVersion, acc);
        if (log().isTraceEnabled()) {
            log().atTrace()
                    .addKeyValue(TestSupportLoggingKeys.CTX, ctx)
                    .addKeyValue(TestSupportLoggingKeys.HEADER, header)
                    .log("Read", ctx, header);
        }
        final Frame frame;
        ApiMessage body = BodyDecoder.decodeRequest(apiKey, apiVersion, acc);
        if (log().isTraceEnabled()) {
            log().atTrace()
                    .addKeyValue(TestSupportLoggingKeys.CTX, ctx)
                    .addKeyValue(TestSupportLoggingKeys.BODY, body)
                    .log("body");
        }

        frame = new DecodedRequestFrame<>(apiVersion, correlationId, header, body, apiVersion);
        if (log().isTraceEnabled()) {
            log().atTrace()
                    .addKeyValue(TestSupportLoggingKeys.CTX, ctx)
                    .addKeyValue(TestSupportLoggingKeys.FRAME, frame)
                    .log("frame");
        }

        return frame;
    }

    private RequestHeaderData readHeader(short headerVersion, Readable accessor) {
        return new RequestHeaderData(accessor, headerVersion);
    }

}
