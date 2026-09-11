/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.integration.codec;

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
            log().atTrace()
                    .addKeyValue("apiId", apiId)
                    .addKeyValue("apiKey", apiKey)
                    .addKeyValue("ctx", ctx)
                    .log("Read");
        }
        short apiVersion = in.readShort();
        if (log().isTraceEnabled()) { // avoid boxing
            log().atTrace()
                    .addKeyValue("ctx", ctx)
                    .addKeyValue("apiVersion", apiVersion)
                    .log("Read");
        }
        int correlationId = in.readInt();
        LOGGER.atDebug()
                .addKeyValue("ctx", ctx)
                .addKeyValue("apiKey", apiKey)
                .addKeyValue("correlationId", correlationId)
                .log("downstream", ctx, apiKey, correlationId);

        RequestHeaderData header;
        var decodeRequest = true;
        LOGGER.atDebug()
                .addKeyValue("apiKey", apiKey)
                .addKeyValue("apiVersion", apiVersion)
                .addKeyValue("decodeRequest", decodeRequest)
                .log("Decode request?", apiKey, apiVersion, decodeRequest);
        boolean decodeResponse = true;
        LOGGER.atDebug()
                .addKeyValue("apiKey", apiKey)
                .addKeyValue("apiVersion", apiVersion)
                .addKeyValue("decodeResponse", decodeResponse)
                .log("Decode response? {}", apiKey, apiVersion, decodeResponse);
        short headerVersion = apiKey.requestHeaderVersion(apiVersion);
        if (log().isTraceEnabled()) { // avoid boxing
            log().atTrace()
                    .addKeyValue("ctx", ctx)
                    .addKeyValue("headerVersion", headerVersion)
                    .log("headerVersion");
        }
        in.readerIndex(sof);
        ByteBufAccessorImpl acc = new ByteBufAccessorImpl(in);
        header = readHeader(headerVersion, acc);
        if (log().isTraceEnabled()) {
            log().atTrace()
                    .addKeyValue("ctx", ctx)
                    .addKeyValue("header", header)
                    .log("Read", ctx, header);
        }
        final Frame frame;
        ApiMessage body = BodyDecoder.decodeRequest(apiKey, apiVersion, acc);
        if (log().isTraceEnabled()) {
            log().atTrace()
                    .addKeyValue("ctx", ctx)
                    .addKeyValue("body", body)
                    .log("body");
        }

        frame = new DecodedRequestFrame<>(apiVersion, correlationId, header, body, apiVersion);
        if (log().isTraceEnabled()) {
            log().atTrace()
                    .addKeyValue("ctx", ctx)
                    .addKeyValue("frame", frame)
                    .log("frame");
        }

        return frame;
    }

    private RequestHeaderData readHeader(short headerVersion, Readable accessor) {
        return new RequestHeaderData(accessor, headerVersion);
    }

}
