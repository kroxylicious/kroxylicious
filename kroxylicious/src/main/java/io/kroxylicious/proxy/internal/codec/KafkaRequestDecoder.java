/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.codec;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Readable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.Frame;
import io.kroxylicious.proxy.frame.OpaqueRequestFrame;
import io.kroxylicious.proxy.frame.RequestFrame;
import io.kroxylicious.proxy.internal.util.Metrics;

public class KafkaRequestDecoder extends KafkaMessageDecoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaRequestDecoder.class);

    private final DecodePredicate decodePredicate;

    public KafkaRequestDecoder(DecodePredicate decodePredicate) {
        super();
        this.decodePredicate = decodePredicate;
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
        // TODO handle unknown api key
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

        RequestHeaderData header = null;
        final ByteBufAccessorImpl accessor;
        Metrics.inboundDownstreamMessagesCounter().increment();
        var decodeRequest = decodePredicate.shouldDecodeRequest(apiKey, apiVersion);
        LOGGER.debug("Decode {}/v{} request? {}, Predicate {} ", apiKey, apiVersion, decodeRequest, decodePredicate);
        boolean decodeResponse = decodePredicate.shouldDecodeResponse(apiKey, apiVersion);
        LOGGER.debug("Decode {}/v{} response? {}, Predicate {}", apiKey, apiVersion, decodeResponse, decodePredicate);
        short headerVersion = apiKey.requestHeaderVersion(apiVersion);
        if (decodeRequest) {
            Metrics.inboundDownstreamDecodedMessagesCounter().increment();
            Metrics.payloadSizeBytesUpstreamSummary(apiKey, apiVersion).record(length);
            if (log().isTraceEnabled()) { // avoid boxing
                log().trace("{}: headerVersion {}", ctx, headerVersion);
            }
            in.readerIndex(sof);

            // TODO Decide whether to decode this API at all
            // TODO Can we implement ApiMessage using an opaque wrapper around a bytebuf?

            accessor = new ByteBufAccessorImpl(in);
            header = readHeader(headerVersion, accessor);
            if (log().isTraceEnabled()) {
                log().trace("{}: header: {}", ctx, header);
            }
        }
        else {
            accessor = null;
        }
        final RequestFrame frame;
        if (decodeRequest) {
            ApiMessage body = BodyDecoder.decodeRequest(apiKey, apiVersion, accessor);
            if (log().isTraceEnabled()) {
                log().trace("{}: body {}", ctx, body);
            }

            frame = new DecodedRequestFrame<ApiMessage>(apiVersion, correlationId, decodeResponse, header, body);
            if (log().isTraceEnabled()) {
                log().trace("{}: frame {}", ctx, frame);
            }
        }
        else {
            in.readerIndex(sof);
            frame = opaqueFrame(in, correlationId, decodeResponse, length);
            in.readerIndex(sof + length);
        }

        return frame;
    }

    private OpaqueRequestFrame opaqueFrame(ByteBuf in,
                                           int correlationId,
                                           boolean decodeResponse,
                                           int length) {
        return new OpaqueRequestFrame(
                in.readSlice(length).retain(),
                correlationId,
                decodeResponse,
                length);
    }

    private RequestHeaderData readHeader(short headerVersion, Readable accessor) {
        return new RequestHeaderData(accessor, headerVersion);
    }

}
