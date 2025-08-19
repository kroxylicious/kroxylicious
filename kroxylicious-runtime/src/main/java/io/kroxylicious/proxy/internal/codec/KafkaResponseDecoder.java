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

import edu.umd.cs.findbugs.annotations.Nullable;

public class KafkaResponseDecoder extends KafkaMessageDecoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaResponseDecoder.class);
    public static final short EMERGENCY_API_VERSION = (short) 0;

    private final CorrelationManager correlationManager;

    public KafkaResponseDecoder(CorrelationManager correlationManager,
                                int socketRequestMaxSizeBytes,
                                @Nullable KafkaMessageListener listener) {
        super(socketRequestMaxSizeBytes, listener);
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
            ApiMessageVersion body = decodeBody(apiKey, apiVersion, accessor);
            log().trace("{}: Body: {}", ctx, body);
            Filter recipient = correlation.recipient();
            if (recipient == null) {
                frame = new DecodedResponseFrame<>(body.apiVersion(), correlationId, header, body.apiMessage());
            }
            else {
                frame = new InternalResponseFrame<>(recipient, body.apiVersion(), correlationId, header, body.apiMessage(), correlation.promise(),
                        correlation.isCacheable());
            }
        }
        else {
            frame = opaqueFrame(correlation.apiKey(), correlation.apiVersion(), in, correlationId, length);
        }
        log().trace("{}: Frame: {}", ctx, frame);
        return frame;
    }

    private static ApiMessageVersion decodeBody(ApiKeys apiKey, short apiVersion, ByteBufAccessorImpl accessor) {
        int prev = accessor.readerIndex();
        try {
            return new ApiMessageVersion(BodyDecoder.decodeResponse(apiKey, apiVersion, accessor), apiVersion);
        }
        catch (RuntimeException e) {
            // KIP-511 when the client receives an unsupported version for the ApiVersionResponse, it fails back to version 0
            // Use the same algorithm as
            // https://github.com/apache/kafka/blob/a41c10fd49841381b5207c184a385622094ed440/clients/src/main/java/org/apache/kafka/common/requests/ApiVersionsResponse.java#L90-L106
            accessor.readerIndex(prev);
            if (ApiKeys.API_VERSIONS.equals(apiKey) && apiVersion != 0) {
                return new ApiMessageVersion(BodyDecoder.decodeResponse(apiKey, EMERGENCY_API_VERSION, accessor), EMERGENCY_API_VERSION);
            }
            else {
                throw e;
            }
        }
    }

    private OpaqueFrame opaqueFrame(short apiKeyId, short apiVersion, ByteBuf in, int correlationId, int length) {
        return new OpaqueResponseFrame(apiKeyId, apiVersion, in.readSlice(length).retain(), correlationId, length);
    }

    private ResponseHeaderData readHeader(short headerVersion, Readable accessor) {
        return new ResponseHeaderData(accessor, headerVersion);
    }

    record ApiMessageVersion(ApiMessage apiMessage, short apiVersion) {}
}
