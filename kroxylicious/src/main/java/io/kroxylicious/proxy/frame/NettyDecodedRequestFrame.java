/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.frame;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

/**
 * A decoded request frame.
 */
public class NettyDecodedRequestFrame<B extends ApiMessage>
        extends NettyDecodedFrame<RequestHeaderData, B>
        implements RequestFrame, DecodedRequestFrame<B> {

    private final boolean decodeResponse;

    public NettyDecodedRequestFrame(short apiVersion,
                                    int correlationId,
                                    boolean decodeResponse,
                                    RequestHeaderData header,
                                    B body) {
        super(apiVersion, correlationId, header, body);
        this.decodeResponse = decodeResponse;
    }

    @Override
    public short headerVersion() {
        return apiKey().messageType.requestHeaderVersion(apiVersion);
    }

    @Override
    public boolean decodeResponse() {
        return decodeResponse;
    }

}
