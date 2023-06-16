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
public class DecodedRequestFrame
        extends DecodedFrame<RequestHeaderData>
        implements RequestFrame {

    private final boolean decodeResponse;

    public DecodedRequestFrame(short apiVersion,
                               int correlationId,
                               boolean decodeResponse,
                               RequestHeaderData header,
                               ApiMessage body) {
        super(apiVersion, correlationId, header, RequestHeaderData.class, body);
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
