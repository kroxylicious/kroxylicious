/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.frame;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

/**
 * A decoded response frame.
 */
public class DecodedResponseFrame<B extends ApiMessage>
        extends DecodedFrame<ResponseHeaderData, B>
        implements ResponseFrame {

    private final RequestResponseState requestResponseState;

    public DecodedResponseFrame(short apiVersion, int correlationId, ResponseHeaderData header, B body, RequestResponseState requestResponseState) {
        super(apiVersion, correlationId, header, body);
        this.requestResponseState = requestResponseState;
    }

    public short headerVersion() {
        return apiKey().messageType.responseHeaderVersion(apiVersion);
    }

    @Override
    public RequestResponseState requestResponseState() {
        return requestResponseState;
    }
}
