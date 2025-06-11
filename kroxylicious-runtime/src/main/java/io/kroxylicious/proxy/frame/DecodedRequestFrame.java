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
public final class DecodedRequestFrame<B extends ApiMessage>
        extends DecodedFrame<RequestHeaderData, B> implements ApiMessageBasedRequestFrame<B> {

    private final boolean decodeResponse;

    public DecodedRequestFrame(short apiVersion,
                               int correlationId,
                               boolean decodeResponse,
                               RequestHeaderData header,
                               B body,
                               int originalEncodedSize) {
        super(apiVersion, correlationId, header, body, originalEncodedSize);
        this.decodeResponse = decodeResponse;
    }

    @Override
    public boolean decodeResponse() {
        return decodeResponse;
    }

}
