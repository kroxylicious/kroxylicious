/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.frame;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.internal.FilterType;

/**
 * A decoded request frame.
 */
public class DecodedRequestFrame<B extends ApiMessage>
        extends DecodedFrame<RequestHeaderData, B>
        implements RequestFrame {

    private final boolean decodeResponse;

    public DecodedRequestFrame(short apiVersion,
                               int correlationId,
                               boolean decodeResponse,
                               RequestHeaderData header,
                               B body) {
        super(FilterType.forKey(ApiKeys.forId(body.apiKey()), true), apiVersion, correlationId, header, body);
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
