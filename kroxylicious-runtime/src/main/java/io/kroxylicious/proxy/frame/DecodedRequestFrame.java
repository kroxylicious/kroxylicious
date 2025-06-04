/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.frame;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import static org.apache.kafka.common.protocol.ApiKeys.PRODUCE;

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

    @Override
    public boolean hasResponse() {
        return !isZeroAcksProduceRequest();
    }

    private boolean isZeroAcksProduceRequest() {
        return apiKeyId() == PRODUCE.id && ((ProduceRequestData) body).acks() == 0;
    }

}
