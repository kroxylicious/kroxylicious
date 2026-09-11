/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.frame;

import io.kroxylicious.kafka.common.message.ProduceRequestData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.ResponseHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;

import static io.kroxylicious.kafka.common.protocol.ApiKeys.PRODUCE;

/**
 * A decoded request frame.
 *
 * @param <B> the type of the request body
 */
public class DecodedRequestFrame<B extends ApiMessage>
        extends DecodedFrame<RequestHeaderData, B>
        implements RequestFrame {

    private final boolean decodeResponse;

    /**
     * Creates a decoded request frame.
     *
     * @param apiVersion the API version of the request
     * @param correlationId the correlation id of the request
     * @param decodeResponse whether the corresponding response should be decoded
     * @param header the request header
     * @param body the request body
     */
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

    /**
     * Creates a response frame corresponding to this request.
     *
     * @param header the response header
     * @param message the response body, which must have the same API key as this request
     * @return the response frame
     */
    // we don't know the response type
    @SuppressWarnings("java:S1452")
    public DecodedResponseFrame<? extends ApiMessage> responseFrame(ResponseHeaderData header, ApiMessage message) {
        if (message.apiKey() != apiKeyId()) {
            throw new AssertionError(
                    "Attempt to create responseFrame with ApiMessage of type " + ApiKeys.forId(message.apiKey()) + " but request is of type "
                            + apiKey());
        }
        return createResponseFrame(header, message);
    }

    /**
     * Creates the response frame instance for this request type.
     *
     * @param header the response header
     * @param message the response body
     * @return the response frame
     */
    // we don't know the response type
    @SuppressWarnings("java:S1452")
    protected DecodedResponseFrame<? extends ApiMessage> createResponseFrame(ResponseHeaderData header, ApiMessage message) {
        return new DecodedResponseFrame<>(apiVersion(), correlationId(), header, message);
    }
}
