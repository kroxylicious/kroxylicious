/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.ResponseHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;

/**
 * A decoded request frame sent out-of-band by a {@link Filter}, rather than originating from
 * the downstream client. The corresponding response is delivered to the recipient filter as an
 * {@link InternalResponseFrame} and completes the supplied promise.
 * @param <B> the type of the request body
 */
public class InternalRequestFrame<B extends ApiMessage> extends DecodedRequestFrame<B> {

    private final CompletableFuture<?> promise;
    private final Filter recipient;

    /**
     * Creates an internal request frame.
     * @param apiVersion the API version of the request
     * @param correlationId the correlation id of the request
     * @param decodeResponse whether the corresponding response should be decoded
     * @param recipient the filter that sent the request and should receive its response
     * @param promise the promise to be completed with the response body
     * @param header the request header
     * @param body the request body
     */
    public InternalRequestFrame(short apiVersion,
                                int correlationId,
                                boolean decodeResponse,
                                Filter recipient,
                                CompletableFuture<?> promise,
                                RequestHeaderData header,
                                B body) {
        super(apiVersion, correlationId, decodeResponse, header, body);
        this.promise = promise;
        this.recipient = Objects.requireNonNull(recipient);
    }

    /**
     * Returns the filter that sent this request and should receive its response.
     * @return the recipient filter
     */
    public Filter recipient() {
        return recipient;
    }

    /**
     * Returns the promise to be completed with the response body.
     * @return the promise
     */
    public CompletableFuture<?> promise() {
        return promise;
    }

    @Override
    protected DecodedResponseFrame<? extends ApiMessage> createResponseFrame(ResponseHeaderData header, ApiMessage message) {
        var response = new InternalResponseFrame<>(recipient, apiVersion, correlationId, header, message, promise);
        response.setRouteName(this.routeName());
        return response;
    }
}
