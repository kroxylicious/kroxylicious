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
 * A decoded request frame sent out-of-band by a {@link Filter} or a router, rather than
 * originating from the downstream client. The corresponding response is delivered to the
 * recipient as an {@link InternalResponseFrame} and completes the promise carried on this
 * frame's {@link #routing()} (see {@link io.kroxylicious.proxy.frame.PathElement#pendingPromise()}).
 * @param <B> the type of the request body
 */
public class InternalRequestFrame<B extends ApiMessage> extends DecodedRequestFrame<B> {

    /**
     * Creates an internal request frame. The recipient identity and promise to complete are
     * carried by this frame's {@link #routing()}, which callers must set (via {@link #setRouting}
     * immediately after construction, before this frame is fired into the pipeline.
     * @param apiVersion the API version of the request
     * @param correlationId the correlation id of the request
     * @param decodeResponse whether the corresponding response should be decoded
     * @param header the request header
     * @param body the request body
     */
    public InternalRequestFrame(short apiVersion,
                                int correlationId,
                                boolean decodeResponse,
                                RequestHeaderData header,
                                B body) {
        super(apiVersion, correlationId, decodeResponse, header, body);
    }

    /**
     * Returns the promise to be completed with the response body.
     * @return the promise
     */
    public CompletableFuture<?> promise() {
        return Objects.requireNonNull(routing(), "InternalRequestFrame has no routing set")
                .pendingPromise()
                .orElseThrow(() -> new IllegalStateException("InternalRequestFrame's routing does not carry a promise: " + routing()));
    }

    @Override
    protected DecodedResponseFrame<? extends ApiMessage> createResponseFrame(ResponseHeaderData header, ApiMessage message) {
        var response = new InternalResponseFrame<>(apiVersion, correlationId, header, message);
        response.setRouting(this.routing());
        return response;
    }
}
