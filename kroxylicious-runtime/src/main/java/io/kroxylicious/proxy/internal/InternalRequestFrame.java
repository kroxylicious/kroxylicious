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
 * frame's {@link #path()} (see {@link io.kroxylicious.proxy.frame.PathElement#pendingPromise()}).
 * @param <B> the type of the request body
 */
public class InternalRequestFrame<B extends ApiMessage> extends DecodedRequestFrame<B> {

    /**
     * Creates an internal request frame. The recipient identity and promise to complete are
     * carried by this frame's {@link #path()}, which callers must set (via {@link #setPath}
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
        return Objects.requireNonNull(path(), "InternalRequestFrame has no path set")
                .pendingPromise()
                .orElseThrow(() -> new IllegalStateException("InternalRequestFrame's path does not carry a promise: " + path()));
    }

    @Override
    protected DecodedResponseFrame<? extends ApiMessage> createResponseFrame(ResponseHeaderData header, ApiMessage message) {
        var response = new InternalResponseFrame<>(apiVersion, correlationId, header, message);
        response.setPath(this.path());
        return response;
    }
}
