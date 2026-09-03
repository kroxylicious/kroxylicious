/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import io.kroxylicious.kafka.common.message.ResponseHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;

/**
 * A decoded response frame answering a request that was sent out-of-band by a {@link Filter} or a
 * router, rather than originating from the downstream client. Its recipient identity and the
 * promise to complete are carried by this frame's {@link #routing()} (see
 * {@link io.kroxylicious.proxy.frame.PathElement#pendingPromise()}).
 * @param <B> the type of the response body
 */
public class InternalResponseFrame<B extends ApiMessage> extends DecodedResponseFrame<B> {

    /**
     * Creates an internal response frame. The recipient identity and promise to complete are
     * carried by this frame's {@link #routing()}, which callers must set (via {@link #setRouting})
     * immediately after construction.
     * @param apiVersion the API version of the response
     * @param correlationId the correlation id of the response
     * @param header the response header
     * @param body the response body
     */
    public InternalResponseFrame(short apiVersion, int correlationId, ResponseHeaderData header, B body) {
        super(apiVersion, correlationId, header, body);
    }

    /**
     * Returns the promise to be completed with the response body.
     * @return the promise
     */
    public CompletableFuture<?> promise() {
        return Objects.requireNonNull(routing(), "InternalResponseFrame has no routing set")
                .pendingPromise()
                .orElseThrow(() -> new IllegalStateException("InternalResponseFrame's routing does not carry a promise: " + routing()));
    }

    @Override
    public String toString() {
        return "InternalResponseFrame(" +
                "routing=" + routing() +
                ", apiVersion=" + apiVersion +
                ", correlationId=" + correlationId +
                ", header=" + header +
                ", body=" + body +
                ')';
    }
}
