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
 * A decoded response frame answering a request that was sent out-of-band by a {@link Filter},
 * rather than originating from the downstream client. It is delivered to the recipient filter
 * only, and its promise is completed with the response body.
 * @param <B> the type of the response body
 */
public class InternalResponseFrame<B extends ApiMessage> extends DecodedResponseFrame<B> {

    private final Filter recipient;

    private final CompletableFuture<?> future;

    /**
     * Creates an internal response frame.
     * @param recipient the filter that sent the out-of-band request and should receive this response
     * @param apiVersion the API version of the response
     * @param correlationId the correlation id of the response
     * @param header the response header
     * @param body the response body
     * @param future the promise to be completed with the response body
     */
    public InternalResponseFrame(Filter recipient, short apiVersion, int correlationId, ResponseHeaderData header, B body, CompletableFuture<?> future) {
        super(apiVersion, correlationId, header, body);
        this.recipient = Objects.requireNonNull(recipient);
        this.future = future;
    }

    /**
     * Determines whether the given filter is the intended recipient of this response.
     * @param candidate the filter to test
     * @return {@code true} if the candidate is the recipient of this response
     */
    public boolean isRecipient(Filter candidate) {
        return recipient != null && recipient.equals(candidate);
    }

    /**
     * Returns the filter that should receive this response.
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
        return future;
    }

    @Override
    public String toString() {
        return "InternalResponseFrame(" +
                "recipient=" + recipient +
                ", promise=" + future +
                ", apiVersion=" + apiVersion +
                ", correlationId=" + correlationId +
                ", header=" + header +
                ", body=" + body +
                ')';
    }
}
