/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.frame.AbstractApiMessageBasedFrame;

/**
 * A frame that has been created within the proxy itself (as opposed to arriving over the wire).
 *
 * @param <H> The header type
 * @param <B> The body type
 *
 */
public abstract class InternalFrame<H extends ApiMessage, B extends ApiMessage>
        extends AbstractApiMessageBasedFrame<H, B> {

    protected final CompletableFuture<?> promise;
    protected final Filter recipient;

    InternalFrame(short apiVersion, int correlationId, H header, B body, CompletableFuture<?> promise, Filter recipient) {
        super(apiVersion, correlationId, header, body);
        this.promise = promise;
        this.recipient = Objects.requireNonNull(recipient);
    }

    public Filter recipient() {
        return recipient;
    }

    public CompletableFuture<?> promise() {
        return promise;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" +
                "recipient=" + recipient +
                ", promise=" + promise() +
                ", apiVersion=" + apiVersion +
                ", correlationId=" + correlationId +
                ", header=" + header +
                ", body=" + body +
                ')';
    }

}
