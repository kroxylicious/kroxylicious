/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.Objects;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.KrpcFilter;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.future.Promise;

public class InternalResponseFrame<B extends ApiMessage> extends DecodedResponseFrame<B> {

    private final KrpcFilter recipient;

    private final Promise<? extends Object> promise;

    public InternalResponseFrame(KrpcFilter recipient, Promise<?> promise, short apiVersion, int correlationId, ResponseHeaderData header, B body) {
        super(apiVersion, correlationId, header, body);
        this.recipient = Objects.requireNonNull(recipient);
        this.promise = Objects.requireNonNull(promise);
    }

    public boolean isRecipient(KrpcFilter candidate) {
        return recipient != null && recipient.equals(candidate);
    }

    public KrpcFilter recipient() {
        return recipient;
    }

    @SuppressWarnings("unchecked")
    public <T extends ApiMessage> Promise<T> promise() {
        return (Promise<T>) promise;
    }

    @Override
    public String toString() {
        return "InternalResponseFrame(" +
                "recipient=" + recipient +
                ", promise=" + promise +
                ", apiVersion=" + apiVersion +
                ", correlationId=" + correlationId +
                ", header=" + header +
                ", body=" + body +
                ')';
    }
}
