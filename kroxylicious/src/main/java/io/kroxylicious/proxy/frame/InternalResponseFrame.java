/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.frame;

import java.util.Objects;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.future.ProxyPromise;

public class InternalResponseFrame<B extends ApiMessage> extends DecodedResponseFrame<B> {

    private final Object recipient;

    private final ProxyPromise<? extends Object> promise;

    public InternalResponseFrame(Object recipient, ProxyPromise<?> promise, short apiVersion, int correlationId, ResponseHeaderData header, B body) {
        super(apiVersion, correlationId, header, body);
        this.recipient = Objects.requireNonNull(recipient);
        this.promise = Objects.requireNonNull(promise);
    }

    public boolean isRecipient(Object candidate) {
        return recipient != null && recipient.equals(candidate);
    }

    public Object recipient() {
        return recipient;
    }

    @SuppressWarnings("unchecked")
    public <T extends ApiMessage> ProxyPromise<T> promise() {
        return (ProxyPromise<T>) promise;
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
