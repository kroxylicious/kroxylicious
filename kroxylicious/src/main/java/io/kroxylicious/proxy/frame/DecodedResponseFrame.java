/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.frame;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.future.ProxyPromise;

/**
 * A decoded response frame.
 */
public class DecodedResponseFrame<B extends ApiMessage>
        extends DecodedFrame<ResponseHeaderData, B>
        implements ResponseFrame {

    private final Object recipient;
    private final ProxyPromise<? extends Object> promise;

    public DecodedResponseFrame(Object recipient, ProxyPromise<? extends Object> promise, short apiVersion, int correlationId, ResponseHeaderData header, B body) {
        super(apiVersion, correlationId, header, body);
        this.recipient = recipient;
        this.promise = promise;
    }

    public short headerVersion() {
        return apiKey().messageType.responseHeaderVersion(apiVersion);
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
}
