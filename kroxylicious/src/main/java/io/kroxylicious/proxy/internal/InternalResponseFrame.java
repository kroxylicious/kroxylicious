/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.KrpcFilter;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;

public class InternalResponseFrame extends DecodedResponseFrame {

    private final KrpcFilter recipient;

    private final CompletableFuture<?> future;

    public InternalResponseFrame(KrpcFilter recipient, short apiVersion, int correlationId, ResponseHeaderData header, ApiMessage body, CompletableFuture<?> future) {
        super(apiVersion, correlationId, header, body);
        this.recipient = Objects.requireNonNull(recipient);
        this.future = future;
    }

    public boolean isRecipient(KrpcFilter candidate) {
        return recipient != null && recipient.equals(candidate);
    }

    public KrpcFilter recipient() {
        return recipient;
    }

    @SuppressWarnings("unchecked")
    public <T extends ApiMessage> CompletableFuture<T> promise() {
        return (CompletableFuture<T>) future;
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
