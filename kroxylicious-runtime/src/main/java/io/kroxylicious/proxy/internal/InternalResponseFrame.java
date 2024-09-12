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

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;

public class InternalResponseFrame<B extends ApiMessage> extends DecodedResponseFrame<B> {

    private final Filter recipient;

    private final CompletableFuture<?> future;

    public InternalResponseFrame(Filter recipient, short apiVersion, int correlationId, ResponseHeaderData header, B body, CompletableFuture<?> future) {
        super(apiVersion, correlationId, header, body);
        this.recipient = Objects.requireNonNull(recipient);
        this.future = future;
    }

    public boolean isRecipient(Filter candidate) {
        return recipient != null && recipient.equals(candidate);
    }

    public Filter recipient() {
        return recipient;
    }

    public CompletableFuture<?> promise() {
        return future;
    }

    @Override
    public String toString() {
        return "InternalResponseFrame("
               +
               "recipient="
               + recipient
               +
               ", promise="
               + future
               +
               ", apiVersion="
               + apiVersion
               +
               ", correlationId="
               + correlationId
               +
               ", header="
               + header
               +
               ", body="
               + body
               +
               ')';
    }
}
