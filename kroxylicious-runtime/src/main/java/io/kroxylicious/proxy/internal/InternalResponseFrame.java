/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.frame.ApiMessageBasedResponseFrame;

public class InternalResponseFrame<B extends ApiMessage> extends InternalFrame<ResponseHeaderData, B> implements ApiMessageBasedResponseFrame<B> {

    public InternalResponseFrame(Filter recipient, short apiVersion, int correlationId, ResponseHeaderData header, B body, CompletableFuture<?> future) {
        super(apiVersion, correlationId, header, body, future, recipient);
    }

    public boolean isRecipient(Filter candidate) {
        return recipient() != null && recipient.equals(candidate);
    }

}
