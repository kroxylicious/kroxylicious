/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.frame.ApiMessageBasedRequestFrame;

public class InternalRequestFrame<B extends ApiMessage> extends InternalFrame<RequestHeaderData, B> implements ApiMessageBasedRequestFrame<B> {
    private final boolean decodeResponse;

    public InternalRequestFrame(short apiVersion,
                                int correlationId,
                                boolean decodeResponse,
                                Filter recipient,
                                CompletableFuture<?> promise,
                                RequestHeaderData header,
                                B body) {
        super(apiVersion, correlationId, header, body, promise, recipient);
        this.decodeResponse = decodeResponse;
    }

    @Override
    public boolean decodeResponse() {
        return decodeResponse;
    }
}
