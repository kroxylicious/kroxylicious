/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;

public class InternalRequestFrame<B extends ApiMessage> extends DecodedRequestFrame<B> {

    private final CompletableFuture<?> promise;
    private final Filter recipient;

    public InternalRequestFrame(
            short apiVersion,
            int correlationId,
            boolean decodeResponse,
            Filter recipient,
            CompletableFuture<?> promise,
            RequestHeaderData header,
            B body
    ) {
        super(apiVersion, correlationId, decodeResponse, header, body);
        this.promise = promise;
        this.recipient = Objects.requireNonNull(recipient);
    }

    public Filter recipient() {
        return recipient;
    }

    public CompletableFuture<?> promise() {
        return promise;
    }
}
