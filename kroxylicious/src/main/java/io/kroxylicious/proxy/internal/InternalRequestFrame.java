/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.Objects;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.KrpcFilter;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.future.BrutalFuture;

public class InternalRequestFrame<B extends ApiMessage> extends DecodedRequestFrame<B> {

    private final BrutalFuture<?> promise;
    private final KrpcFilter recipient;

    public InternalRequestFrame(short apiVersion,
                                int correlationId,
                                boolean decodeResponse,
                                KrpcFilter recipient,
                                BrutalFuture<?> promise,
                                RequestHeaderData header,
                                B body) {
        super(apiVersion, correlationId, decodeResponse, header, body);
        this.promise = promise;
        this.recipient = Objects.requireNonNull(recipient);
    }

    public KrpcFilter recipient() {
        return recipient;
    }

    public BrutalFuture<?> promise() {
        return promise;
    }
}
