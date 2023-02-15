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
import io.kroxylicious.proxy.frame.NettyDecodedRequestFrame;
import io.kroxylicious.proxy.future.Promise;

public class InternalRequestFrame<B extends ApiMessage> extends NettyDecodedRequestFrame<B> {

    private final Promise<? extends Object> promise;
    private final KrpcFilter recipient;

    public InternalRequestFrame(short apiVersion,
                                int correlationId,
                                boolean decodeResponse,
                                KrpcFilter recipient,
                                Promise<? extends Object> promise,
                                RequestHeaderData header,
                                B body) {
        super(apiVersion, correlationId, decodeResponse, header, body);
        this.promise = Objects.requireNonNull(promise);
        this.recipient = Objects.requireNonNull(recipient);
    }

    public KrpcFilter recipient() {
        return recipient;
    }

    public Promise<? extends Object> promise() {
        return promise;
    }
}
