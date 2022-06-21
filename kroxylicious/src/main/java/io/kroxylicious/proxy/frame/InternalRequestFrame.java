/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.frame;

import java.util.Objects;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.future.ProxyPromise;

public class InternalRequestFrame<B extends ApiMessage> extends DecodedRequestFrame<B> {

    private final ProxyPromise<? extends Object> promise;
    private final Object recipient;

    public InternalRequestFrame(short apiVersion,
                                int correlationId,
                                boolean decodeResponse,
                                Object recipient,
                                ProxyPromise<? extends Object> promise,
                                RequestHeaderData header,
                                B body) {
        super(apiVersion, correlationId, decodeResponse, header, body);
        this.promise = Objects.requireNonNull(promise);
        this.recipient = Objects.requireNonNull(recipient);
    }

    public Object recipient() {
        return recipient;
    }

    public ProxyPromise<? extends Object> promise() {
        return promise;
    }
}
