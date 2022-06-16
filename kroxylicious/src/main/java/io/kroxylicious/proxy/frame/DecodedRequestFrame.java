/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.frame;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.future.ProxyPromise;

/**
 * A decoded request frame.
 */
public class DecodedRequestFrame<B extends ApiMessage>
        extends DecodedFrame<RequestHeaderData, B>
        implements RequestFrame {

    private final ProxyPromise<? extends Object> promise;
    boolean decodeResponse;
    private final Object recipient;

    private DecodedRequestFrame(short apiVersion,
                                int correlationId,
                                boolean decodeResponse,
                                Object recipient,
                                ProxyPromise<? extends Object> promise,
                                RequestHeaderData header,
                                B body) {
        super(apiVersion, correlationId, header, body);
        if (recipient != null && !decodeResponse) {
            throw new IllegalArgumentException();
        }
        this.decodeResponse = decodeResponse;
        this.recipient = recipient;
        this.promise = promise;
    }

    public static <B extends ApiMessage> DecodedRequestFrame<B> internalRequest(short apiVersion,
                                                                                int correlationId,
                                                                                boolean decodeResponse,
                                                                                Object recipient,
                                                                                ProxyPromise<? extends Object> promise,
                                                                                RequestHeaderData header,
                                                                                B body) {
        return new DecodedRequestFrame<B>(apiVersion, correlationId, decodeResponse, recipient, promise, header, body);
    }

    public static <B extends ApiMessage> DecodedRequestFrame<B> clientRequest(short apiVersion,
                                                                              int correlationId,
                                                                              boolean decodeResponse,
                                                                              RequestHeaderData header,
                                                                              B body) {
        return new DecodedRequestFrame<B>(apiVersion, correlationId, decodeResponse, null, null, header, body);
    }

    @Override
    public short headerVersion() {
        return apiKey().messageType.requestHeaderVersion(apiVersion);
    }

    @Override
    public boolean decodeResponse() {
        return decodeResponse;
    }

    public Object recipient() {
        return recipient;
    }

    public ProxyPromise<? extends Object> promise() {
        return promise;
    }
}
