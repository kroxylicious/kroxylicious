/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.test.codec;

import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.ProduceRequest;

import io.kroxylicious.test.client.SequencedResponse;

/**
 * A decoded request frame.
 * @param <B> type of api message in decoded frame
 */
public class DecodedRequestFrame<B extends ApiMessage>
        extends DecodedFrame<RequestHeaderData, B>
        implements RequestFrame {

    private final CompletableFuture<SequencedResponse> responseFuture = new CompletableFuture<>();

    /**
     * Create a decoded request frame
     * @param apiVersion apiVersion
     * @param correlationId correlationId
     * @param header header
     * @param body body
     */
    public DecodedRequestFrame(short apiVersion,
                               int correlationId,
                               RequestHeaderData header,
                               B body) {
        super(apiVersion, correlationId, header, body);
    }

    @Override
    public short headerVersion() {
        return apiKey().messageType.requestHeaderVersion(apiVersion);
    }

    public CompletableFuture<SequencedResponse> getResponseFuture() {
        return responseFuture;
    }

    /**
     * Whether the Kafka Client expects a response to this request
     * @return Whether the Kafka Client expects a response to this request
     */
    @Override
    public boolean hasResponse() {
        return !(body instanceof ProduceRequest pr && pr.acks() == 0);
    }
}
