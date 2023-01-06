/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter;

import org.apache.kafka.common.protocol.ApiMessage;

import io.netty.buffer.ByteBuf;

import io.kroxylicious.proxy.future.Future;

/**
 * A context to allow filters to interact with other filters and the pipeline.
 */
public interface KrpcFilterContext {
    /**
     * @return A description of this channel (typically used for logging).
     */
    String channelDescriptor();

    /**
     * Allocate a ByteBuffer of the given capacity.
     * The buffer will be deallocated when the request processing is completed
     * @param initialCapacity The initial capacity of the buffer.
     * @return The allocated buffer
     */
    ByteBuf allocate(int initialCapacity);

    String sniHostname();

    /**
     * Send a request towards the broker, invoking upstream filters.
     * @param request The request to forward to the broker.
     */
    void forwardRequest(ApiMessage request);

    /**
     * Send a message from a filter towards the broker, invoking upstream filters
     * and being informed of the response via TODO.
     * The response will pass through upstream filters prior to the handler being invoked.
     * Response propagation will stop once the handler has completed,
     * i.e. the downstream filters will not receive the response.
     *
     * @param apiVersion The version of the request to use
     * @param request The request to send.
     * @param <T> The type of the response
     */
    <T extends ApiMessage> Future<T> sendRequest(short apiVersion, ApiMessage request);

    /**
     * Send a response towards the client, invoking downstream filters.
     * @param response The response to forward to the client.
     */
    void forwardResponse(ApiMessage response);

    // TODO an API to allow a filter to add/remove another filter from the pipeline
}
