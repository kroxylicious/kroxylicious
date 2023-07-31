/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

/**
 * A context to allow filters to interact with other filters and the pipeline.
 */
public interface KrpcFilterContext {
    /**
     * A description of this channel.
     * @return A description of this channel (typically used for logging).
     */
    String channelDescriptor();

    /**
     * Create a ByteBufferOutputStream of the given capacity.
     * The backing buffer will be deallocated when the request processing is completed
     * @param initialCapacity The initial capacity of the buffer.
     * @return The allocated ByteBufferOutputStream
     */
    ByteBufferOutputStream createByteBufferOutputStream(int initialCapacity);

    /**
     * The SNI hostname provided by the client, if any.
     * @return the SNI hostname provided by the client.  Will be null if the client is
     * using a non-TLS connection or the TLS client hello didn't provide one.
     */
    String sniHostname();

    // /**
    // * Send a request towards the broker, invoking upstream filters.
    // *
    // * @param header The header to forward to the broker.
    // * @param request The request to forward to the broker.
    // */
    // void forwardRequest(RequestHeaderData header, ApiMessage request);

    RequestFilterResultBuilder requestFilterResultBuilder();

    /**
     * Send a message from a filter towards the broker, invoking upstream filters
     * and being informed of the response via TODO.
     * The response will pass through upstream filters, invoking them, prior to the handler being invoked.
     * Response propagation will stop once the handler has completed,
     * i.e. the downstream filters will not receive the response.
     *
     * @param apiVersion The version of the request to use
     * @param request The request to send.
     * @param <T> The type of the response
     * @return CompletionStage providing the response.
     */
    <T extends ApiMessage> CompletionStage<T> sendRequest(short apiVersion, ApiMessage request);

    // /**
    // * Send a response towards the client, invoking downstream filters.
    // * <p>If this is invoked while the message is flowing downstream towards the broker, then
    // * it will not be sent to the broker. So this method can be used to generate responses in
    // * the proxy.</p>
    // * @param header The header to forward to the client.
    // * @param response The response to forward to the client.
    // * @throws AssertionError if response is logically inconsistent, for example responding with request data
    // * or responding with a produce response to a fetch request. It is up to specific implementations to
    // * determine what logically inconsistent means.
    // */
    // void forwardResponse(ResponseHeaderData header, ApiMessage response);

    // /**
    // * Send a response towards the client, invoking downstream filters.
    // * <p>If this is invoked while the message is flowing downstream towards the broker, then
    // * it will not be sent to the broker. So this method can be used to generate responses in
    // * the proxy. In this case response headers will be created with a correlationId matching the request</p>
    // * @param response The response to forward to the client.
    // * @throws AssertionError if response is logically inconsistent, for example responding with request data
    // * or responding with a produce response to a fetch request. It is up to specific implementations to
    // * determine what logically inconsistent means.
    // */
    // void forwardResponse(ApiMessage response);

    ResponseFilterResultBuilder responseFilterResultBuilder();

    // /**
    // * Allows a filter to decide to close the connection. The client will be disconnected. The client is free
    // * to retry the connection. The client will receive no indication why the connection was closed.
    // */
    // void closeConnection();

    // TODO an API to allow a filter to add/remove another filter from the pipeline
}
