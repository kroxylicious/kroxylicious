/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.TimestampType;

/**
 * A context to allow filters to interact with other filters and the pipeline.
 */
public interface KrpcFilterContext {
    /**
     * @return A description of this channel (typically used for logging).
     */
    String channelDescriptor();

    /**
     * Creates a MemoryRecordsBuilder of the given capacity.
     * @param initialCapacity The initial capacity of the backing buffer in bytes.
     * @param compressionType The compression type.
     * @param timestampType The timestamp type.
     * @param baseOffset The bash offset.
     * @return The created MemoryRecordsBuilder
     */
    MemoryRecordsBuilder memoryRecordsBuilder(int initialCapacity,
                                              CompressionType compressionType,
                                              TimestampType timestampType,
                                              long baseOffset);

    /**
     * @return the SNI hostname provided by the client.  Will be null if the client is
     * using a non-TLS connection or the TLS client hello didn't provide one.
     */
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
    <T extends ApiMessage> CompletionStage<T> sendRequest(short apiVersion, ApiMessage request);

    /**
     * Send a response towards the client, invoking downstream filters.
     * @param response The response to forward to the client.
     */
    void forwardResponse(ApiMessage response);

    // TODO an API to allow a filter to add/remove another filter from the pipeline
}
