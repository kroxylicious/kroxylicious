/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.codec;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.frame.RequestResponseState;
import io.kroxylicious.proxy.tag.VisibleForTesting;

/**
 * Manages correlation ids for a single connection (across the proxy) between a single client
 * and a single broker.
 */
public class CorrelationManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(CorrelationManager.class);

    // TODO use a specialized map
    @VisibleForTesting
    final Map<Integer, Correlation> brokerRequests = new HashMap<>();

    /** The correlation id with the upstream broker */
    private int upstreamId;

    public CorrelationManager() {
        this(0);
    }

    public CorrelationManager(int initialCorrelationId) {
        upstreamId = initialCorrelationId;
    }

    /**
     * Allocate and return a correlation id for an outgoing request to the broker.
     *
     * @param apiKey                  The API key.
     * @param apiVersion              The API version.
     * @param downstreamCorrelationId The downstream client's correlation id.
     * @param hasResponse             Whether a response is expected.
     * @param promise
     * @param decodeResponse          Whether the response should be decoded.
     */
    public int putBrokerRequest(short apiKey,
                                short apiVersion,
                                int downstreamCorrelationId,
                                boolean hasResponse,
                                Filter recipient,
                                CompletableFuture<?> promise,
                                boolean decodeResponse,
                                RequestResponseState state) {
        // need to allocate an id and put in a map for quick lookup, along with the "tag"
        int upstreamCorrelationId = upstreamId++;
        LOGGER.trace("Allocated upstream id {} for downstream id {}", upstreamCorrelationId, downstreamCorrelationId);
        if (hasResponse) {
            Correlation existing = this.brokerRequests.put(upstreamCorrelationId,
                    new Correlation(apiKey, apiVersion, downstreamCorrelationId, decodeResponse, recipient, promise, state));
            if (existing != null) {
                LOGGER.error("Duplicate upstream correlation id {}", upstreamCorrelationId);
            }
        }
        return upstreamCorrelationId;
    }

    /**
     * Find (and remove) the Correlation for an incoming response from the broker
     * @param upstreamCorrelationId The (upstream) correlation id in the response.
     */
    public Correlation getBrokerCorrelation(int upstreamCorrelationId) {
        // Set the tag on the response object
        return brokerRequests.remove(upstreamCorrelationId);
    }

    /**
     * A record for which responses should be decoded, together with their
     * API key and version.
     */
    public record Correlation(short apiKey,
                              short apiVersion,
                              int downstreamCorrelationId,
                              boolean decodeResponse,
                              Filter recipient,
                              CompletableFuture<?> promise,
                              RequestResponseState state) {};
}
