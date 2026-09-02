/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.codec;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.proxy.frame.PathElement;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

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

    /**
     * Creates a correlation manager whose first allocated upstream correlation id is zero.
     */
    public CorrelationManager() {
        this(0);
    }

    /**
     * Creates a correlation manager.
     * @param initialCorrelationId The first upstream correlation id to allocate.
     */
    public CorrelationManager(int initialCorrelationId) {
        upstreamId = initialCorrelationId;
    }

    /**
     * Allocate and return a correlation id for an outgoing request to the broker.
     *
     * @param apiKey                  The API key.
     * @param apiVersion              The API version.
     * @param downstreamCorrelationId The downstream correlation id to restore onto the response
     *                                (the client's own id for ordinary traffic, or the id
     *                                allocated for an internally-issued request).
     * @param hasResponse             Whether a response is expected.
     * @param path                    The position in the routing/filter tree this request was
     *                                sent from, or {@code null} if routing is not in use for this
     *                                virtual cluster; restored onto the response so it can be
     *                                observed by the same route's filters, and (via
     *                                {@link PathElement#pendingPromise()}) delivered back to whichever
     *                                filter or router issued it.
     * @param decodeResponse          Whether the response should be decoded.
     * @return The allocated upstream correlation id.
     */
    public int putBrokerRequest(short apiKey,
                                short apiVersion,
                                int downstreamCorrelationId,
                                boolean hasResponse,
                                @Nullable PathElement path,
                                boolean decodeResponse) {
        // need to allocate an id and put in a map for quick lookup, along with the "tag"
        int upstreamCorrelationId = upstreamId++;
        LOGGER.atTrace()
                .addKeyValue("upstreamCorrelationId", upstreamCorrelationId)
                .addKeyValue("downstreamCorrelationId", downstreamCorrelationId)
                .log("Allocated upstream id for downstream id");
        if (hasResponse) {
            Correlation existing = this.brokerRequests.put(upstreamCorrelationId,
                    new Correlation(apiKey, apiVersion, decodeResponse, downstreamCorrelationId, path));
            if (existing != null) {
                LOGGER.atError()
                        .addKeyValue("upstreamCorrelationId", upstreamCorrelationId)
                        .log("Duplicate upstream correlation id");
            }
        }
        return upstreamCorrelationId;
    }

    /**
     * Find (and remove) the Correlation for an incoming response from the broker
     * @param upstreamCorrelationId The (upstream) correlation id in the response.
     * @return The correlation for the given upstream correlation id, or null if none was recorded.
     */
    public Correlation getBrokerCorrelation(int upstreamCorrelationId) {
        // Set the tag on the response object
        return brokerRequests.remove(upstreamCorrelationId);
    }

    /**
     * A record for which responses should be decoded, together with their API key and version,
     * the downstream correlation id to restore, and the path the originating request was sent
     * from.
     *
     * @param apiKey the api key of the request
     * @param apiVersion the api version of the request
     * @param decodeResponse whether the response should be decoded
     * @param downstreamCorrelationId the downstream client's correlation id
     * @param path the position in the routing/filter tree the request was sent from, or
     *             {@code null} if routing is not in use for this virtual cluster
     */
    public record Correlation(short apiKey, short apiVersion, boolean decodeResponse, int downstreamCorrelationId, @Nullable PathElement path) {

        @Override
        public String toString() {
            return "Correlation(" +
                    "apiKey=" + ApiKeys.forId(apiKey) +
                    ", apiVersion=" + apiVersion +
                    ", decodeResponse=" + decodeResponse +
                    ", downstreamCorrelationId=" + downstreamCorrelationId +
                    ", path=" + path +
                    ')';
        }
    }
}
