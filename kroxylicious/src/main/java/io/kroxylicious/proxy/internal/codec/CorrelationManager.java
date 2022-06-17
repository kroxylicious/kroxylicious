/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.codec;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import io.kroxylicious.proxy.tag.VisibleForTesting;

/**
 * Manages correlation ids for a single connection (across the proxy) between a single client
 * and a single broker.
 */
public class CorrelationManager {

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
     * @param apiKey The API key.
     * @param apiVersion The API version.
     * @param downstreamCorrelationId The downstream client's correlation id.
     * @param decodeResponse Whether the response should be decoded.
     * @param hasResponse Whether a response is expected.
     */
    public int putBrokerRequest(short apiKey,
                                short apiVersion,
                                int downstreamCorrelationId,
                                boolean hasResponse,
                                boolean decodeResponse) {
        // need to allocate an id and put in a map for quick lookup, along with the "tag"
        int upstreamCorrelationId = upstreamId++;
        if (hasResponse) {
            this.brokerRequests.put(upstreamCorrelationId, new Correlation(apiKey, apiVersion, downstreamCorrelationId, decodeResponse));
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
    // TODO a perfect value type
    public static class Correlation {
        private final short apiKey;
        private final short apiVersion;

        private final int downstreamCorrelationId;
        private final boolean decodeResponse;

        private Correlation(short apiKey, short apiVersion, int downstreamCorrelationId, boolean decodeResponse) {
            this.apiKey = apiKey;
            this.apiVersion = apiVersion;
            this.downstreamCorrelationId = downstreamCorrelationId;
            this.decodeResponse = decodeResponse;
        }

        public int downstreamCorrelationId() {
            return downstreamCorrelationId;
        }

        @Override
        public String toString() {
            return "Correlation(" +
                    "apiKey=" + apiKey() +
                    ", apiVersion=" + apiVersion() +
                    ", downstreamCorrelationId=" + downstreamCorrelationId() +
                    ", decodeResponse=" + decodeResponse() +
                    ')';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Correlation that = (Correlation) o;
            return apiKey == that.apiKey && apiVersion == that.apiVersion && downstreamCorrelationId == that.downstreamCorrelationId
                    && decodeResponse == that.decodeResponse;
        }

        @Override
        public int hashCode() {
            return Objects.hash(apiKey, apiVersion, downstreamCorrelationId, decodeResponse);
        }

        public short apiKey() {
            return apiKey;
        }

        public short apiVersion() {
            return apiVersion;
        }

        public boolean decodeResponse() {
            return decodeResponse;
        }
    }
}
