/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.test.client;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks api version for requests
 */
public class CorrelationManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(CorrelationManager.class);

    final Map<Integer, Correlation> brokerRequests = new HashMap<>();

    /**
     * Creates an empty CorrelationManager
     */
    public CorrelationManager() {
    }

    /**
     * Allocate and return a correlation id for an outgoing request to the broker.
     *
     * @param apiKey                  The API key.
     * @param apiVersion              The API version.
     * @param correlationId The request's correlation id.
     */
    public void putBrokerRequest(short apiKey,
                                 short apiVersion,
                                 int correlationId) {
        Correlation existing = this.brokerRequests.put(correlationId, new Correlation(apiKey, apiVersion));
        if (existing != null) {
            LOGGER.error("Duplicate upstream correlation id {}", correlationId);
        }
    }

    /**
     * Find (and remove) the Correlation for an incoming response from the broker
     * @param correlationId The correlation id in the response.
     * @return the correlation
     */
    public Correlation getBrokerCorrelation(int correlationId) {
        return brokerRequests.remove(correlationId);
    }

    /**
     * A record for which responses should be decoded, together with their
     * API key and version.
     */
    // TODO a perfect value type
    public static class Correlation {
        private final short apiKey;
        private final short apiVersion;

        private Correlation(short apiKey,
                            short apiVersion) {
            this.apiKey = apiKey;
            this.apiVersion = apiVersion;
        }

        @Override
        public String toString() {
            return "Correlation(" +
                    "apiKey=" + ApiKeys.forId(apiKey) +
                    ", apiVersion=" + apiVersion +
                    ')';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Correlation that = (Correlation) o;
            return apiKey == that.apiKey && apiVersion == that.apiVersion;
        }

        @Override
        public int hashCode() {
            return Objects.hash(apiKey, apiVersion);
        }

        /**
         * get the api key of the request
         * @return api key
         */
        public short apiKey() {
            return apiKey;
        }

        /**
         * get the api version of the request
         * @return api version
         */
        public short apiVersion() {
            return apiVersion;
        }
    }
}
