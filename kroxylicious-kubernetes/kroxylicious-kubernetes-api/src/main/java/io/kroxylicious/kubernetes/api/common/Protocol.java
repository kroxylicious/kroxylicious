/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.api.common;

/**
 * The transport protocol used between Kafka clients and an ingress endpoint of the proxy.
 */
public enum Protocol {

    /** Plain TCP; the connection between client and proxy is not encrypted. */
    @com.fasterxml.jackson.annotation.JsonProperty("TCP")
    TCP("TCP"),
    /** TLS; the connection between client and proxy is encrypted. */
    @com.fasterxml.jackson.annotation.JsonProperty("TLS")
    TLS("TLS");

    private final java.lang.String value;

    Protocol(java.lang.String value) {
        this.value = value;
    }

    /**
     * The value used to represent this protocol in the CRD schema.
     *
     * @return the CRD schema value for this protocol.
     */
    @com.fasterxml.jackson.annotation.JsonValue()
    public java.lang.String getValue() {
        return value;
    }
}
