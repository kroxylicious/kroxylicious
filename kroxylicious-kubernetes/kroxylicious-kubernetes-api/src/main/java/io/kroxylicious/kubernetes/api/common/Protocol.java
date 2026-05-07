/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.api.common;

public enum Protocol {

    @com.fasterxml.jackson.annotation.JsonProperty("TCP")
    TCP("TCP"),
    @com.fasterxml.jackson.annotation.JsonProperty("TLS")
    TLS("TLS");

    private final java.lang.String value;

    Protocol(java.lang.String value) {
        this.value = value;
    }

    @com.fasterxml.jackson.annotation.JsonValue()
    public java.lang.String getValue() {
        return value;
    }
}
