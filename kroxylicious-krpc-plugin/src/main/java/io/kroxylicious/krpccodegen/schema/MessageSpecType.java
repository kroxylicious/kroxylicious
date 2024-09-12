/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.krpccodegen.schema;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum MessageSpecType {
    /**
     * Kafka request RPCs.
     */
    @JsonProperty(
        "request"
    ) REQUEST,

    /**
     * Kafka response RPCs.
     */
    @JsonProperty(
        "response"
    ) RESPONSE,

    /**
     * Kafka RPC headers.
     */
    @JsonProperty(
        "header"
    ) HEADER,

    /**
     * KIP-631 controller records.
     */
    @JsonProperty(
        "metadata"
    ) METADATA,

    /**
     * Other message spec types.
     */
    @JsonProperty(
        "data"
    ) DATA;
}
