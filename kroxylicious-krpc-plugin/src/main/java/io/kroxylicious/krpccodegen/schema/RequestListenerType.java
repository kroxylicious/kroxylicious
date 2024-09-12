/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.krpccodegen.schema;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum RequestListenerType {
    @JsonProperty(
        "zkBroker"
    ) ZK_BROKER,

    @JsonProperty(
        "broker"
    ) BROKER,

    @JsonProperty(
        "controller"
    ) CONTROLLER;
}
