/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Policy for handling virtual clusters that reach the terminal {@code stopped} state.
 */
public enum VirtualClusterFailurePolicy {

    /** Any virtual cluster reaching stopped kills the proxy. This is the default. */
    @JsonProperty("none")
    NONE,

    /** Serve virtual clusters that initialised successfully, report stopped ones. */
    @JsonProperty("successful")
    SUCCESSFUL
}
