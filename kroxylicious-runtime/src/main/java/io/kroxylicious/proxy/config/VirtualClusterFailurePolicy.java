/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Policy for handling virtual cluster startup failures.
 */
public enum VirtualClusterFailurePolicy {

    /** Any virtual cluster failure kills the proxy. This is the default. */
    @JsonProperty("none")
    NONE,

    /** Serve healthy virtual clusters, report failed ones. */
    @JsonProperty("remaining")
    REMAINING
}
