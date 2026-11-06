/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.api;

/**
 * Where a Kafka client runs relative to the proxy.
 * Determines which bootstrap address the proxy handle returns.
 */
public enum ClientLocation {
    ON_CLUSTER,
    OFF_CLUSTER
}
