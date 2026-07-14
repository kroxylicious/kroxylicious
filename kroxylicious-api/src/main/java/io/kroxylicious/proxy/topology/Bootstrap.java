/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.topology;

/**
 * A bootstrap endpoint — a cluster entry point with no specific broker
 * identity. The runtime selects which broker to connect to.
 */
public record Bootstrap() implements EndpointType {}
