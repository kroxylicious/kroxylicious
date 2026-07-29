/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.topology;

/**
 * Describes the type of endpoint that a client connected to.
 *
 * <p>A client connection arrives on either a {@link Bootstrap} endpoint (a
 * cluster entry point with no specific broker identity) or a
 * {@link VirtualNode} endpoint (a broker-specific address in the virtual
 * cluster topology).</p>
 */
public sealed interface EndpointType permits Bootstrap, VirtualNode {
}
