/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.topology;

/**
 * A broker in the virtual cluster topology, identified by its
 * downstream (virtual) node ID.
 *
 * <p>{@code VirtualNode} instances are obtained from
 * {@link io.kroxylicious.proxy.router.RouterContext#endpoint() RouterContext.endpoint()}
 * (for broker-specific connections) or
 * {@link io.kroxylicious.proxy.router.RouterContext#nodeForId(int) RouterContext.nodeForId()}
 * (from protocol response node IDs), and passed to
 * {@link io.kroxylicious.proxy.router.RouterContext#sendRequest RouterContext.sendRequest()}
 * as request targets.</p>
 *
 * @param downstreamNodeId the node ID as seen by the client
 */
public record VirtualNode(int downstreamNodeId) implements EndpointType {}
