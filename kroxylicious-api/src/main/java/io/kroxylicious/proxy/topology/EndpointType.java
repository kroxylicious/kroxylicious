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
 *
 * <p>{@link VirtualNode} instances are also used as request targets in
 * {@link io.kroxylicious.proxy.router.RouterContext#sendRequest
 * RouterContext.sendRequest} — a virtual node identifies a specific
 * broker regardless of whether it came from the connection endpoint or
 * from a protocol response via
 * {@link io.kroxylicious.proxy.router.RouterContext#nodeForId
 * RouterContext.nodeForId}.</p>
 */
public sealed interface EndpointType {

    /**
     * A bootstrap endpoint — a cluster entry point with no specific broker
     * identity. The runtime selects which broker to connect to.
     */
    record Bootstrap() implements EndpointType {}

    /**
     * A broker-specific endpoint in the virtual cluster topology.
     * Identifies a specific broker by its downstream (virtual) node ID.
     *
     * <p>Implementations provide {@code equals}/{@code hashCode} so that
     * {@code VirtualNode} instances can be used as map keys.</p>
     *
     * @param downstreamNodeId the node ID as seen by the client
     */
    record VirtualNode(int downstreamNodeId) implements EndpointType {}
}
