/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

/**
 * Runtime-internal identity for a proxy endpoint within a gateway.
 * <p>
 * This type is the canonical key used by the {@link EndpointRegistry} to track bound
 * channels and resolve actual ports. It distinguishes bootstrap connections (no specific
 * broker node) from connections targeting a specific broker node.
 * <p>
 * <b>Note:</b> This is a temporary internal type. The routing API introduces a
 * {@code VirtualNode} concept that serves the same purpose and will be exposed to
 * Router/Filter plugins. When that API lands, this type will be replaced by the
 * routing API's {@code VirtualNode} throughout the runtime.
 */
public sealed interface VirtualNodeId permits VirtualNodeId.Bootstrap, VirtualNodeId.Broker {

    /**
     * The gateway this node identity belongs to.
     */
    EndpointGateway gateway();

    /**
     * Identity for a bootstrap connection — no specific broker node targeted.
     * Bootstrap connections are handled by forwarding to any upstream broker
     * for initial topology discovery.
     */
    record Bootstrap(EndpointGateway gateway) implements VirtualNodeId {}

    /**
     * Identity for a connection targeting a specific broker node.
     */
    record Broker(EndpointGateway gateway, int nodeId) implements VirtualNodeId {}
}
