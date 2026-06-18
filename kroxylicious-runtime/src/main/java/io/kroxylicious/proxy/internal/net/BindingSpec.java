/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.kroxylicious.proxy.service.HostPort;

/**
 * Describes what network sockets a gateway needs the OS to open.
 * <p>
 * This is asked once at startup by the {@link EndpointRegistry}. Ports may be zero,
 * signalling that the OS should assign an ephemeral port. The actual bound port is
 * available via {@link EndpointRegistry#resolvePort(VirtualNodeId)} after binding completes.
 * <p>
 * This is one of three views of a gateway's network configuration. See also
 * {@link AdvertisingSpec} (what to advertise to clients) and {@link RoutingSpec}
 * (how to identify incoming connections).
 */
public interface BindingSpec {

    /**
     * Address the proxy should bind for bootstrap connections. Port may be zero for OS assignment.
     */
    HostPort getBootstrapBindAddress();

    /**
     * Addresses to pre-bind for each known node before upstream topology is discovered.
     * Keys are {@link VirtualNodeId.Broker} instances; values are bind addresses (port may be zero).
     * <p>
     * These are bound before the first reconciliation and initially point to the upstream
     * bootstrap for metadata discovery only. They are replaced by real broker bindings
     * once topology is known.
     */
    Map<VirtualNodeId.Broker, HostPort> nodeBindAddresses();

    /**
     * Ports that this gateway requires exclusive use of. No other gateway may bind these.
     */
    Set<Integer> getExclusivePorts();

    /**
     * Ports that this gateway can share with other gateways (typically for SNI-based routing
     * where multiple gateways multiplex over one port).
     */
    Set<Integer> getSharedPorts();

    /**
     * Network interface to bind on, or empty to bind on all interfaces (0.0.0.0).
     */
    Optional<String> getBindAddress();

    /**
     * Whether connections to this gateway require TLS with Server Name Indication.
     */
    boolean requiresServerNameIndication();
}
