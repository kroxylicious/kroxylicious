/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.service.NodeIdentificationStrategy;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A virtual cluster listener.
 *
 * @param name name of the listener
 * @param portIdentifiesNode network config
 * @param sniHostIdentifiesNode network config
 * @param tls tls settings
 * @param proxyProtocol per-gateway PROXY protocol configuration; {@code null} means {@link ProxyProtocolMode#FORBIDDEN}
 */
public record VirtualClusterGateway(@JsonProperty(required = true) String name,
                                    @Nullable @JsonProperty(required = false) PortIdentifiesNodeIdentificationStrategy portIdentifiesNode,
                                    @Nullable @JsonProperty(required = false) SniHostIdentifiesNodeIdentificationStrategy sniHostIdentifiesNode,
                                    Optional<Tls> tls,
                                    @Nullable @JsonProperty(required = false) ProxyProtocolConfig proxyProtocol) {

    /**
     * Validates the gateway: exactly one of {@code portIdentifiesNode} or
     * {@code sniHostIdentifiesNode} must be given, and TLS is mandatory when SNI-based
     * node identification is used.
     */
    public VirtualClusterGateway {
        Objects.requireNonNull(name);
        Objects.requireNonNull(tls);

        if ((portIdentifiesNode == null) == (sniHostIdentifiesNode == null)) {
            throw new IllegalConfigurationException("Must specify either portIdentifiesNode or sniHostIdentifiesNode (virtual cluster listener %s)".formatted(name));
        }

        if (sniHostIdentifiesNode != null && tls.isEmpty()) {
            throw new IllegalConfigurationException("When using 'sniHostIdentifiesNode', 'tls' must be provided (virtual cluster listener %s)".formatted(name));
        }
    }

    /**
     * Returns the effective PROXY protocol mode for this gateway.
     * When no {@code proxyProtocol} block is configured, {@link ProxyProtocolMode#FORBIDDEN} is returned.
     *
     * @return the proxy protocol mode
     */
    public ProxyProtocolMode proxyProtocolMode() {
        return proxyProtocol != null ? proxyProtocol.mode() : ProxyProtocolMode.FORBIDDEN;
    }

    /**
     * Returns the configured bootstrap port for this gateway, or {@code 0} if the port is
     * OS-assigned (i.e. not yet known).
     *
     * @return bootstrap port, or 0 for ephemeral/OS-assigned ports
     */
    int bootstrapPort() {
        if (portIdentifiesNode != null) {
            var addr = portIdentifiesNode.getBootstrapAddress();
            return addr != null ? addr.port() : 0;
        }
        if (sniHostIdentifiesNode != null) {
            return sniHostIdentifiesNode.getBootstrapPort();
        }
        return 0;
    }

    /**
     * Builds the {@link NodeIdentificationStrategy} from whichever of the port-based or
     * SNI-based configurations was specified.
     *
     * @param clusterName name of the virtual cluster owning this gateway
     * @return the node identification strategy
     */
    public NodeIdentificationStrategy buildNodeIdentificationStrategy(String clusterName) {
        if (portIdentifiesNode() != null) {
            return portIdentifiesNode().buildStrategy(clusterName);
        }
        else if (sniHostIdentifiesNode() != null) {
            return sniHostIdentifiesNode().buildStrategy(clusterName);
        }
        else {
            throw new IllegalStateException("Failed to create provider for " + this);
        }
    }

    @Override
    public String toString() {
        return "VirtualClusterListener{" +
                "name='" + name + '\'' +
                ", portIdentifiesNode=" + portIdentifiesNode +
                ", sniHostIdentifiesNode=" + sniHostIdentifiesNode +
                ", tls=" + tls +
                ", proxyProtocol=" + proxyProtocol +
                '}';
    }
}
