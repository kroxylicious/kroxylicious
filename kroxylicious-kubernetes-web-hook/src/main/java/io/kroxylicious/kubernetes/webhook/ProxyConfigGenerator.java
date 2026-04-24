/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.webhook;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;

import io.kroxylicious.kubernetes.api.v1alpha1.KroxyliciousSidecarConfigSpec;
import io.kroxylicious.kubernetes.api.v1alpha1.kroxylicioussidecarconfigspec.NodeIdRange;
import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.NamedRange;
import io.kroxylicious.proxy.config.PortIdentifiesNodeIdentificationStrategy;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.config.VirtualClusterGateway;
import io.kroxylicious.proxy.config.admin.ManagementConfiguration;
import io.kroxylicious.proxy.service.HostPort;

/**
 * Generates proxy configuration YAML for the sidecar container from a
 * {@link KroxyliciousSidecarConfigSpec}.
 */
class ProxyConfigGenerator {

    private static final String VIRTUAL_CLUSTER_NAME = "sidecar";
    private static final String GATEWAY_NAME = "local";
    private static final String LOCALHOST = "localhost";
    private static final int DEFAULT_BOOTSTRAP_PORT = 19092;
    private static final int DEFAULT_MANAGEMENT_PORT = 9190;
    private static final int DEFAULT_NODE_ID_START = 0;
    private static final int DEFAULT_NODE_ID_END = 2;

    private ProxyConfigGenerator() {
    }

    /**
     * Generates proxy configuration YAML for a sidecar.
     *
     * @param spec the sidecar config spec from the CRD
     * @return YAML string suitable for passing to the proxy via {@code --config}
     */
    static String generateConfig(KroxyliciousSidecarConfigSpec spec) {
        int bootstrapPort = resolveBootstrapPort(spec);
        int managementPort = resolveManagementPort(spec);
        int nodeIdStart = DEFAULT_NODE_ID_START;
        int nodeIdEnd = DEFAULT_NODE_ID_END;

        NodeIdRange nodeIdRange = spec.getNodeIdRange();
        if (nodeIdRange != null) {
            if (nodeIdRange.getStartInclusive() != null) {
                nodeIdStart = nodeIdRange.getStartInclusive().intValue();
            }
            if (nodeIdRange.getEndInclusive() != null) {
                nodeIdEnd = nodeIdRange.getEndInclusive().intValue();
            }
        }

        var portStrategy = new PortIdentifiesNodeIdentificationStrategy(
                new HostPort(LOCALHOST, bootstrapPort),
                LOCALHOST,
                bootstrapPort + 1,
                List.of(new NamedRange("default", nodeIdStart, nodeIdEnd)));

        var gateway = new VirtualClusterGateway(
                GATEWAY_NAME,
                portStrategy,
                null,
                Optional.empty());

        var targetCluster = new TargetCluster(
                spec.getUpstreamBootstrapServers(),
                Optional.empty());

        var virtualCluster = new VirtualCluster(
                VIRTUAL_CLUSTER_NAME,
                targetCluster,
                List.of(gateway),
                false,
                false,
                null);

        var management = new ManagementConfiguration(
                "0.0.0.0",
                managementPort,
                null);

        var configuration = new Configuration(
                management,
                null,
                null,
                List.of(virtualCluster),
                null,
                false,
                Optional.empty(),
                null,
                null);

        return toYaml(configuration);
    }

    static int resolveBootstrapPort(KroxyliciousSidecarConfigSpec spec) {
        Long port = spec.getBootstrapPort();
        return port != null ? port.intValue() : DEFAULT_BOOTSTRAP_PORT;
    }

    static int resolveManagementPort(KroxyliciousSidecarConfigSpec spec) {
        Long port = spec.getManagementPort();
        return port != null ? port.intValue() : DEFAULT_MANAGEMENT_PORT;
    }

    private static String toYaml(Configuration configuration) {
        try {
            return ConfigParser.createObjectMapper()
                    .writeValueAsString(configuration)
                    .stripTrailing();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
