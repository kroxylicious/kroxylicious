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
import io.kroxylicious.kubernetes.api.v1alpha1.kroxylicioussidecarconfigspec.FilterDefinitions;
import io.kroxylicious.kubernetes.api.v1alpha1.kroxylicioussidecarconfigspec.NodeIdRange;
import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.NamedRange;
import io.kroxylicious.proxy.config.PortIdentifiesNodeIdentificationStrategy;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.config.VirtualClusterGateway;
import io.kroxylicious.proxy.config.admin.ManagementConfiguration;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.config.tls.TrustStore;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Generates proxy configuration YAML for the sidecar container from a
 * {@link KroxyliciousSidecarConfigSpec}.
 */
class ProxyConfigGenerator {

    private static final String VIRTUAL_CLUSTER_NAME = "sidecar";
    private static final String GATEWAY_NAME = "local";
    private static final String LOCALHOST = "localhost";
    static final int DEFAULT_BOOTSTRAP_PORT;
    static final int DEFAULT_MANAGEMENT_PORT;
    static {
        // Use the fact that the generated code does actually apply the defaults
        KroxyliciousSidecarConfigSpec spec = new KroxyliciousSidecarConfigSpec();
        DEFAULT_BOOTSTRAP_PORT = spec.getBootstrapPort().intValue();
        DEFAULT_MANAGEMENT_PORT = spec.getManagementPort().intValue();
    }
    private static final int DEFAULT_NODE_ID_START;
    private static final int DEFAULT_NODE_ID_END;
    static {
        NodeIdRange nodeIdRange = new NodeIdRange();
        DEFAULT_NODE_ID_START = nodeIdRange.getStartInclusive().intValue();
        DEFAULT_NODE_ID_END = nodeIdRange.getEndInclusive().intValue();
    }

    private ProxyConfigGenerator() {
    }

    /**
     * Generates proxy configuration YAML for a sidecar.
     *
     * @param spec the sidecar config spec from the CRD
     * @param upstreamTrustStorePath path to the mounted CA cert file for upstream TLS, or null if no TLS
     * @return YAML string suitable for passing to the proxy via {@code --config}
     */
    static String generateConfig(
                                 KroxyliciousSidecarConfigSpec spec,
                                 @Nullable String upstreamTrustStorePath) {
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

        Optional<Tls> upstreamTls = Optional.empty();
        if (upstreamTrustStorePath != null) {
            upstreamTls = Optional.of(new Tls(
                    null,
                    new TrustStore(upstreamTrustStorePath, null, "PEM"),
                    null,
                    null));
        }

        var targetCluster = new TargetCluster(
                spec.getTargetBootstrapServers(),
                upstreamTls);

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

        // Convert CRD filter definitions to proxy config model
        List<NamedFilterDefinition> filterDefs = toNamedFilterDefinitions(spec);
        List<String> defaultFilters = filterDefs != null
                ? filterDefs.stream().map(NamedFilterDefinition::name).toList()
                : null;

        var configuration = new Configuration(
                management,
                filterDefs,
                defaultFilters,
                List.of(virtualCluster),
                null,
                false,
                Optional.empty(),
                null,
                null);

        return toYaml(configuration);
    }

    /**
     * Overload for backwards compatibility (no upstream TLS).
     */
    static String generateConfig(KroxyliciousSidecarConfigSpec spec) {
        return generateConfig(spec, null);
    }

    @Nullable
    private static List<NamedFilterDefinition> toNamedFilterDefinitions(KroxyliciousSidecarConfigSpec spec) {
        List<FilterDefinitions> crdFilters = spec.getFilterDefinitions();
        if (crdFilters == null || crdFilters.isEmpty()) {
            return null;
        }
        return crdFilters.stream()
                .map(f -> new NamedFilterDefinition(f.getName(), f.getType(), f.getConfig()))
                .toList();
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
