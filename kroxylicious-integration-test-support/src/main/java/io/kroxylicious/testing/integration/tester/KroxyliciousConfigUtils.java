/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.integration.tester;

import java.time.Duration;
import java.util.Optional;
import java.util.function.BiFunction;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.config.VirtualClusterGatewayBuilder;
import io.kroxylicious.proxy.internal.net.EndpointRegistry;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.testing.kafka.api.KafkaCluster;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Class for utilities related to manipulating KroxyliciousConfig and it's builder.
 */
public class KroxyliciousConfigUtils {

    private KroxyliciousConfigUtils() {
    }

    public static final String DEFAULT_VIRTUAL_CLUSTER = "demo";
    public static final String DEFAULT_GATEWAY_NAME = "default";

    public static final HostPort DEFAULT_PROXY_BOOTSTRAP = new HostPort("localhost", EndpointRegistry.OS_ASSIGNED_PORT);

    /**
     * Create a KroxyliciousConfigBuilder with a single virtual cluster configured to
     * proxy an externally provided bootstrap server.
     * @param clusterBootstrapServers external bootstrap server
     * @return builder
     */
    public static ConfigurationBuilder proxy(String clusterBootstrapServers) {
        return proxy(clusterBootstrapServers, DEFAULT_VIRTUAL_CLUSTER);
    }

    /**
     * Create a KroxyliciousConfigBuilder with a virtual cluster for each supplied name configured to
     * proxy an externally provided single bootstrap server. I.e. many virtual clusters on a single target cluster.
     *
     * @param clusterBootstrapServers external bootstrap server
     * @param virtualClusterNames the name to use for the virtual cluster
     * @return builder
     */
    public static ConfigurationBuilder proxy(String clusterBootstrapServers, String... virtualClusterNames) {
        final ConfigurationBuilder configurationBuilder = baseConfigurationBuilder();
        for (int i = 0; i < virtualClusterNames.length; i++) {
            String virtualClusterName = virtualClusterNames[i];
            var vcb = new VirtualClusterBuilder()
                    .withName(virtualClusterName)
                    .withNewTargetCluster()
                    .withBootstrapServers(clusterBootstrapServers)
                    .endTargetCluster()
                    .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(new HostPort(DEFAULT_PROXY_BOOTSTRAP.host(), DEFAULT_PROXY_BOOTSTRAP.port() + i * 10))
                            .build());
            configurationBuilder
                    .addToVirtualClusters(vcb.build());
        }
        return configurationBuilder;
    }

    /**
     * Create a KroxyliciousConfigBuilder with a single virtual cluster configured to
     * proxy a KafkaCluster.
     * @param cluster kafka cluster to proxy
     * @return builder
     */
    public static ConfigurationBuilder proxy(KafkaCluster cluster) {
        return proxy(cluster.getBootstrapServers());
    }

    /**
     * Resolves the bootstrap address for a named virtual cluster and gateway, using a caller-supplied
     * port resolver to obtain the actual listening port.
     *
     * @param virtualCluster the virtual cluster name
     * @param config the proxy configuration
     * @param gateway the gateway name
     * @param portResolver maps (bindAddress, configuredPort) to the actual port; for fixed ports, returns the port unchanged
     * @return the bootstrap address with the resolved port
     */
    public static HostPort bootstrapAddressFor(String virtualCluster, Configuration config, String gateway,
                                               BiFunction<Optional<String>, Integer, Integer> portResolver) {
        var vc = config.virtualClusters().stream()
                .filter(v -> v.name().equals(virtualCluster))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("virtualCluster " + virtualCluster + " not found in config"));
        var strategy = vc.gateways().stream()
                .filter(g -> g.name().equals(gateway))
                .map(g -> g.buildNodeIdentificationStrategy(virtualCluster))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(virtualCluster + " does not have gateway named " + gateway));
        var bootstrapAddress = strategy.getClusterBootstrapAddress();
        return new HostPort(bootstrapAddress.host(), portResolver.apply(strategy.getBindAddress(), bootstrapAddress.port()));
    }

    public static VirtualClusterGatewayBuilder defaultGatewayBuilder() {
        return new VirtualClusterGatewayBuilder().withName(DEFAULT_GATEWAY_NAME);
    }

    public static VirtualClusterGatewayBuilder defaultPortIdentifiesNodeGatewayBuilder(HostPort proxyAddress) {
        return defaultGatewayBuilder()
                .withNewPortIdentifiesNode()
                .withBootstrapAddress(proxyAddress)
                .endPortIdentifiesNode();
    }

    public static VirtualClusterGatewayBuilder defaultPortIdentifiesNodeGatewayBuilder(String proxyAddress) {
        return defaultPortIdentifiesNodeGatewayBuilder(HostPort.parse(proxyAddress));
    }

    public static VirtualClusterGatewayBuilder defaultSniHostIdentifiesNodeGatewayBuilder(HostPort bootstrapAddress, String advertisedBrokerAddressPattern) {
        return defaultGatewayBuilder()
                .withNewSniHostIdentifiesNode()
                .withBootstrapAddress(bootstrapAddress.toString())
                .withAdvertisedBrokerAddressPattern(advertisedBrokerAddressPattern)
                .endSniHostIdentifiesNode();
    }

    public static VirtualClusterGatewayBuilder defaultSniHostIdentifiesNodeGatewayBuilder(String bootstrapAddress, String advertisedBrokerAddressPattern) {
        return defaultSniHostIdentifiesNodeGatewayBuilder(HostPort.parse(bootstrapAddress), advertisedBrokerAddressPattern);
    }

    @NonNull
    public static VirtualClusterBuilder baseVirtualClusterBuilder(KafkaCluster cluster, String clusterName) {
        return new VirtualClusterBuilder()
                .withNewTargetCluster()
                .withBootstrapServers(cluster.getBootstrapServers())
                .endTargetCluster()
                .withName(clusterName);
    }

    public static ConfigurationBuilder baseConfigurationBuilder() {
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.withNewNetwork()
                .withNewManagement().withShutdownQuietPeriod(Duration.ZERO).endManagement()
                .withNewProxy().withShutdownQuietPeriod(Duration.ZERO).endProxy()
                .endNetwork();
        return configurationBuilder;
    }
}
