/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.integration.tester;

import java.time.Duration;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.config.VirtualClusterGatewayBuilder;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.testing.kafka.api.KafkaCluster;

/**
 * Class for utilities related to manipulating KroxyliciousConfig and it's builder.
 */
public class KroxyliciousConfigUtils {

    private KroxyliciousConfigUtils() {
    }

    /**
     * The name of the virtual cluster used when no name is supplied.
     */
    public static final String DEFAULT_VIRTUAL_CLUSTER = "demo";

    /**
     * The name of the gateway used when no name is supplied.
     */
    public static final String DEFAULT_GATEWAY_NAME = "default";

    /**
     * A bootstrap address on localhost with an OS-assigned (ephemeral) port.
     */
    public static final HostPort OS_ASSIGNED_BOOTSTRAP = new HostPort("localhost", 0);

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
        for (String virtualClusterName : virtualClusterNames) {
            var vcb = new VirtualClusterBuilder()
                    .withName(virtualClusterName)
                    .withNewTargetCluster()
                    .withBootstrapServers(clusterBootstrapServers)
                    .endTargetCluster()
                    .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(OS_ASSIGNED_BOOTSTRAP).build());
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
     * Locate the bootstrap servers for a virtual cluster
     * @param virtualCluster virtual cluster
     * @param config config to retrieve the bootstrap from
     * @param gateway gateway of the virtual cluster
     * @return bootstrap address
     * @throws IllegalStateException if we encounter an unknown endpoint config provider type for the virtualcluster
     * @throws IllegalArgumentException if the virtualCluster is not in the kroxylicious config
     */
    static String bootstrapServersFor(String virtualCluster, Configuration config, String gateway) {
        var cluster = config.virtualClusters().stream().filter(v -> v.name().equals(virtualCluster)).findFirst();
        if (cluster.isEmpty()) {
            throw new IllegalArgumentException("virtualCluster " + virtualCluster + " not found in config: " + config);
        }
        var first = cluster.get().gateways().stream().filter(l -> l.name().equals(gateway)).map(
                virtualClusterGateway -> virtualClusterGateway.buildNodeIdentificationStrategy(virtualCluster)).findFirst();
        var nodeIdentificationStrategy = first.orElseThrow(() -> new IllegalArgumentException(virtualCluster + " does not have gateway named " + gateway));
        // Need proper way to do this for embedded use-cases. We should have a way to query kroxy for the virtual cluster's
        // actual bootstrap after the proxy is started. The provider might support dynamic ports (port 0), so querying the
        // config might not work.
        return nodeIdentificationStrategy.getClusterBootstrapAddress().toString();
    }

    /**
     * Create a gateway builder with the default gateway name.
     * @return gateway builder
     */
    public static VirtualClusterGatewayBuilder defaultGatewayBuilder() {
        return new VirtualClusterGatewayBuilder().withName(DEFAULT_GATEWAY_NAME);
    }

    /**
     * Create a gateway builder with the default gateway name, using the port-identifies-node
     * scheme with the given bootstrap address.
     * @param proxyAddress the proxy's bootstrap address
     * @return gateway builder
     */
    public static VirtualClusterGatewayBuilder defaultPortIdentifiesNodeGatewayBuilder(HostPort proxyAddress) {
        return defaultGatewayBuilder()
                .withNewPortIdentifiesNode()
                .withBootstrapAddress(proxyAddress)
                .endPortIdentifiesNode();
    }

    /**
     * Create a gateway builder with the default gateway name, using the port-identifies-node
     * scheme with the given bootstrap address.
     * @param proxyAddress the proxy's bootstrap address ({@code host:port})
     * @return gateway builder
     */
    public static VirtualClusterGatewayBuilder defaultPortIdentifiesNodeGatewayBuilder(String proxyAddress) {
        return defaultPortIdentifiesNodeGatewayBuilder(HostPort.parse(proxyAddress));
    }

    /**
     * Create a gateway builder with the default gateway name, using the SNI-host-identifies-node
     * scheme with the given bootstrap address and broker address pattern.
     * @param bootstrapAddress the proxy's bootstrap address
     * @param advertisedBrokerAddressPattern the advertised broker address pattern
     * @return gateway builder
     */
    public static VirtualClusterGatewayBuilder defaultSniHostIdentifiesNodeGatewayBuilder(HostPort bootstrapAddress, String advertisedBrokerAddressPattern) {
        return defaultGatewayBuilder()
                .withNewSniHostIdentifiesNode()
                .withBootstrapAddress(bootstrapAddress.toString())
                .withAdvertisedBrokerAddressPattern(advertisedBrokerAddressPattern)
                .endSniHostIdentifiesNode();
    }

    /**
     * Create a gateway builder with the default gateway name, using the SNI-host-identifies-node
     * scheme with the given bootstrap address and broker address pattern.
     * @param bootstrapAddress the proxy's bootstrap address ({@code host:port})
     * @param advertisedBrokerAddressPattern the advertised broker address pattern
     * @return gateway builder
     */
    public static VirtualClusterGatewayBuilder defaultSniHostIdentifiesNodeGatewayBuilder(String bootstrapAddress, String advertisedBrokerAddressPattern) {
        return defaultSniHostIdentifiesNodeGatewayBuilder(HostPort.parse(bootstrapAddress), advertisedBrokerAddressPattern);
    }

    /**
     * Create a virtual cluster builder with the given name, targeting the given Kafka cluster.
     * @param cluster kafka cluster to proxy
     * @param clusterName name of the virtual cluster
     * @return virtual cluster builder
     */
    public static VirtualClusterBuilder baseVirtualClusterBuilder(KafkaCluster cluster, String clusterName) {
        return new VirtualClusterBuilder()
                .withNewTargetCluster()
                .withBootstrapServers(cluster.getBootstrapServers())
                .endTargetCluster()
                .withName(clusterName);
    }

    /**
     * Create a configuration builder pre-configured with zero shutdown quiet periods, so
     * that proxies started by tests stop promptly.
     * @return configuration builder
     */
    public static ConfigurationBuilder baseConfigurationBuilder() {
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.withNewNetwork()
                .withNewManagement().withShutdownQuietPeriod(Duration.ZERO).endManagement()
                .withNewProxy().withShutdownQuietPeriod(Duration.ZERO).endProxy()
                .endNetwork();
        return configurationBuilder;
    }
}
