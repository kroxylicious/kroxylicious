/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.tester;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedRange;
import io.kroxylicious.proxy.config.PortIdentifiesNodeIdentificationStrategy;
import io.kroxylicious.proxy.config.SniHostIdentifiesNodeIdentificationStrategy;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.config.VirtualClusterGateway;
import io.kroxylicious.proxy.config.VirtualClusterGatewayBuilder;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.RangeAwarePortPerNodeClusterNetworkAddressConfigProvider.RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.testing.kafka.api.KafkaCluster;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.PortPerBrokerClusterNetworkAddressConfigProvider.PortPerBrokerClusterNetworkAddressConfigProviderConfig;
import static io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.SniRoutingClusterNetworkAddressConfigProvider.SniRoutingClusterNetworkAddressConfigProviderConfig;

/**
 * Class for utilities related to manipulating KroxyliciousConfig and it's builder.
 */
public class KroxyliciousConfigUtils {

    private KroxyliciousConfigUtils() {
    }

    public static final String DEFAULT_VIRTUAL_CLUSTER = "demo";
    public static final String DEFAULT_GATEWAY_NAME = "default";

    static final HostPort DEFAULT_PROXY_BOOTSTRAP = new HostPort("localhost", 9192);

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
        final ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
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
        var first = getVirtualClusterGatewayStream(cluster.get()).filter(l -> l.name().equals(gateway)).map(VirtualClusterGateway::clusterNetworkAddressConfigProvider)
                .findFirst();
        var provider = first.orElseThrow(() -> new IllegalArgumentException(virtualCluster + " does not have gateway named " + gateway));

        // Need proper way to do this for embedded use-cases. We should have a way to query kroxy for the virtual cluster's
        // actual bootstrap after the proxy is started. The provider might support dynamic ports (port 0), so querying the
        // config might not work.
        if (provider.config() instanceof PortPerBrokerClusterNetworkAddressConfigProviderConfig c) {
            return c.getBootstrapAddress().toString();
        }
        if (provider.config() instanceof RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig c) {
            return c.getBootstrapAddress().toString();
        }
        else if (provider.config() instanceof SniRoutingClusterNetworkAddressConfigProviderConfig c) {
            String bootstrapAddressPattern = c.getBootstrapAddressPattern();
            String replaced = bootstrapAddressPattern.replace("$(virtualClusterName)", virtualCluster);
            return new HostPort(replaced, c.getAdvertisedPort()).toString();
        }
        else {
            throw new IllegalStateException("I don't know how to handle ClusterEndpointConfigProvider type:" + provider.type());
        }
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

    public static Stream<VirtualClusterGateway> getVirtualClusterGatewayStream(VirtualCluster cluster) {
        return Optional.ofNullable(cluster.gateways())
                .filter(Predicate.not(List::isEmpty))
                .map(Collection::stream)
                .orElseGet(() -> buildGatewayFromDeprecatedNetworkProvider(cluster));
    }

    @NonNull
    @SuppressWarnings("removal")
    private static Stream<VirtualClusterGateway> buildGatewayFromDeprecatedNetworkProvider(VirtualCluster cluster) {
        var providerDefinition = cluster.clusterNetworkAddressConfigProvider();
        if (providerDefinition.config() instanceof PortPerBrokerClusterNetworkAddressConfigProviderConfig pc) {
            var strategy = new PortIdentifiesNodeIdentificationStrategy(pc.getBootstrapAddress(),
                    pc.getBrokerAddressPattern(),
                    pc.getBrokerStartPort(),
                    null);

            return Stream.of(new VirtualClusterGateway(DEFAULT_GATEWAY_NAME,
                    strategy, null, cluster.tls()));
        }
        else if (providerDefinition.config() instanceof RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig rc) {
            var ranges = rc.getNodeIdRanges().stream()
                    .map(nir -> new NamedRange(nir.name(), nir.rangeSpec().startInclusive(), nir.rangeSpec().endExclusive() - 1))
                    .toList();
            var strategy = new PortIdentifiesNodeIdentificationStrategy(rc.getBootstrapAddress(),
                    rc.getNodeAddressPattern(),
                    rc.getNodeStartPort(),
                    ranges);

            return Stream.of(new VirtualClusterGateway(DEFAULT_GATEWAY_NAME,
                    strategy, null, cluster.tls()));
        }
        else if (providerDefinition.config() instanceof SniRoutingClusterNetworkAddressConfigProviderConfig sc) {
            var strategy = new SniHostIdentifiesNodeIdentificationStrategy(new HostPort(sc.getBootstrapAddressPattern(), sc.getAdvertisedPort()).toString(),
                    sc.getAdvertisedBrokerAddressPattern());

            return Stream.of(new VirtualClusterGateway(DEFAULT_GATEWAY_NAME,
                    null, strategy, cluster.tls()));
        }
        throw new UnsupportedOperationException(providerDefinition.type() + " is unrecognised");
    }

    @NonNull
    public static VirtualClusterBuilder baseVirtualClusterBuilder(KafkaCluster cluster, String clusterName) {
        return new VirtualClusterBuilder()
                .withNewTargetCluster()
                .withBootstrapServers(cluster.getBootstrapServers())
                .endTargetCluster()
                .withName(clusterName);
    }
}
