/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.tester;

import io.kroxylicious.proxy.config.ClusterNetworkAddressConfigProviderDefinitionBuilder;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.PortPerBrokerClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.RangeAwarePortPerNodeClusterNetworkAddressConfigProvider.RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.testing.kafka.api.KafkaCluster;

import static io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.PortPerBrokerClusterNetworkAddressConfigProvider.PortPerBrokerClusterNetworkAddressConfigProviderConfig;
import static io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.SniRoutingClusterNetworkAddressConfigProvider.SniRoutingClusterNetworkAddressConfigProviderConfig;

/**
 * Class for utilities related to manipulating KroxyliciousConfig and it's builder.
 */
public class KroxyliciousConfigUtils {

    private KroxyliciousConfigUtils() {
    }

    public static final String DEFAULT_VIRTUAL_CLUSTER = "demo";
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
                    .withNewTargetCluster()
                    .withBootstrapServers(clusterBootstrapServers)
                    .endTargetCluster()
                    .withClusterNetworkAddressConfigProvider(
                            new ClusterNetworkAddressConfigProviderDefinitionBuilder(PortPerBrokerClusterNetworkAddressConfigProvider.class.getName())
                                    .withConfig("bootstrapAddress", new HostPort(DEFAULT_PROXY_BOOTSTRAP.host(), DEFAULT_PROXY_BOOTSTRAP.port() + i * 10))
                                    .build());
            configurationBuilder
                    .addToVirtualClusters(virtualClusterName, vcb.build());
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
     * @return bootstrap address
     * @throws IllegalStateException if we encounter an unknown endpoint config provider type for the virtualcluster
     * @throws IllegalArgumentException if the virtualCluster is not in the kroxylicious config
     */
    static String bootstrapServersFor(String virtualCluster, Configuration config) {
        var cluster = config.virtualClusters().get(virtualCluster);
        if (cluster == null) {
            throw new IllegalArgumentException("virtualCluster " + virtualCluster + " not found in config: " + config);
        }
        var provider = cluster.clusterNetworkAddressConfigProvider();
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
            return new HostPort(c.getBootstrapAddress().host(), c.getAdvertisedPort()).toString();
        }
        else {
            throw new IllegalStateException("I don't know how to handle ClusterEndpointConfigProvider type:" + provider.type());
        }
    }
}
