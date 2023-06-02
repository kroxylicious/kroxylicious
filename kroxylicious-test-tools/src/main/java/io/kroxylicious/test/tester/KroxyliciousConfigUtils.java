/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.tester;

import java.util.Map;

import io.kroxylicious.proxy.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.KroxyliciousConfig;
import io.kroxylicious.proxy.KroxyliciousConfigBuilder;
import io.kroxylicious.proxy.VirtualCluster;
import io.kroxylicious.proxy.VirtualClusterBuilder;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.testing.kafka.api.KafkaCluster;

/**
 * Class for utilities related to manipulating KroxyliciousConfig and it's builder.
 */
public class KroxyliciousConfigUtils {

    public static final String DEFAULT_VIRTUAL_CLUSTER = "demo";
    private static final HostPort DEFAULT_PROXY_BOOTSTRAP = new HostPort("localhost", 9192);

    /**
     * Create a KroxyliciousConfigBuilder with a single virtual cluster configured to
     * proxy an externally provided bootstrap server.
     * @param clusterBootstrapServers external bootstrap server
     * @return builder
     */
    public static KroxyliciousConfigBuilder proxy(String clusterBootstrapServers) {
        return KroxyliciousConfig.builder().addToVirtualClusters(DEFAULT_VIRTUAL_CLUSTER, new VirtualClusterBuilder()
                .withNewTargetCluster()
                .withBootstrapServers(clusterBootstrapServers)
                .endTargetCluster()
                .withNewClusterNetworkAddressConfigProvider()
                .withType("PortPerBroker")
                .withConfig(Map.of("bootstrapAddress", DEFAULT_PROXY_BOOTSTRAP.toString()))
                .endClusterNetworkAddressConfigProvider()
                .build());
    }

    /**
     * Create a KroxyliciousConfigBuilder with a single virtual cluster configured to
     * proxy a KafkaCluster.
     * @param cluster kafka cluster to proxy
     * @return builder
     */
    public static KroxyliciousConfigBuilder proxy(KafkaCluster cluster) {
        return proxy(cluster.getBootstrapServers());
    }

    /**
     * Augments a KroxyliciousConfigBuilder with standard filters required to proxy a Kafka broker
     * @param builder builder to add filters to
     * @return builder
     */
    public static KroxyliciousConfigBuilder withDefaultFilters(KroxyliciousConfigBuilder builder) {
        return builder.addNewFilter().withType("ApiVersions").endFilter();
    }

    /**
     * Locate the bootstrap servers for a virtual cluster
     * @param virtualCluster virtual cluster
     * @param config config to retrieve the bootstrap from
     * @return bootstrap address
     * @throws IllegalStateException if we encounter an unknown endpoint config provider type for the virtualcluster
     * @throws IllegalArgumentException if the virtualCluster is not in the kroxylicious config
     */
    static String bootstrapServersFor(String virtualCluster, KroxyliciousConfig config) {
        VirtualCluster cluster = config.getVirtualClusters().get(virtualCluster);
        if (cluster == null) {
            throw new IllegalArgumentException("virtualCluster " + virtualCluster + " not found in config: " + config);
        }
        ClusterNetworkAddressConfigProvider provider = cluster.clusterNetworkAddressConfigProvider();
        if (provider.type().equals("PortPerBroker") || provider.type().equals("SniRouting")) {
            Object bootstrapAddress = provider.config().get("bootstrapAddress");
            return (String) bootstrapAddress;
        }
        else {
            throw new IllegalStateException("I don't know how to handle ClusterEndpointConfigProvider type:" + provider.type());
        }
    }
}
