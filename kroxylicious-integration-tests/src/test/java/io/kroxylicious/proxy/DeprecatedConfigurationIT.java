/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.proxy.config.ClusterNetworkAddressConfigProviderDefinition;
import io.kroxylicious.proxy.config.ClusterNetworkAddressConfigProviderDefinitionBuilder;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.PortPerBrokerClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.RangeAwarePortPerNodeClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.testing.kafka.api.KafkaCluster;

import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

/**
 * Tests of deprecated configurations and features
 */
public class DeprecatedConfigurationIT extends BaseIT {

    private static final HostPort PROXY_ADDRESS = HostPort.parse("localhost:9192");

    @SuppressWarnings("removal")
    static Stream<Arguments> shouldSupportDeprecatedClusterNetworkAddressConfigProvider() {
        return Stream.of(argumentSet("PortPerBrokerClusterNetworkAddressConfigProvider", new ClusterNetworkAddressConfigProviderDefinitionBuilder(
                PortPerBrokerClusterNetworkAddressConfigProvider.class.getName())
                .withConfig("bootstrapAddress", PROXY_ADDRESS)
                .build()),
                argumentSet("RangeAwarePortPerNodeClusterNetworkAddressConfigProvider", new ClusterNetworkAddressConfigProviderDefinitionBuilder(
                        RangeAwarePortPerNodeClusterNetworkAddressConfigProvider.class.getName())
                        .withConfig("bootstrapAddress", PROXY_ADDRESS,
                                "nodeIdRanges", List.of(Map.of("name", "myrange", "range", Map.of("startInclusive", 0, "endExclusive", "1"))))
                        .build()));
    }

    @ParameterizedTest
    @MethodSource
    @SuppressWarnings("deprecation")
    void shouldSupportDeprecatedClusterNetworkAddressConfigProvider(ClusterNetworkAddressConfigProviderDefinition provider, KafkaCluster cluster) {

        var builder = new ConfigurationBuilder()
                .addToVirtualClusters(new VirtualClusterBuilder()
                        .withName("demo")
                        .withNewTargetCluster()
                        .withBootstrapServers(cluster.getBootstrapServers())
                        .endTargetCluster()
                        .withClusterNetworkAddressConfigProvider(provider)
                        .build());

        try (var tester = kroxyliciousTester(builder);
                var admin = tester.admin()) {
            // do some work to ensure connection is opened
            final CreateTopicsResult createTopicsResult = createTopic(admin, "mytopic", 1);
            assertThat(createTopicsResult.all()).isDone();
        }
    }
}
