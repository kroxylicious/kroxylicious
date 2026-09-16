/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.integration.config;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;

import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.IllegalConfigurationException;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.PortIdentifiesNodeIdentificationStrategy;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.config.VirtualClusterGateway;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for backward compatibility with deprecated targetCluster configuration format.
 * These tests verify that the deprecated inline targetCluster format continues to work
 * while users migrate to the new clusterDefinitions + target.cluster reference pattern.
 *
 * @deprecated These tests cover deprecated functionality. New tests should use clusterDefinitions.
 */
@Deprecated(since = "0.22.0", forRemoval = true)
@SuppressWarnings("removal")
class ConfigurationDeprecatedVirtualClusterTargetClusterCompatibilityTest {

    private static final VirtualClusterGateway VIRTUAL_CLUSTER_GATEWAY = KroxyliciousConfigUtils.defaultGatewayBuilder()
            .withNewPortIdentifiesNode()
            .withBootstrapAddress(HostPort.parse("example.com:1234"))
            .endPortIdentifiesNode()
            .build();

    private final ConfigParser configParser = new ConfigParser();

    @Test
    void shouldRejectVirtualClusterWithNoGateways() {
        assertThatThrownBy(() -> configParser.parseConfiguration(
                """
                        virtualClusters:
                          - name: cluster
                            targetCluster:
                              bootstrapServers: kafka.example:1234
                        """)).isInstanceOf(IllegalArgumentException.class)
                .cause()
                .isInstanceOf(MismatchedInputException.class)
                .hasMessageContaining("Missing required creator property 'gateways'");
    }

    @Test
    void shouldRejectVirtualClusterWithNullGateways() {
        assertThatThrownBy(() -> configParser.parseConfiguration(
                """
                        virtualClusters:
                          - name: cluster
                            targetCluster:
                              bootstrapServers: kafka.example:1234
                            gateways: null
                        """)).isInstanceOf(IllegalArgumentException.class)
                .cause()
                .isInstanceOf(ValueInstantiationException.class)
                .hasCauseInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("no gateways configured for virtual cluster 'cluster'");
    }

    @Test
    void shouldRejectVirtualClusterNullGatewayValue() {
        assertThatThrownBy(() -> configParser.parseConfiguration(
                """
                        virtualClusters:
                          - name: cluster
                            targetCluster:
                              bootstrapServers: kafka.example:1234
                            gateways: [null]
                        """)).isInstanceOf(IllegalArgumentException.class)
                .cause()
                .isInstanceOf(ValueInstantiationException.class)
                .hasCauseInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("one or more gateways were null for virtual cluster 'cluster'");
    }

    @Test
    void shouldRejectMissingClusterFilterWithDeprecatedTargetCluster() {
        Optional<Map<String, Object>> development = Optional.empty();
        List<NamedFilterDefinition> filterDefinitions = List.of();
        List<VirtualClusterGateway> defaultGateway = List.of(VIRTUAL_CLUSTER_GATEWAY);
        TargetCluster targetCluster = new TargetCluster("unused:9082", Optional.empty());
        List<VirtualCluster> virtualClusters = List
                .of(new VirtualCluster("vc1", targetCluster, defaultGateway, false, false, List.of("missing")));
        assertThatThrownBy(() -> new Configuration(
                null,
                null,
                filterDefinitions,
                null,
                null,
                virtualClusters,
                null,
                false,
                development,
                null,
                null))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessage("'virtualClusters.vc1.filters' references filters not defined in 'filterDefinitions': [missing]");
    }

    @Test
    void shouldSupportDeprecatedTargetClusterInVirtualClusterConstructor() {
        // Verify that VirtualCluster constructor with deprecated TargetCluster still works
        List<VirtualClusterGateway> gateways = List.of(new VirtualClusterGateway("mygateway",
                new PortIdentifiesNodeIdentificationStrategy(new HostPort("example.com", 3), null, null, null),
                null,
                Optional.empty()));

        TargetCluster targetCluster = new TargetCluster("kafka.example:1234", Optional.empty());

        VirtualCluster vc = new VirtualCluster("test-vc", targetCluster, gateways, false, false, null);

        assertThat(vc.targetCluster()).isNotNull();
        assertThat(vc.targetCluster().bootstrapServers()).isEqualTo("kafka.example:1234");
        assertThat(vc.target()).isNull();
    }
}
