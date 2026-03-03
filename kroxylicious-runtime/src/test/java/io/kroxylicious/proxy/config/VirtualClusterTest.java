/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.common.base.Strings;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

@ExtendWith(MockitoExtension.class)
class VirtualClusterTest {

    private static final List<String> NO_FILTERS = List.of();
    @Mock
    TargetCluster targetCluster;

    @Mock
    PortIdentifiesNodeIdentificationStrategy portIdentifiesNode1;
    @Mock
    PortIdentifiesNodeIdentificationStrategy portIdentifiesNode2;

    @Test
    void supportsMultipleGateways() {
        // Given
        var gateways = List.of(new VirtualClusterGateway("mygateway1", portIdentifiesNode1, null, Optional.empty()),
                new VirtualClusterGateway("mygateway2", portIdentifiesNode2, null, Optional.empty()));

        // When
        var vc = new VirtualCluster("mycluster", targetCluster, gateways, false, false, NO_FILTERS);

        // Then
        assertThat(vc.gateways())
                .hasSize(2)
                .isEqualTo(gateways);
    }

    @Test
    void disallowMissingGateways() {
        // Given/When/Then
        assertThatThrownBy(() -> new VirtualCluster("mycluster", targetCluster, null, false, false, NO_FILTERS))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    @Test
    void disallowNoGateways() {
        // Given
        var noGateways = List.<VirtualClusterGateway> of();
        // When/Then
        assertThatThrownBy(() -> new VirtualCluster("mycluster", targetCluster, noGateways, false, false, NO_FILTERS))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    @Test
    void disallowsGatewaysWithDuplicateNames() {
        // Given
        var gateways = List.of(new VirtualClusterGateway("dup", portIdentifiesNode1, null, Optional.empty()),
                new VirtualClusterGateway("dup", portIdentifiesNode2, null, Optional.empty()));
        // When/Then
        assertThatThrownBy(() -> new VirtualCluster("mycluster", targetCluster, gateways, false, false, NO_FILTERS))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("Gateway names for a virtual cluster must be unique. The following gateway names are duplicated: [dup]");
    }

    public static Stream<Arguments> rejectVirtualClusterNamesThatArentDnsLabels() {
        return Stream.of(argumentSet("hyphen at start", "-cluster"),
                argumentSet("hyphen at end", "cluster-"),
                argumentSet("hyphen at start and end", "-cluster-"),
                argumentSet("too long", Strings.repeat("a", 64)));
    }

    @ParameterizedTest
    @MethodSource
    void rejectVirtualClusterNamesThatArentDnsLabels(String clusterName) {
        // Given
        var gateways = List.of(new VirtualClusterGateway("mygateway1", portIdentifiesNode1, null, Optional.empty()));

        // When
        // Then
        assertThatThrownBy(() -> {
            new VirtualCluster(clusterName, targetCluster, gateways, false, false, NO_FILTERS);
        }).isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("Virtual cluster name '" + clusterName
                        + "' is invalid. It must be less than 64 characters long and match pattern ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ (case insensitive)");
    }

    public static Stream<Arguments> acceptVirtualClusterNamesThatAreDnsLabels() {
        return Stream.of(argumentSet("start with number", "1a"),
                argumentSet("start with alpha", "a1"),
                argumentSet("start with uppercase alpha", "A1"),
                argumentSet("alphabetical only", "a"),
                argumentSet("uppercase alphabetical only", "A"),
                argumentSet("contains hyphen", "a-b"),
                argumentSet("max length", Strings.repeat("a", 63)));
    }

    @ParameterizedTest
    @MethodSource
    void acceptVirtualClusterNamesThatAreDnsLabels(String clusterName) {
        // Given
        var gateways = List.of(new VirtualClusterGateway("mygateway1", portIdentifiesNode1, null, Optional.empty()));

        // When
        // Then
        assertThatCode(() -> {
            new VirtualCluster(clusterName, targetCluster, gateways, false, false, NO_FILTERS);
        }).doesNotThrowAnyException();
    }
}
