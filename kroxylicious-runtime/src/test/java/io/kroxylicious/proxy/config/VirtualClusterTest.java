/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.proxy.config.tls.Tls;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(MockitoExtension.class)
class VirtualClusterTest {

    private static final List<String> NO_FILTERS = List.of();
    @Mock
    TargetCluster targetCluster;

    @Mock
    PortIdentifiesNodeIdentificationStrategy portIdentifiesNode1;
    @Mock
    PortIdentifiesNodeIdentificationStrategy portIdentifiesNode2;

    @Mock
    ClusterNetworkAddressConfigProviderDefinition provider;

    @Test
    @SuppressWarnings("removal")
    void supportsDeprecatedConfigProvider() {
        // Given/When
        var vc = new VirtualCluster("mycluster", targetCluster, provider, Optional.empty(), null, false, false, NO_FILTERS);

        // Then
        assertThat(vc.clusterNetworkAddressConfigProvider()).isEqualTo(provider);
    }

    @Test
    void supportsMultipleGateways() {
        // Given
        var gateways = List.of(new VirtualClusterGateway("mygateway1", portIdentifiesNode1, null, Optional.empty()),
                new VirtualClusterGateway("mygateway2", portIdentifiesNode2, null, Optional.empty()));

        // When
        var vc = new VirtualCluster("mycluster", targetCluster, null, null, gateways, false, false, NO_FILTERS);

        // Then
        assertThat(vc.gateways())
                .hasSize(2)
                .isEqualTo(gateways);
    }

    @Test
    void disallowsGatewaysAndDeprecatedConfigProvider() {
        // Given
        var gateways = List.of(new VirtualClusterGateway("mygateway", portIdentifiesNode1, null, Optional.empty()));

        // When/Then
        assertThatThrownBy(() -> new VirtualCluster("mycluster", targetCluster, provider, null, gateways, false, false, NO_FILTERS))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    @Test
    void disallowsGatewaysAndDeprecatedTls() {
        // Given
        var gateways = List.of(new VirtualClusterGateway("mygateway", portIdentifiesNode1, null, Optional.empty()));
        var tls = Optional.of(new Tls(null, null, null, null));

        // When/Then
        assertThatThrownBy(() -> new VirtualCluster("mycluster", targetCluster, null, tls, gateways, false, false, NO_FILTERS))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    @Test
    void disallowMissingGateways() {
        // Given/When/Then
        assertThatThrownBy(() -> new VirtualCluster("mycluster", targetCluster, null, null, null, false, false, NO_FILTERS))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    @Test
    void disallowNoGateways() {
        // Given
        var noGateways = List.<VirtualClusterGateway> of();
        // When/Then
        assertThatThrownBy(() -> new VirtualCluster("mycluster", targetCluster, null, null, noGateways, false, false, NO_FILTERS))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    @Test
    void disallowsGatewaysWithDuplicateNames() {
        // Given
        var gateways = List.of(new VirtualClusterGateway("dup", portIdentifiesNode1, null, Optional.empty()),
                new VirtualClusterGateway("dup", portIdentifiesNode2, null, Optional.empty()));
        // When/Then
        assertThatThrownBy(() -> new VirtualCluster("mycluster", targetCluster, null, null, gateways, false, false, NO_FILTERS))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("Gateway names for a virtual cluster must be unique. The following gateway names are duplicated: [dup]");
    }
}
