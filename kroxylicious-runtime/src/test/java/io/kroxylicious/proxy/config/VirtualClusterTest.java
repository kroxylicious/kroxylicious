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
        var vc = new VirtualCluster(targetCluster, provider, Optional.empty(), null, false, false, NO_FILTERS);

        // Then
        assertThat(vc.clusterNetworkAddressConfigProvider()).isEqualTo(provider);
    }

    @Test
    void supportsMultipleListeners() {
        // Given
        var listeners = List.of(new VirtualClusterListener("mylistener1", portIdentifiesNode1, null, Optional.empty()),
                new VirtualClusterListener("mylistener2", portIdentifiesNode2, null, Optional.empty()));

        // When
        var vc = new VirtualCluster(targetCluster, null, null, listeners, false, false, NO_FILTERS);

        // Then
        assertThat(vc.listeners())
                .hasSize(2)
                .isEqualTo(listeners);
    }

    @Test
    void disallowsListenersAndDeprecatedConfigProvider() {
        // Given
        var listeners = List.of(new VirtualClusterListener("mylistener", portIdentifiesNode1, null, Optional.empty()));

        // When/Then
        assertThatThrownBy(() -> new VirtualCluster(targetCluster, provider, null, listeners, false, false, NO_FILTERS))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    @Test
    void disallowsListenersAndDeprecatedTls() {
        // Given
        var listeners = List.of(new VirtualClusterListener("mylistener", portIdentifiesNode1, null, Optional.empty()));
        var tls = Optional.of(new Tls(null, null, null, null));

        // When/Then
        assertThatThrownBy(() -> new VirtualCluster(targetCluster, null, tls, listeners, false, false, NO_FILTERS))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    @Test
    void disallowMissingListeners() {
        // Given/When/Then
        assertThatThrownBy(() -> new VirtualCluster(targetCluster, null, null, null, false, false, NO_FILTERS))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    @Test
    void disallowNoListeners() {
        // Given
        var noListeners = List.<VirtualClusterListener> of();
        // When/Then
        assertThatThrownBy(() -> new VirtualCluster(targetCluster, null, null, noListeners, false, false, NO_FILTERS))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    @Test
    void disallowsListenersWithDuplicateNames() {
        // Given
        var listeners = List.of(new VirtualClusterListener("dup", portIdentifiesNode1, null, Optional.empty()),
                new VirtualClusterListener("dup", portIdentifiesNode2, null, Optional.empty()));
        // When/Then
        assertThatThrownBy(() -> new VirtualCluster(targetCluster, null, null, listeners, false, false, NO_FILTERS))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("Listener names for a virtual cluster must be unique. The following listener names are duplicated: [dup]");
    }
}
