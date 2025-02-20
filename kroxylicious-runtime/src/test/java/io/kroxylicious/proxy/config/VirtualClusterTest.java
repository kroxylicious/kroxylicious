/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.List;
import java.util.Map;
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
    ClusterNetworkAddressConfigProviderDefinition provider1;

    @Mock
    ClusterNetworkAddressConfigProviderDefinition provider2;

    @Test
    void supportsDeprecatedConfigProviderTreatedAsSingletonListener() {
        // Given/When
        var vc = new VirtualCluster(targetCluster, provider1, Optional.empty(), null, false, false, NO_FILTERS);

        // Then
        assertThat(vc.listeners())
                .hasSize(1)
                .hasEntrySatisfying("default", vcl -> {
                    assertThat(vcl).isEqualTo(new VirtualClusterListener(provider1, Optional.empty()));
                });
    }

    @Test
    void supportsListeners() {
        // Given
        var listeners = Map.of("mylistener1", new VirtualClusterListener(provider1, Optional.empty()),
                "mylistener2", new VirtualClusterListener(provider2, Optional.empty()));

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
        var listeners = Map.of("mylistener", new VirtualClusterListener(provider1, Optional.empty()));

        // When/Then
        assertThatThrownBy(() -> new VirtualCluster(targetCluster, provider2, null, listeners, false, false, NO_FILTERS))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    @Test
    void disallowsListenersAndDeprecatedTls() {
        // Given
        var listeners = Map.of("mylistener", new VirtualClusterListener(provider1, Optional.empty()));
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
        var noListeners = Map.<String, VirtualClusterListener> of();
        // When/Then
        assertThatThrownBy(() -> new VirtualCluster(targetCluster, null, null, noListeners, false, false, NO_FILTERS))
                .isInstanceOf(IllegalConfigurationException.class);
    }
}
