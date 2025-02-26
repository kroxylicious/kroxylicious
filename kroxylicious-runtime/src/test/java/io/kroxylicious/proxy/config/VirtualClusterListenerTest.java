/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.proxy.config.tls.Tls;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class VirtualClusterListenerTest {

    @Mock
    PortIdentifiesNodeIdentificationStrategy portIdentifiesNode;

    @Mock
    SniHostIdentifiesNodeIdentificationStrategy sniHostIdentifiesNode;

    @Mock
    ClusterNetworkAddressConfigProviderDefinition providerDefinition;

    @Test
    void rejectsMissing() {
        var empty = Optional.<Tls> empty();

        assertThatThrownBy(() -> new VirtualClusterListener("name", null, null, empty))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    @Test
    void rejectsBoth() {
        var empty = Optional.<Tls> empty();

        assertThatThrownBy(() -> new VirtualClusterListener("name", portIdentifiesNode, sniHostIdentifiesNode, empty))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    @Test
    void rejectsSniHostIdentifiesNodeWithoutTls() {
        var empty = Optional.<Tls> empty();

        assertThatThrownBy(() -> new VirtualClusterListener("name", null, sniHostIdentifiesNode, empty))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    @Test
    void acceptsPortIdentifiesNode() {
        var empty = Optional.<Tls> empty();

        var listener = new VirtualClusterListener("name", portIdentifiesNode, null, empty);
        assertThat(listener).isNotNull();
    }

    @Test
    void acceptsSniHostIdentifiesNode() {
        var tls = Optional.of(new Tls(null, null, null, null));

        var listener = new VirtualClusterListener("name", null, sniHostIdentifiesNode, tls);
        assertThat(listener).isNotNull();
    }

    @Test
    void createsPortIdentifiesProvider() {
        when(portIdentifiesNode.get()).thenReturn(providerDefinition);
        var listener = new VirtualClusterListener("name", portIdentifiesNode, null, Optional.empty());
        var provider = listener.clusterNetworkAddressConfigProvider();
        assertThat(provider).isEqualTo(providerDefinition);
    }

    @Test
    void createsSniHostIdentifiesProvider() {
        var tls = Optional.of(new Tls(null, null, null, null));

        when(sniHostIdentifiesNode.get()).thenReturn(providerDefinition);
        var listener = new VirtualClusterListener("name", null, sniHostIdentifiesNode, tls);
        var provider = listener.clusterNetworkAddressConfigProvider();
        assertThat(provider).isEqualTo(providerDefinition);
    }
}
