/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.SniRoutingClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.service.HostPort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@SuppressWarnings("removal")
class SniHostIdentifiesNodeIdentificationStrategyTest {

    private final SniRoutingClusterNetworkAddressConfigProvider service = new SniRoutingClusterNetworkAddressConfigProvider();

    @Test
    void providesDefinition() {
        var strategy = new SniHostIdentifiesNodeIdentificationStrategy("boot:1234", "mybroker-$(nodeId)");
        var definition = strategy.get();
        assertThat(definition.type()).isEqualTo(SniRoutingClusterNetworkAddressConfigProvider.class.getSimpleName());
    }

    @Test
    void canConstructProviderFromDefinition() {
        var bootstrap = HostPort.parse("boot:1234");
        var strategy = new SniHostIdentifiesNodeIdentificationStrategy(bootstrap.toString(), "mybroker-$(nodeId)");
        var definition = strategy.get();

        VirtualClusterModel mock = Mockito.mock(
                VirtualClusterModel.class);
        when(mock.getClusterName()).thenReturn("my-cluster");
        var provider = service.build((SniRoutingClusterNetworkAddressConfigProvider.SniRoutingClusterNetworkAddressConfigProviderConfig) definition.config(),
                mock);
        assertThat(provider.getClusterBootstrapAddress()).isEqualTo(bootstrap);
    }

    @Test
    void canConstructProviderFromDefinitionWithClusterNameReplacementToken() {
        String virtualClusterName = "my-cluster";
        var bootstrap = HostPort.parse("boot:1234");
        var strategy = new SniHostIdentifiesNodeIdentificationStrategy(bootstrap.toString(), "my-broker-$(virtualClusterName)-$(nodeId)");
        var definition = strategy.get();

        VirtualClusterModel mock = Mockito.mock(
                VirtualClusterModel.class);
        when(mock.getClusterName()).thenReturn(virtualClusterName);
        var provider = service.build((SniRoutingClusterNetworkAddressConfigProvider.SniRoutingClusterNetworkAddressConfigProviderConfig) definition.config(),
                mock);
        assertThat(provider.getClusterBootstrapAddress()).isEqualTo(bootstrap);
        assertThat(provider.getBrokerAddress(1)).isEqualTo(HostPort.parse("my-broker-my-cluster-1:1234"));
        assertThat(provider.getBrokerIdFromBrokerAddress(HostPort.parse("my-broker-my-cluster-1:1234"))).isEqualTo(1);
        assertThat(provider.getBrokerIdFromBrokerAddress(HostPort.parse("my-broker-another-cluster-1:1234"))).isNull();
        assertThat(provider.getAdvertisedBrokerAddress(1)).isEqualTo(HostPort.parse("my-broker-my-cluster-1:1234"));
    }
}
