/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.RangeAwarePortPerNodeClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.RangeAwarePortPerNodeClusterNetworkAddressConfigProvider.RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SuppressWarnings("removal")
class PortIdentifiesNodeIdentificationStrategyTest {

    private final RangeAwarePortPerNodeClusterNetworkAddressConfigProvider service = new RangeAwarePortPerNodeClusterNetworkAddressConfigProvider();

    @Test
    void providesDefinition() {
        var strategy = new PortIdentifiesNodeIdentificationStrategy(HostPort.parse("boot:1234"), "mybroker", null, null);
        var definition = strategy.get();
        assertThat(definition.type()).isEqualTo(RangeAwarePortPerNodeClusterNetworkAddressConfigProvider.class.getSimpleName());
    }

    @Test
    void canConstructProviderFromDefinition() {
        var bootstrap = HostPort.parse("boot:1234");
        var strategy = new PortIdentifiesNodeIdentificationStrategy(bootstrap, "mybroker", null, null);
        var provider = buildProvider(strategy);
        assertThat(provider.getClusterBootstrapAddress()).isEqualTo(bootstrap);
    }

    @Test
    void providesDefaultRange() {
        var bootstrap = HostPort.parse("boot:1234");
        var strategy = new PortIdentifiesNodeIdentificationStrategy(bootstrap, "mybroker", null, null);
        var provider = buildProvider(strategy);
        assertThat(provider.getBrokerAddress(0)).isEqualTo(HostPort.parse("mybroker:1235"));
        assertThat(provider.getBrokerAddress(1)).isEqualTo(HostPort.parse("mybroker:1236"));
        assertThat(provider.getBrokerAddress(2)).isEqualTo(HostPort.parse("mybroker:1237"));
        assertThatThrownBy(() -> provider.getBrokerAddress(3))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void respectsSingleRange() {
        var bootstrap = HostPort.parse("boot:1234");
        var strategy = new PortIdentifiesNodeIdentificationStrategy(bootstrap, "mybroker", null, List.of(new NamedRange("foo", 10, 11)));
        var provider = buildProvider(strategy);

        assertThat(provider.getBrokerAddress(10)).isEqualTo(HostPort.parse("mybroker:1235"));
        assertThat(provider.getBrokerAddress(11)).isEqualTo(HostPort.parse("mybroker:1236"));

        assertThatThrownBy(() -> provider.getBrokerAddress(9))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> provider.getBrokerAddress(12))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void respectsStartPort() {
        var bootstrap = HostPort.parse("boot:1234");
        var strategy = new PortIdentifiesNodeIdentificationStrategy(bootstrap, "mybroker", 1240, null);
        var provider = buildProvider(strategy);
        assertThat(provider.getAdvertisedBrokerAddress(0)).isEqualTo(HostPort.parse("mybroker:1240"));
    }

    @NonNull
    private ClusterNetworkAddressConfigProvider buildProvider(PortIdentifiesNodeIdentificationStrategy strategy) {
        var definition = strategy.get();
        return service.build(
                ((RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig) definition.config()), Mockito.mock(VirtualClusterModel.class));
    }

}
