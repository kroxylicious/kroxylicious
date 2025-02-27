/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.SniRoutingClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.HostPort;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("removal")
class SniHostIdentifiesNodeIdentificationStrategyTest {

    private final SniRoutingClusterNetworkAddressConfigProvider service = new SniRoutingClusterNetworkAddressConfigProvider();

    @Test
    void providesDefinition() {
        var strategy = new SniHostIdentifiesNodeIdentificationStrategy(HostPort.parse("boot:1234"), "mybroker-$(nodeId)");
        var definition = strategy.get();
        assertThat(definition.type()).isEqualTo(SniRoutingClusterNetworkAddressConfigProvider.class.getSimpleName());
    }

    @Test
    void canConstructProviderFromDefinition() {
        var bootstrap = HostPort.parse("boot:1234");
        var strategy = new SniHostIdentifiesNodeIdentificationStrategy(bootstrap, "mybroker-$(nodeId)");
        var definition = strategy.get();

        var provider = service.build((SniRoutingClusterNetworkAddressConfigProvider.SniRoutingClusterNetworkAddressConfigProviderConfig) definition.config());
        assertThat(provider.getClusterBootstrapAddress()).isEqualTo(bootstrap);
    }
}
