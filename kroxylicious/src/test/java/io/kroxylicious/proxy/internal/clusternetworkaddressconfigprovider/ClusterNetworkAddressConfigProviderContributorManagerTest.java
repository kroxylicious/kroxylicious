/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.PortPerBrokerClusterNetworkAddressConfigProvider.PortPerBrokerClusterNetworkAddressConfigProviderConfig;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.SniRoutingClusterNetworkAddressConfigProvider.SniRoutingClusterNetworkAddressConfigProviderConfig;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.HostPort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterNetworkAddressConfigProviderContributorManagerTest {
    @Test
    void testNonExistentConfigType() {
        assertThatThrownBy(() -> getConfigType("nonexist")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testNonExistentGetInstance() {
        BaseConfig config = new BaseConfig();
        assertThatThrownBy(() -> getInstance("nonexist", config)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testGetConfigForPortPerBroker() {
        assertThat(getConfigType("PortPerBroker")).isEqualTo(PortPerBrokerClusterNetworkAddressConfigProviderConfig.class);
    }

    @Test
    void testGetConfigForSniRouting() {
        assertThat(getConfigType("SniRouting")).isEqualTo(SniRoutingClusterNetworkAddressConfigProviderConfig.class);
    }

    @Test
    void testGetInstanceForPortPerBroker() {
        assertThat(getInstance("PortPerBroker",
                new PortPerBrokerClusterNetworkAddressConfigProviderConfig(HostPort.parse("localhost:8080"), "brokerpattern.com", 9092, 3))).isInstanceOf(
                        PortPerBrokerClusterNetworkAddressConfigProvider.class);
    }

    @Test
    void testGetInstanceForSniRouting() {
        assertThat(getInstance("SniRouting",
                new SniRoutingClusterNetworkAddressConfigProviderConfig(HostPort.parse("localhost:8080"), "$(nodeId)-brokerpattern.com"))).isInstanceOf(
                        SniRoutingClusterNetworkAddressConfigProvider.class);
    }

    private ClusterNetworkAddressConfigProvider getInstance(String shortName, BaseConfig config) {
        return ClusterNetworkAddressConfigProviderContributorManager.getInstance().getInstance(shortName, config);
    }

    private static Class<? extends BaseConfig> getConfigType(String shortName) {
        return ClusterNetworkAddressConfigProviderContributorManager.getInstance().getConfigType(shortName);
    }

}