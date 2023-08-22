/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.ContributorContext;

import static io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.TestClusterNetworkAddressConfigProviderContributor.SHORT_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterNetworkAddressConfigProviderContributorManagerTest {

    public static final ClusterNetworkAddressConfigProviderContributorManager INSTANCE = ClusterNetworkAddressConfigProviderContributorManager.getInstance();

    @Test
    void testGetConfigType() {
        Class<? extends BaseConfig> clazz = INSTANCE.getConfigType(SHORT_NAME);
        assertThat(clazz).isEqualTo(Config.class);
    }

    @Test
    void testGetConfigTypeFailsWhenShortnameHasNoMatches() {
        assertThatThrownBy(() -> INSTANCE.getConfigType("mismatch")).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No endpoint provider found for name 'mismatch'");
    }

    @Test
    void testGetClusterEndpointProvider() {
        Config config = new Config();
        ClusterNetworkAddressConfigProvider provider = INSTANCE.getClusterEndpointConfigProvider(SHORT_NAME, config);
        assertThat(provider).isInstanceOf(TestClusterNetworkAddressConfigProvider.class);
        TestClusterNetworkAddressConfigProvider testProvider = (TestClusterNetworkAddressConfigProvider) provider;
        assertThat(testProvider.config()).isSameAs(config);
        assertThat(testProvider.shortName()).isEqualTo(SHORT_NAME);
        assertThat(testProvider.context()).isEqualTo(ContributorContext.instance());
    }

    @Test
    void testGetClusterEndpointProviderFailsWhenShortnameHasNoMatches() {
        Config config = new Config();
        assertThatThrownBy(() -> INSTANCE.getClusterEndpointConfigProvider("mismatch", config))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("No endpoint provider found for name 'mismatch'");
    }

}