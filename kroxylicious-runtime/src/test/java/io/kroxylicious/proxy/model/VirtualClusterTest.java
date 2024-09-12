/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.model;

import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.config.tls.InsecureTls;
import io.kroxylicious.proxy.config.tls.KeyPair;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.config.tls.TlsTestConstants;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.PortPerBrokerClusterNetworkAddressConfigProvider;

import static io.kroxylicious.proxy.service.HostPort.parse;
import static org.assertj.core.api.Assertions.assertThat;

class VirtualClusterTest {

    @Test
    void shouldBuildSslContext() {
        // Given
        final KeyPair keyPair = new KeyPair(
                TlsTestConstants.getResourceLocationOnFilesystem("server.key"),
                TlsTestConstants.getResourceLocationOnFilesystem("server.crt"),
                null
        );
        final Optional<Tls> tls = Optional.of(
                new Tls(
                        keyPair,
                        new InsecureTls(false)
                )
        );
        final PortPerBrokerClusterNetworkAddressConfigProvider.PortPerBrokerClusterNetworkAddressConfigProviderConfig clusterNetworkAddressConfigProviderConfig = new PortPerBrokerClusterNetworkAddressConfigProvider.PortPerBrokerClusterNetworkAddressConfigProviderConfig(
                parse("localhost:1235"),
                "localhost",
                19092,
                0,
                1
        );
        final PortPerBrokerClusterNetworkAddressConfigProvider clusterNetworkAddressConfigProvider = new PortPerBrokerClusterNetworkAddressConfigProvider(
                clusterNetworkAddressConfigProviderConfig
        );

        // When

        final VirtualCluster virtualCluster = new VirtualCluster(
                "wibble",
                new TargetCluster("bootstrap:9092", tls),
                clusterNetworkAddressConfigProvider,
                tls,
                false,
                false
        );

        // Then
        assertThat(virtualCluster).isNotNull().extracting("upstreamSslContext").isNotNull();
    }
}
