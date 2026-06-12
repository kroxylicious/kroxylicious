/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.model;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.proxy.config.CacheConfiguration;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.service.NodeIdentificationStrategy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class VirtualClusterGatewayModelTest {

    private static final String CLUSTER_NAME = "test-cluster";
    private static final String GATEWAY_NAME = "test-gateway";
    private static final HostPort BOOTSTRAP_NON_ZERO = HostPort.parse("boot.kafka:9192");
    private static final HostPort BOOTSTRAP_PORT_ZERO = HostPort.parse("boot.kafka:0");

    @Mock
    private NodeIdentificationStrategy strategy;

    private VirtualClusterModel makeVirtualClusterModel() {
        var targetCluster = new TargetCluster("upstream:9092", Optional.empty());
        return new VirtualClusterModel(CLUSTER_NAME, targetCluster, false, false, List.of(),
                CacheConfiguration.DEFAULT, null, Duration.ofSeconds(10));
    }

    private VirtualClusterModel.VirtualClusterGatewayModel makeGatewayModel(HostPort bootstrap) {
        when(strategy.getClusterBootstrapAddress()).thenReturn(bootstrap);
        return new VirtualClusterModel.VirtualClusterGatewayModel(makeVirtualClusterModel(), strategy, Optional.empty(), GATEWAY_NAME);
    }

    @Test
    void constructionWithPortZeroSucceeds() {
        // Port 0 is accepted — this exercises the warning log path without asserting on log output
        assertThat(makeGatewayModel(BOOTSTRAP_PORT_ZERO)).isNotNull();
    }

    @Test
    void advertisedBrokerAddressReturnsDelegateAddressForNonZeroPort() {
        var brokerAddress = HostPort.parse("broker-0.kafka:9192");
        when(strategy.getAdvertisedBrokerAddress(0)).thenReturn(brokerAddress);
        var model = makeGatewayModel(BOOTSTRAP_NON_ZERO);

        assertThat(model.getAdvertisedBrokerAddress(0)).isEqualTo(brokerAddress);
    }

    @Test
    void advertisedBrokerAddressThrowsWhenPortZeroAndNotYetResolved() {
        when(strategy.getAdvertisedBrokerAddress(0)).thenReturn(HostPort.parse("broker-0.kafka:0"));
        var model = makeGatewayModel(BOOTSTRAP_PORT_ZERO);

        assertThatThrownBy(() -> model.getAdvertisedBrokerAddress(0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("OS-assigned port has not been resolved yet");
    }

    @Test
    void advertisedBrokerAddressSubstitutesActualPortAfterResolve() {
        when(strategy.getAdvertisedBrokerAddress(0)).thenReturn(HostPort.parse("broker-0.kafka:0"));
        var model = makeGatewayModel(BOOTSTRAP_PORT_ZERO);
        model.resolveActualPort(54321);

        assertThat(model.getAdvertisedBrokerAddress(0)).isEqualTo(HostPort.parse("broker-0.kafka:54321"));
    }

    @Test
    void getBrokerIdFromBrokerAddressDelegatesToStrategyForNonZeroPort() {
        var brokerAddress = HostPort.parse("broker-0.kafka:9192");
        when(strategy.getBrokerIdFromBrokerAddress(brokerAddress)).thenReturn(0);
        var model = makeGatewayModel(BOOTSTRAP_NON_ZERO);

        assertThat(model.getBrokerIdFromBrokerAddress(brokerAddress)).isEqualTo(0);
    }

    @Test
    void getBrokerIdFromBrokerAddressNormalizesActualPortToZeroBeforeDelegating() {
        var brokerAddressWithActualPort = HostPort.parse("broker-0.kafka:54321");
        var brokerAddressNormalized = HostPort.parse("broker-0.kafka:0");
        when(strategy.getBrokerIdFromBrokerAddress(brokerAddressNormalized)).thenReturn(0);
        var model = makeGatewayModel(BOOTSTRAP_PORT_ZERO);
        model.resolveActualPort(54321);

        assertThat(model.getBrokerIdFromBrokerAddress(brokerAddressWithActualPort)).isEqualTo(0);
    }

    @Test
    void getBrokerIdFromBrokerAddressDoesNotNormalizeWhenActualPortNotYetResolved() {
        var brokerAddress = HostPort.parse("broker-0.kafka:54321");
        when(strategy.getBrokerIdFromBrokerAddress(brokerAddress)).thenReturn(null);
        var model = makeGatewayModel(BOOTSTRAP_PORT_ZERO);
        // resolveActualPort not called

        assertThat(model.getBrokerIdFromBrokerAddress(brokerAddress)).isNull();
    }
}
