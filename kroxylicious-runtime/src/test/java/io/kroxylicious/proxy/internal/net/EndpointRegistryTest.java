/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.converter.ConvertWith;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

import io.kroxylicious.proxy.HostPortConverter;
import io.kroxylicious.proxy.bootstrap.BootstrapSelectionStrategy;
import io.kroxylicious.proxy.bootstrap.FixedBootstrapSelectionStrategy;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.service.HostPort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mock.Strictness.LENIENT;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EndpointRegistryTest {
    private static final HostPort DOWNSTREAM_BOOTSTRAP = HostPort.parse("downstream-bootstrap:9192");
    private static final HostPort DOWNSTREAM_BOOTSTRAP_DIFF_SNI = new HostPort(DOWNSTREAM_BOOTSTRAP.host() + ".diff.sni", DOWNSTREAM_BOOTSTRAP.port());
    private static final HostPort DOWNSTREAM_BOOTSTRAP_DIFF_PORT = new HostPort(DOWNSTREAM_BOOTSTRAP.host(), DOWNSTREAM_BOOTSTRAP.port() + 1);
    private static final HostPort DOWNSTREAM_BROKER_0 = HostPort.parse("downstream-broker0:9193");
    private static final HostPort DOWNSTREAM_BROKER_1 = HostPort.parse("downstream-broker1:9194");
    private static final String SNI_DOMAIN_SUFFIX = ".cluster.kafka.com";
    private static final HostPort SNI_DOWNSTREAM_BOOTSTRAP = new HostPort("bootstrap" + SNI_DOMAIN_SUFFIX, 9192);
    private static final HostPort SNI_DOWNSTREAM_BROKER_0 = new HostPort("broker-0" + SNI_DOMAIN_SUFFIX, SNI_DOWNSTREAM_BOOTSTRAP.port());
    private static final HostPort SNI_DOWNSTREAM_BROKER_1 = new HostPort("broker-1" + SNI_DOMAIN_SUFFIX, SNI_DOWNSTREAM_BOOTSTRAP.port());
    private static final HostPort UPSTREAM_BOOTSTRAP = HostPort.parse("upstream-bootstrap:19192");
    private static final HostPort UPSTREAM_BROKER_0 = HostPort.parse("upstream-broker0:19193");
    private static final HostPort UPSTREAM_BROKER_1 = HostPort.parse("upstream-broker1:19194");
    private final TestNetworkBindingOperationProcessor bindingOperationProcessor = new TestNetworkBindingOperationProcessor();
    private final EndpointRegistry endpointRegistry = new EndpointRegistry(bindingOperationProcessor);
    private final Map<Endpoint, Channel> acceptorChannels = new java.util.concurrent.ConcurrentHashMap<>();
    @Mock(strictness = LENIENT)
    private EndpointGateway virtualClusterModel1;
    @Mock(strictness = LENIENT)
    private EndpointGateway virtualClusterModel2;
    private final ProxyNodeId.Bootstrap vc1BootstrapNodeId = new ProxyNodeId.Bootstrap(virtualClusterModel1);

    @Test
    void registerVirtualCluster() throws Exception {
        configureVirtualClusterMock(virtualClusterModel1, DOWNSTREAM_BOOTSTRAP, UPSTREAM_BOOTSTRAP, false);

        var rf = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BOOTSTRAP.port(), false));
        verifyVirtualClusterRegisterFuture(DOWNSTREAM_BOOTSTRAP.port(), false, rf);

        assertThat(endpointRegistry.isRegistered(virtualClusterModel1)).isTrue();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);
    }

    @Test
    void registerVirtualClusterWithDiscoveryAddresses() throws Exception {
        configureVirtualClusterMock(virtualClusterModel1, DOWNSTREAM_BOOTSTRAP, UPSTREAM_BOOTSTRAP, false, false,
                Map.of(0, DOWNSTREAM_BROKER_0, 1, DOWNSTREAM_BROKER_1), new FixedBootstrapSelectionStrategy(0));

        var rf = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BOOTSTRAP.port(), false),
                createTestNetworkBindRequest(DOWNSTREAM_BROKER_0.port(), false),
                createTestNetworkBindRequest(DOWNSTREAM_BROKER_1.port(), false));
        assertThat(rf).isCompleted();

        verifyVirtualClusterRegisterFuture(DOWNSTREAM_BOOTSTRAP.port(), false, rf);

        assertThat(endpointRegistry.isRegistered(virtualClusterModel1)).isTrue();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(3);
    }

    @Test
    void registerVirtualClusterTls() throws Exception {
        configureVirtualClusterMock(virtualClusterModel1, DOWNSTREAM_BOOTSTRAP, UPSTREAM_BOOTSTRAP, true);

        var rf = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BOOTSTRAP.port(), true));
        verifyVirtualClusterRegisterFuture(DOWNSTREAM_BOOTSTRAP.port(), true, rf);
    }

    @Test
    void registerSameVirtualClusterIsIdempotent() throws Exception {
        configureVirtualClusterMock(virtualClusterModel1, DOWNSTREAM_BOOTSTRAP, UPSTREAM_BOOTSTRAP, false);

        var rf1 = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        var rf2 = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BOOTSTRAP.port(), false));
        verifyVirtualClusterRegisterFuture(DOWNSTREAM_BOOTSTRAP.port(), false, rf1);
        verifyVirtualClusterRegisterFuture(DOWNSTREAM_BOOTSTRAP.port(), false, rf2);

        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);
    }

    @Test
    void registerTwoClustersThatShareSameNetworkEndpoint() throws Exception {
        // Same port..different SNI
        configureVirtualClusterMock(virtualClusterModel1, DOWNSTREAM_BOOTSTRAP, UPSTREAM_BOOTSTRAP, true);
        configureVirtualClusterMock(virtualClusterModel2, DOWNSTREAM_BOOTSTRAP_DIFF_SNI, UPSTREAM_BOOTSTRAP, true);

        var rf1 = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        var rf2 = endpointRegistry.registerVirtualCluster(virtualClusterModel2).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BOOTSTRAP.port(), true));
        verifyVirtualClusterRegisterFuture(DOWNSTREAM_BOOTSTRAP.port(), true, rf1);
        verifyVirtualClusterRegisterFuture(DOWNSTREAM_BOOTSTRAP.port(), true, rf2);

        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);
    }

    @Test
    void registerTwoClustersThatUsesDistinctNetworkEndpoints() throws Exception {
        configureVirtualClusterMock(virtualClusterModel1, DOWNSTREAM_BOOTSTRAP, UPSTREAM_BOOTSTRAP, false);
        configureVirtualClusterMock(virtualClusterModel2, DOWNSTREAM_BOOTSTRAP_DIFF_PORT, UPSTREAM_BOOTSTRAP, false);

        var rf1 = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        var rf2 = endpointRegistry.registerVirtualCluster(virtualClusterModel2).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BOOTSTRAP.port(), false),
                createTestNetworkBindRequest(DOWNSTREAM_BOOTSTRAP_DIFF_PORT.port(), false));
        verifyVirtualClusterRegisterFuture(DOWNSTREAM_BOOTSTRAP.port(), false, rf1);
        verifyVirtualClusterRegisterFuture(DOWNSTREAM_BOOTSTRAP_DIFF_PORT.port(), false, rf2);

        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(2);
    }

    @Test
    void registerRejectsDuplicatedBinding() throws Exception {
        configureVirtualClusterMock(virtualClusterModel1, DOWNSTREAM_BOOTSTRAP, UPSTREAM_BOOTSTRAP, false);
        configureVirtualClusterMock(virtualClusterModel2, DOWNSTREAM_BOOTSTRAP, UPSTREAM_BOOTSTRAP, false);

        var rf1 = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BOOTSTRAP.port(), false));
        verifyVirtualClusterRegisterFuture(DOWNSTREAM_BOOTSTRAP.port(), false, rf1);
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);

        verifyAndProcessNetworkEventQueue();
        var executionException = assertThrows(ExecutionException.class,
                () -> endpointRegistry.registerVirtualCluster(virtualClusterModel2).toCompletableFuture().get());
        assertThat(executionException).hasCauseInstanceOf(EndpointBindingException.class);

        assertThat(endpointRegistry.isRegistered(virtualClusterModel1)).isTrue();
        assertThat(endpointRegistry.isRegistered(virtualClusterModel2)).isFalse();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);
    }

    @Test
    void registerVirtualClusterFailsDueToExternalPortConflict() {
        configureVirtualClusterMock(virtualClusterModel1, DOWNSTREAM_BOOTSTRAP, UPSTREAM_BOOTSTRAP, false);

        var rf = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(
                createTestNetworkBindRequest(Optional.empty(), DOWNSTREAM_BOOTSTRAP.port(), false,
                        CompletableFuture.failedFuture(new IOException("mocked port in use"))));
        assertThat(rf.isDone()).isTrue();
        var executionException = assertThrows(ExecutionException.class, rf::get);
        assertThat(executionException).hasRootCauseInstanceOf(IOException.class);

        assertThat(endpointRegistry.isRegistered(virtualClusterModel1)).isFalse();
        assertThat(endpointRegistry.listeningChannelCount()).isZero();
    }

    @Test
    void deregisterVirtualCluster() {
        configureVirtualClusterMock(virtualClusterModel1, DOWNSTREAM_BOOTSTRAP, UPSTREAM_BOOTSTRAP, true);

        var rf = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BOOTSTRAP.port(), true));
        assertThat(rf.isDone()).isTrue();

        assertThat(endpointRegistry.isRegistered(virtualClusterModel1)).isTrue();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);

        var df = endpointRegistry.deregisterVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkUnbindRequest(DOWNSTREAM_BOOTSTRAP.port(), true));
        assertThat(df.isDone()).isTrue();

        assertThat(endpointRegistry.isRegistered(virtualClusterModel1)).isFalse();
        assertThat(endpointRegistry.listeningChannelCount()).isZero();
    }

    @Test
    void deregisterPortZeroVirtualCluster() {
        int osAssignedPort = 58392;
        var bootstrapAddress = new HostPort("bootstrap.kafka", 0);
        configureVirtualClusterMock(virtualClusterModel1, bootstrapAddress, UPSTREAM_BOOTSTRAP, false);

        var rf = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(
                createTestNetworkBindRequest(Optional.empty(), 0, false, CompletableFuture.completedFuture(createMockNettyChannel(osAssignedPort))));
        assertThat(rf.isDone()).isTrue();

        assertThat(endpointRegistry.isRegistered(virtualClusterModel1)).isTrue();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);

        var df = endpointRegistry.deregisterVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkUnbindRequest(osAssignedPort, false));
        assertThat(df.isDone()).isTrue();

        assertThat(endpointRegistry.isRegistered(virtualClusterModel1)).isFalse();
        assertThat(endpointRegistry.listeningChannelCount()).isZero();
    }

    @Test
    void deregisterPortZeroVirtualClusterWithDiscoveryBrokers() {
        int bootstrapOsPort = 58392;
        int broker0OsPort = 58393;
        int broker1OsPort = 58394;
        var bootstrapAddress = new HostPort("bootstrap.kafka", 0);
        var broker0Address = new HostPort("broker0.kafka", 0);
        var broker1Address = new HostPort("broker1.kafka", 0);
        configureVirtualClusterMock(virtualClusterModel1, bootstrapAddress, UPSTREAM_BOOTSTRAP, false, false,
                Map.of(0, broker0Address, 1, broker1Address), new FixedBootstrapSelectionStrategy(0));

        var rf = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(
                createTestNetworkBindRequest(Optional.empty(), 0, false, CompletableFuture.completedFuture(createMockNettyChannel(bootstrapOsPort))),
                createTestNetworkBindRequest(Optional.empty(), 0, false, CompletableFuture.completedFuture(createMockNettyChannel(broker0OsPort))),
                createTestNetworkBindRequest(Optional.empty(), 0, false, CompletableFuture.completedFuture(createMockNettyChannel(broker1OsPort))));
        assertThat(rf).isDone();

        assertThat(endpointRegistry.isRegistered(virtualClusterModel1)).isTrue();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(3);

        var df = endpointRegistry.deregisterVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(
                createTestNetworkUnbindRequest(bootstrapOsPort, false),
                createTestNetworkUnbindRequest(broker0OsPort, false),
                createTestNetworkUnbindRequest(broker1OsPort, false));
        assertThat(df.isDone()).isTrue();

        assertThat(endpointRegistry.isRegistered(virtualClusterModel1)).isFalse();
        assertThat(endpointRegistry.listeningChannelCount()).isZero();
    }

    @Test
    void registerPortZeroNotifiesBindingSpecOfActualPorts() {
        // Given
        int bootstrapOsPort = 58392;
        int broker0OsPort = 58393;
        int broker1OsPort = 58394;
        var bootstrapAddress = new HostPort("bootstrap.kafka", 0);
        var broker0Address = new HostPort("broker0.kafka", 0);
        var broker1Address = new HostPort("broker1.kafka", 0);
        configureVirtualClusterMock(virtualClusterModel1, bootstrapAddress, UPSTREAM_BOOTSTRAP, false, false,
                Map.of(0, broker0Address, 1, broker1Address), new FixedBootstrapSelectionStrategy(0));

        // When
        endpointRegistry.registerVirtualCluster(virtualClusterModel1);
        verifyAndProcessNetworkEventQueue(
                createTestNetworkBindRequest(Optional.empty(), 0, false, CompletableFuture.completedFuture(createMockNettyChannel(bootstrapOsPort))),
                createTestNetworkBindRequest(Optional.empty(), 0, false, CompletableFuture.completedFuture(createMockNettyChannel(broker0OsPort))),
                createTestNetworkBindRequest(Optional.empty(), 0, false, CompletableFuture.completedFuture(createMockNettyChannel(broker1OsPort))));

        // Then — the binding spec learns the actual OS-assigned port for the bootstrap and each broker
        var bindingSpec = virtualClusterModel1.bindingSpec();
        verify(bindingSpec).registerBoundBootstrapPort(bootstrapOsPort);
        verify(bindingSpec).registerBoundPort(0, broker0OsPort);
        verify(bindingSpec).registerBoundPort(1, broker1OsPort);
    }

    @Test
    void deregisterPortZeroVirtualClusterAfterReconciliation() {
        int bootstrapOsPort = 58392;
        int brokerOsPort = 58393;
        var bootstrapAddress = new HostPort("bootstrap.kafka", 0);
        configureVirtualClusterMock(virtualClusterModel1, bootstrapAddress, UPSTREAM_BOOTSTRAP, false);

        // Register on port 0
        var rf = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(
                createTestNetworkBindRequest(Optional.empty(), 0, false, CompletableFuture.completedFuture(createMockNettyChannel(bootstrapOsPort))));
        assertThat(rf.isDone()).isTrue();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);

        // Reconcile adds a broker binding (also port 0 → new synthetic channel)
        when(virtualClusterModel1.getBrokerAddress(0)).thenReturn(new HostPort("broker0.kafka", 0));
        var recf = endpointRegistry.reconcile(virtualClusterModel1, Map.of(0, UPSTREAM_BROKER_0)).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(
                createTestNetworkBindRequest(Optional.empty(), 0, false, CompletableFuture.completedFuture(createMockNettyChannel(brokerOsPort))));
        assertThat(recf.isDone()).isTrue();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(2);

        // Deregister — should close BOTH channels
        var df = endpointRegistry.deregisterVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(
                createTestNetworkUnbindRequest(bootstrapOsPort, false),
                createTestNetworkUnbindRequest(brokerOsPort, false));
        assertThat(df.isDone()).isTrue();

        assertThat(endpointRegistry.isRegistered(virtualClusterModel1)).isFalse();
        assertThat(endpointRegistry.listeningChannelCount()).isZero();
    }

    @Test
    void deregisterPortZeroThenReRegisterOnExplicitPort() {
        // Simulates ReplaceCluster: deregister the port-0 VC, then re-register on an explicit port.
        int osAssignedPort = 58392;
        var bootstrapAddress = new HostPort("bootstrap.kafka", 0);
        configureVirtualClusterMock(virtualClusterModel1, bootstrapAddress, UPSTREAM_BOOTSTRAP, false);

        // Register on port 0
        var rf = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(
                createTestNetworkBindRequest(Optional.empty(), 0, false, CompletableFuture.completedFuture(createMockNettyChannel(osAssignedPort))));
        assertThat(rf.isDone()).isTrue();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);

        // Deregister (RemoveCluster half)
        var df = endpointRegistry.deregisterVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkUnbindRequest(osAssignedPort, false));
        assertThat(df.isDone()).isTrue();
        assertThat(endpointRegistry.listeningChannelCount()).isZero();

        // Re-register on explicit port (AddCluster half with new model)
        int explicitPort = 9192;
        configureVirtualClusterMock(virtualClusterModel2, new HostPort("bootstrap.kafka", explicitPort), UPSTREAM_BOOTSTRAP, false);
        var rf2 = endpointRegistry.registerVirtualCluster(virtualClusterModel2).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(explicitPort, false));
        assertThat(rf2.isDone()).isTrue();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);
    }

    @Test
    void deregisterSameVirtualClusterIsIdempotent() throws Exception {
        configureVirtualClusterMock(virtualClusterModel1, DOWNSTREAM_BOOTSTRAP, UPSTREAM_BOOTSTRAP, true);

        var rf = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BOOTSTRAP.port(), true));
        verifyVirtualClusterRegisterFuture(DOWNSTREAM_BOOTSTRAP.port(), true, rf);

        var df1 = endpointRegistry.deregisterVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkUnbindRequest(DOWNSTREAM_BOOTSTRAP.port(), true));
        assertThat(df1.isDone()).isTrue();

        var df2 = endpointRegistry.deregisterVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue();
        assertThat(df2.isDone()).isTrue();
    }

    @Test
    void deregisterClusterThatSharesEndpoint() throws Exception {
        configureVirtualClusterMock(virtualClusterModel1, DOWNSTREAM_BOOTSTRAP, UPSTREAM_BOOTSTRAP, true);
        configureVirtualClusterMock(virtualClusterModel2, DOWNSTREAM_BOOTSTRAP_DIFF_SNI, UPSTREAM_BOOTSTRAP, true);

        var rf1 = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        var rf2 = endpointRegistry.registerVirtualCluster(virtualClusterModel2).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BOOTSTRAP.port(), true));
        verifyVirtualClusterRegisterFuture(DOWNSTREAM_BOOTSTRAP.port(), true, rf1);
        verifyVirtualClusterRegisterFuture(DOWNSTREAM_BOOTSTRAP_DIFF_SNI.port(), true, rf2);

        var df1 = endpointRegistry.deregisterVirtualCluster(virtualClusterModel1).toCompletableFuture();
        // Port 9192 is shared by the second virtualcluster, so it can't be unbound yet
        verifyAndProcessNetworkEventQueue();
        assertThat(df1.isDone()).isTrue();

        var df2 = endpointRegistry.deregisterVirtualCluster(virtualClusterModel2).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkUnbindRequest(DOWNSTREAM_BOOTSTRAP.port(), true));
        assertThat(df2.isDone()).isTrue();
    }

    @Test
    void reregisterClusterWhilstDeregisterIsInProgress() throws Exception {
        configureVirtualClusterMock(virtualClusterModel1, DOWNSTREAM_BOOTSTRAP, UPSTREAM_BOOTSTRAP, true);

        var rf1 = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BOOTSTRAP.port(), true));
        assertThat(rf1.isDone()).isTrue();

        var df1 = endpointRegistry.deregisterVirtualCluster(virtualClusterModel1).toCompletableFuture();
        // The de-registration for cluster1 is queued up so the future won't be completed.
        assertThat(df1.isDone()).isFalse();

        var rereg = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        assertThat(rereg.isDone()).isFalse();

        // we expect an unbind for 9192
        verifyAndProcessNetworkEventQueue(createTestNetworkUnbindRequest(DOWNSTREAM_BOOTSTRAP.port(), true));
        assertThat(df1.isDone()).isTrue();

        // followed by an immediate rebind of the same port.
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BOOTSTRAP.port(), true));

        verifyVirtualClusterRegisterFuture(DOWNSTREAM_BOOTSTRAP.port(), true, rereg);
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);
    }

    @Test
    void registerClusterWhileAnotherIsDeregistering() throws Exception {
        configureVirtualClusterMock(virtualClusterModel1, DOWNSTREAM_BOOTSTRAP, UPSTREAM_BOOTSTRAP, true);
        configureVirtualClusterMock(virtualClusterModel2, DOWNSTREAM_BOOTSTRAP_DIFF_SNI, UPSTREAM_BOOTSTRAP, true);

        var rf1 = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BOOTSTRAP.port(), true));
        assertThat(rf1.isDone()).isTrue();

        var df1 = endpointRegistry.deregisterVirtualCluster(virtualClusterModel1).toCompletableFuture();
        // The de-registration for cluster1 is queued up so the future won't be completed.
        assertThat(df1.isDone()).isFalse();

        var rf2 = endpointRegistry.registerVirtualCluster(virtualClusterModel2).toCompletableFuture();
        assertThat(rf2.isDone()).isFalse();

        // we expect an unbind for 9192 followed by an immediate rebind of the same port.
        verifyAndProcessNetworkEventQueue(createTestNetworkUnbindRequest(DOWNSTREAM_BOOTSTRAP.port(), true));
        assertThat(df1.isDone()).isTrue();

        // followed by an immediate rebind of the same port.
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BOOTSTRAP.port(), true));

        verifyVirtualClusterRegisterFuture(DOWNSTREAM_BOOTSTRAP.port(), true, rf2);
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);
    }

    @ParameterizedTest
    @CsvSource({ "mycluster1:9192,upstream1:9192,true,true", "mycluster1:9192,upstream1:9192,true,false", "localhost:9192,upstream1:9192,false,false" })
    void resolveBootstrap(@ConvertWith(HostPortConverter.class) HostPort downstreamBootstrap, @ConvertWith(HostPortConverter.class) HostPort upstreamBootstrap,
                          boolean tls, boolean sni)
            throws Exception {
        configureVirtualClusterMock(virtualClusterModel1, HostPort.parse(downstreamBootstrap.toString()), HostPort.parse(upstreamBootstrap.toString()), tls, sni,
                Map.of(), new FixedBootstrapSelectionStrategy(0));

        var f = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(downstreamBootstrap.port(), tls));
        assertThat(f.isDone()).isTrue();

        var childChannel = createMockChildChannel(acceptorChannels.get(Endpoint.createEndpoint(downstreamBootstrap.port(), tls)));
        var binding = endpointRegistry.resolve(childChannel, tls ? downstreamBootstrap.host() : null).toCompletableFuture().get();
        assertThat(binding).isEqualTo(new BootstrapEndpointBinding(virtualClusterModel1));
    }

    @Test
    void resolveBootstrapResolutionFailsForMismatchingSni() {
        // Given — registered with SNI "mycluster1", resolve with SNI "mycluster2"
        var downstreamBootstrap = HostPort.parse("mycluster1:9192");
        configureVirtualClusterMock(virtualClusterModel1, downstreamBootstrap, UPSTREAM_BOOTSTRAP, true);

        var f = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(downstreamBootstrap.port(), true));
        assertThat(f.isDone()).isTrue();

        // When — resolve with a different SNI hostname on the same acceptor
        var childChannel = createMockChildChannel(acceptorChannels.get(Endpoint.createEndpoint(downstreamBootstrap.port(), true)));
        var executionException = assertThrows(ExecutionException.class,
                () -> endpointRegistry.resolve(childChannel, "mycluster2").toCompletableFuture().get());

        // Then
        assertThat(executionException).hasCauseInstanceOf(EndpointResolutionException.class);
    }

    @Test
    void resolveBootstrapResolutionFailsForChannelWithNoParent() {
        // Given — a channel with no parent (not accepted from an acceptor)
        var orphanChannel = mock(Channel.class);
        when(orphanChannel.parent()).thenReturn(null);

        // When / Then
        var executionException = assertThrows(ExecutionException.class,
                () -> endpointRegistry.resolve(orphanChannel, null).toCompletableFuture().get());
        assertThat(executionException).hasCauseInstanceOf(EndpointResolutionException.class);
    }

    @ParameterizedTest
    @CsvSource({ "mycluster1:9192,upstream1:9192,MyClUsTeR1:9192",
            "69.2.0.192.in-addr.arpa:9192,upstream1:9192,69.2.0.192.in-ADDR.ARPA:9192" })
    void resolveRespectsCaseInsensitivityRfc4343(@ConvertWith(HostPortConverter.class) HostPort downstreamBootstrap,
                                                 @ConvertWith(HostPortConverter.class) HostPort upstreamBootstrap,
                                                 @ConvertWith(HostPortConverter.class) HostPort resolveAddress)
            throws Exception {
        configureVirtualClusterMock(virtualClusterModel1, HostPort.parse(downstreamBootstrap.toString()), HostPort.parse(upstreamBootstrap.toString()), true);

        var f = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(downstreamBootstrap.port(), true));
        assertThat(f.isDone()).isTrue();

        when(virtualClusterModel1.addressingSpec().identify(resolveAddress.port(), resolveAddress.host())).thenReturn(new AddressingSpec.Target.Bootstrap());
        var childChannel = createMockChildChannel(acceptorChannels.get(Endpoint.createEndpoint(resolveAddress.port(), true)));
        var binding = endpointRegistry.resolve(childChannel, resolveAddress.host()).toCompletableFuture().get();
        assertThat(binding).isEqualTo(new BootstrapEndpointBinding(virtualClusterModel1));
    }

    @Test
    void resolveUsingSniMatch() {
        configureVirtualClusterMock(virtualClusterModel1, SNI_DOWNSTREAM_BOOTSTRAP, UPSTREAM_BOOTSTRAP, true);

        var regf = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(SNI_DOWNSTREAM_BOOTSTRAP.port(), true));
        assertThat(regf).isCompleted();

        when(virtualClusterModel1.addressingSpec().identify(SNI_DOWNSTREAM_BOOTSTRAP.port(), SNI_DOWNSTREAM_BROKER_0.host()))
                .thenReturn(new AddressingSpec.Target.Node(0));

        var childChannel = createMockChildChannel(acceptorChannels.get(Endpoint.createEndpoint(SNI_DOWNSTREAM_BOOTSTRAP.port(), true)));
        assertThat(endpointRegistry.resolve(childChannel, SNI_DOWNSTREAM_BROKER_0.host()))
                .succeedsWithin(Duration.ofSeconds(1))
                .isEqualTo(new MetadataDiscoveryBrokerEndpointBinding(virtualClusterModel1, 0));
    }

    @Test
    void resolveUsingSniFailsDueToMismatch() {
        configureVirtualClusterMock(virtualClusterModel1, SNI_DOWNSTREAM_BOOTSTRAP, UPSTREAM_BOOTSTRAP, true);

        var regf = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(SNI_DOWNSTREAM_BOOTSTRAP.port(), true));
        assertThat(regf).isCompleted();

        // SNI_DOWNSTREAM_BROKER_0 is not stubbed, so it falls through to the default NotRecognised()
        var childChannel = createMockChildChannel(acceptorChannels.get(Endpoint.createEndpoint(SNI_DOWNSTREAM_BOOTSTRAP.port(), true)));
        var executionException = assertThrows(ExecutionException.class,
                () -> endpointRegistry.resolve(childChannel, SNI_DOWNSTREAM_BROKER_0.host()).toCompletableFuture()
                        .get());
        assertThat(executionException).hasCauseInstanceOf(EndpointResolutionException.class);
    }

    @Test
    void resolveIgnoresRestrictedSniMatchAfterReconcile() throws Exception {
        configureVirtualClusterMock(virtualClusterModel1, SNI_DOWNSTREAM_BOOTSTRAP, UPSTREAM_BOOTSTRAP, true);

        var regf = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(SNI_DOWNSTREAM_BOOTSTRAP.port(), true));
        assertThat(regf).isCompleted();

        when(virtualClusterModel1.getBrokerAddress(0)).thenReturn(SNI_DOWNSTREAM_BROKER_0);
        var recf = endpointRegistry.reconcile(virtualClusterModel1, Map.of(0, UPSTREAM_BROKER_0)).toCompletableFuture();
        verifyAndProcessNetworkEventQueue();
        assertThat(recf).isCompleted();

        when(virtualClusterModel1.addressingSpec().identify(SNI_DOWNSTREAM_BOOTSTRAP.port(), SNI_DOWNSTREAM_BROKER_0.host()))
                .thenReturn(new AddressingSpec.Target.Node(0));
        var childChannel0 = createMockChildChannel(acceptorChannels.get(Endpoint.createEndpoint(SNI_DOWNSTREAM_BOOTSTRAP.port(), true)));
        var brokerBinding = endpointRegistry.resolve(childChannel0, SNI_DOWNSTREAM_BROKER_0.host()).toCompletableFuture()
                .get();
        assertThat(brokerBinding).isEqualTo(new BrokerEndpointBinding(virtualClusterModel1, UPSTREAM_BROKER_0, 0));

        // Now try to resolve another broker address that matches the pattern, but isn't resolved.
        when(virtualClusterModel1.addressingSpec().identify(SNI_DOWNSTREAM_BOOTSTRAP.port(), SNI_DOWNSTREAM_BROKER_1.host()))
                .thenReturn(new AddressingSpec.Target.Node(1));
        var childChannel1 = createMockChildChannel(acceptorChannels.get(Endpoint.createEndpoint(SNI_DOWNSTREAM_BOOTSTRAP.port(), true)));
        var sniMatchBinding = endpointRegistry.resolve(childChannel1, SNI_DOWNSTREAM_BROKER_1.host())
                .toCompletableFuture()
                .get();
        assertThat(sniMatchBinding).isEqualTo(new MetadataDiscoveryBrokerEndpointBinding(virtualClusterModel1, 1));
    }

    @Test
    void bindingAddressEndpointSeparation() throws Exception {
        var bindingAddress1 = Optional.of("127.0.0.1");
        configureVirtualClusterMock(virtualClusterModel1, HostPort.parse("localhost:9192"), HostPort.parse("upstream1:9192"),
                false, false, Map.of(), new FixedBootstrapSelectionStrategy(0), bindingAddress1);

        var bindingAddress2 = Optional.of("192.168.0.1");
        configureVirtualClusterMock(virtualClusterModel2, HostPort.parse("myhost:9192"), HostPort.parse("upstream2:9192"),
                false, false, Map.of(), new FixedBootstrapSelectionStrategy(0), bindingAddress2);

        var rf1 = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        var rf2 = endpointRegistry.registerVirtualCluster(virtualClusterModel2).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(bindingAddress1, 9192, false),
                createTestNetworkBindRequest(bindingAddress2, 9192, false));
        assertThat(CompletableFuture.allOf(rf1, rf2).isDone()).isTrue();

        var rsf1 = endpointRegistry.resolve(childChannelFor(bindingAddress1, 9192, false), null).toCompletableFuture().get();
        assertThat(rsf1).isNotNull();
        assertThat(rsf1.endpointGateway()).isEqualTo(virtualClusterModel1);

        var rsf2 = endpointRegistry.resolve(childChannelFor(bindingAddress2, 9192, false), null).toCompletableFuture().get();
        assertThat(rsf2).isNotNull();
        assertThat(rsf2.endpointGateway()).isEqualTo(virtualClusterModel2);
    }

    @Test
    void reconcileAddsNewBrokerEndpoint() throws Exception {
        configureVirtualClusterMock(virtualClusterModel1, DOWNSTREAM_BOOTSTRAP, UPSTREAM_BOOTSTRAP, false);

        var regf = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BOOTSTRAP.port(), false));
        assertThat(regf.isDone()).isTrue();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);

        // Add a new node (1) to the cluster
        when(virtualClusterModel1.getBrokerAddress(0)).thenReturn(DOWNSTREAM_BROKER_0);

        var recf = endpointRegistry.reconcile(virtualClusterModel1, Map.of(0, UPSTREAM_BROKER_0)).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BROKER_0.port(), false));
        assertThat(recf.isDone()).isTrue();
        assertThat(recf.get()).isNull();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(2);
    }

    @Test
    void resolveReconciledBrokerAddress() throws Exception {
        configureVirtualClusterMock(virtualClusterModel1, DOWNSTREAM_BOOTSTRAP, UPSTREAM_BOOTSTRAP, false);

        var regf = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BOOTSTRAP.port(), false));
        assertThat(regf.isDone()).isTrue();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);

        // Add a new node (1) to the cluster
        when(virtualClusterModel1.getBrokerAddress(0)).thenReturn(DOWNSTREAM_BROKER_0);

        var recf = endpointRegistry.reconcile(virtualClusterModel1, Map.of(0, UPSTREAM_BROKER_0)).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BROKER_0.port(), false));
        assertThat(recf.isDone()).isTrue();
        assertThat(recf.get()).isNull();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(2);

        when(virtualClusterModel1.addressingSpec().identify(DOWNSTREAM_BROKER_0.port(), null)).thenReturn(new AddressingSpec.Target.Node(0));
        var binding = endpointRegistry.resolve(childChannelFor(DOWNSTREAM_BROKER_0.port(), false), null).toCompletableFuture().get();
        assertThat(binding).isEqualTo(new BrokerEndpointBinding(virtualClusterModel1, UPSTREAM_BROKER_0, 0));
    }

    @Test
    void resolveDiscoveryBrokerAddress() throws Exception {
        configureVirtualClusterMock(virtualClusterModel1, DOWNSTREAM_BOOTSTRAP, UPSTREAM_BOOTSTRAP, false, false, Map.of(0, DOWNSTREAM_BROKER_0),
                new FixedBootstrapSelectionStrategy(0));

        var regf = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BOOTSTRAP.port(), false),
                createTestNetworkBindRequest(DOWNSTREAM_BROKER_0.port(), false));
        assertThat(regf).isCompleted();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(2);

        when(virtualClusterModel1.addressingSpec().identify(DOWNSTREAM_BROKER_0.port(), null)).thenReturn(new AddressingSpec.Target.Node(0));
        var binding = endpointRegistry.resolve(childChannelFor(DOWNSTREAM_BROKER_0.port(), false), null).toCompletableFuture().get();
        assertThat(binding).isEqualTo(new MetadataDiscoveryBrokerEndpointBinding(virtualClusterModel1, 0));
    }

    @Test
    void reconcileRemovesBrokerEndpoint() throws Exception {
        configureVirtualClusterMock(virtualClusterModel1, DOWNSTREAM_BOOTSTRAP, UPSTREAM_BOOTSTRAP, false);

        var regf = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BOOTSTRAP.port(), false));
        assertThat(regf.isDone()).isTrue();

        // Add brokers (0,1) to the cluster
        when(virtualClusterModel1.getBrokerAddress(0)).thenReturn(DOWNSTREAM_BROKER_0);

        var recf1 = endpointRegistry.reconcile(virtualClusterModel1, Map.of(0, UPSTREAM_BROKER_0)).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BROKER_0.port(), false));
        assertThat(recf1.isDone()).isTrue();

        when(virtualClusterModel1.getBrokerAddress(1)).thenReturn(DOWNSTREAM_BROKER_1);
        var recf2 = endpointRegistry.reconcile(virtualClusterModel1, Map.of(0, UPSTREAM_BROKER_0, 1, UPSTREAM_BROKER_1)).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BROKER_1.port(), false));
        assertThat(recf2.isDone()).isTrue();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(3);

        // Removal of node (0) from the cluster
        var recf3 = endpointRegistry.reconcile(virtualClusterModel1, Map.of(1, UPSTREAM_BROKER_1)).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkUnbindRequest(DOWNSTREAM_BROKER_0.port(), false));
        assertThat(recf3.isDone()).isTrue();
        assertThat(recf3.get()).isNull();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(2);
    }

    @Test
    void reconcileChangesTargetClusterBrokerAddress() throws Exception {
        var upstreamBrokerUpdated0 = HostPort.parse("upstreamupd:29193");

        configureVirtualClusterMock(virtualClusterModel1, DOWNSTREAM_BOOTSTRAP, UPSTREAM_BOOTSTRAP, false);

        var regf = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BOOTSTRAP.port(), false));
        assertThat(regf.isDone()).isTrue();

        when(virtualClusterModel1.getBrokerAddress(0)).thenReturn(DOWNSTREAM_BROKER_0);
        var recf1 = endpointRegistry.reconcile(virtualClusterModel1, Map.of(0, UPSTREAM_BROKER_0)).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BROKER_0.port(), false));
        assertThat(recf1.isDone()).isTrue();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(2);

        when(virtualClusterModel1.addressingSpec().identify(DOWNSTREAM_BROKER_0.port(), null)).thenReturn(new AddressingSpec.Target.Node(0));
        var resolvedBindingBeforeChange = endpointRegistry.resolve(childChannelFor(DOWNSTREAM_BROKER_0.port(), false), null).toCompletableFuture().get();
        assertThat(resolvedBindingBeforeChange).isEqualTo(new BrokerEndpointBinding(virtualClusterModel1, UPSTREAM_BROKER_0, 0));

        // Target cluster updates the address for broker 0
        var recf2 = endpointRegistry.reconcile(virtualClusterModel1, Map.of(0, upstreamBrokerUpdated0)).toCompletableFuture();
        verifyAndProcessNetworkEventQueue();
        assertThat(recf2.isDone()).isTrue();
        assertThat(recf2.get()).isNull();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(2);

        var resolvedBindingAfterChange = endpointRegistry.resolve(childChannelFor(DOWNSTREAM_BROKER_0.port(), false), null).toCompletableFuture().get();
        assertThat(resolvedBindingAfterChange).isEqualTo(new BrokerEndpointBinding(virtualClusterModel1, upstreamBrokerUpdated0, 0));
    }

    @Test
    void reconcileReplacesDiscoveryAddress() throws Exception {
        FixedBootstrapSelectionStrategy selectionStrategy = new FixedBootstrapSelectionStrategy(0);
        configureVirtualClusterMock(virtualClusterModel1, DOWNSTREAM_BOOTSTRAP, UPSTREAM_BOOTSTRAP, false, false,
                Map.of(0, DOWNSTREAM_BROKER_0, 1, DOWNSTREAM_BROKER_1), selectionStrategy);

        var regf = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BOOTSTRAP.port(), false),
                createTestNetworkBindRequest(DOWNSTREAM_BROKER_0.port(), false),
                createTestNetworkBindRequest(DOWNSTREAM_BROKER_1.port(), false));
        assertThat(regf).isDone();

        when(virtualClusterModel1.addressingSpec().identify(DOWNSTREAM_BROKER_0.port(), null)).thenReturn(new AddressingSpec.Target.Node(0));
        when(virtualClusterModel1.addressingSpec().identify(DOWNSTREAM_BROKER_1.port(), null)).thenReturn(new AddressingSpec.Target.Node(1));

        var resolveBroker0BeforeReconcile = endpointRegistry.resolve(childChannelFor(DOWNSTREAM_BROKER_0.port(), false), null).toCompletableFuture().get();
        assertThat(resolveBroker0BeforeReconcile)
                .describedAs("Resolving pre-bound broker 0 should yield the upstream address for bootstrap")
                .isEqualTo(new MetadataDiscoveryBrokerEndpointBinding(virtualClusterModel1, 0));

        // Reconcile learns that upstream broker topology actually has only one broker
        // DOWNSTREAM_BROKER_0 binding has to be updated
        when(virtualClusterModel1.getBrokerAddress(0)).thenReturn(DOWNSTREAM_BROKER_0);
        assertThat(endpointRegistry.reconcile(virtualClusterModel1, Map.of(0, UPSTREAM_BROKER_0))).succeedsWithin(Duration.ofSeconds(1));
        verifyAndProcessNetworkEventQueue();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(3);

        // Now resolving broker 0 yields the upstream address for broker 0
        var resolveBroker0AfterReconcile = endpointRegistry.resolve(childChannelFor(DOWNSTREAM_BROKER_0.port(), false), null).toCompletableFuture().get();
        assertThat(resolveBroker0AfterReconcile)
                .describedAs("Resolving reconciled broker 0 should yield the actual upstream address for broker0")
                .isEqualTo(new BrokerEndpointBinding(virtualClusterModel1, UPSTREAM_BROKER_0, 0));

        // And resolving broker 1 still yields the bootstrap
        var resolveBroker1AfterReconcile = endpointRegistry.resolve(childChannelFor(DOWNSTREAM_BROKER_1.port(), false), null).toCompletableFuture().get();
        assertThat(resolveBroker1AfterReconcile)
                .describedAs("Resolving reconciled broker 1 should still yield bootstrap")
                .isEqualTo(new MetadataDiscoveryBrokerEndpointBinding(virtualClusterModel1, 1));

    }

    @Test
    void reconcileNoOp() throws Exception {
        configureVirtualClusterMock(virtualClusterModel1, DOWNSTREAM_BOOTSTRAP, UPSTREAM_BOOTSTRAP, false);

        var regf = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BOOTSTRAP.port(), false));
        assertThat(regf.isDone()).isTrue();

        // Add broker to the cluster
        when(virtualClusterModel1.getBrokerAddress(0)).thenReturn(DOWNSTREAM_BROKER_0);
        var recf1 = endpointRegistry.reconcile(virtualClusterModel1, Map.of(0, UPSTREAM_BROKER_0)).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BROKER_0.port(), false));
        assertThat(recf1.isDone()).isTrue();

        // Add 2nd broker to the cluster
        when(virtualClusterModel1.getBrokerAddress(1)).thenReturn(DOWNSTREAM_BROKER_1);
        var recf2 = endpointRegistry.reconcile(virtualClusterModel1, Map.of(0, UPSTREAM_BROKER_0, 1, UPSTREAM_BROKER_1)).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BROKER_1.port(), false));
        assertThat(recf2.isDone()).isTrue();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(3);

        var rcf3 = endpointRegistry.reconcile(virtualClusterModel1, Map.of(0, UPSTREAM_BROKER_0, 1, UPSTREAM_BROKER_1)).toCompletableFuture();
        assertThat(rcf3.isDone()).isTrue();
        assertThat(rcf3.get()).isNull();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(3);
    }

    @Test
    void reconcileDeleteWhilstPreviousAddInFlight() throws Exception {
        configureVirtualClusterMock(virtualClusterModel1, DOWNSTREAM_BOOTSTRAP, UPSTREAM_BOOTSTRAP, false);
        when(virtualClusterModel1.getBrokerAddress(0)).thenReturn(DOWNSTREAM_BROKER_0);
        when(virtualClusterModel1.getBrokerAddress(1)).thenReturn(DOWNSTREAM_BROKER_1);

        var regf = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BOOTSTRAP.port(), false));
        assertThat(regf.isDone()).isTrue();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);

        // reconcile adds a node
        when(virtualClusterModel1.getBrokerAddress(0)).thenReturn(DOWNSTREAM_BROKER_0);
        var add1 = endpointRegistry.reconcile(virtualClusterModel1, Map.of(0, UPSTREAM_BROKER_0)).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BROKER_0.port(), false));
        assertThat(add1.isDone()).isTrue();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(2);

        // reconcile adds a node, but organise so the network event is not processed yet so the future won't complete
        when(virtualClusterModel1.getBrokerAddress(1)).thenReturn(DOWNSTREAM_BROKER_1);
        var add2 = endpointRegistry.reconcile(virtualClusterModel1, Map.of(0, UPSTREAM_BROKER_0, 1, UPSTREAM_BROKER_1)).toCompletableFuture();
        assertThat(add2.isDone()).isFalse();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(3);

        // reconcile now removes a node, it cannot be processed because it is behind the add
        var remove = endpointRegistry.reconcile(virtualClusterModel1, Map.of(1, UPSTREAM_BROKER_1)).toCompletableFuture();
        assertThat(remove.isDone()).isFalse();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(3);

        // process add event
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BROKER_1.port(), false));
        assertThat(add2.isDone()).isTrue();
        assertThat(add2.get()).isNull();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(3);
        assertThat(remove.isDone()).isFalse();

        // process remove event
        verifyAndProcessNetworkEventQueue(createTestNetworkUnbindRequest(DOWNSTREAM_BROKER_0.port(), false));
        assertThat(remove.isDone()).isTrue();
        assertThat(remove.get()).isNull();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(2);
    }

    @Test
    void reconcileFailsDueToExternalPortConflict() {
        doReconcileFailsDueToExternalPortConflict(DOWNSTREAM_BROKER_0, UPSTREAM_BROKER_0);
    }

    private EndpointGateway doReconcileFailsDueToExternalPortConflict(HostPort downstreamBroker0, HostPort upstreamBroker0) {
        configureVirtualClusterMock(virtualClusterModel1, DOWNSTREAM_BOOTSTRAP, UPSTREAM_BOOTSTRAP, false);

        var rgf = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BOOTSTRAP.port(), false));
        assertThat(rgf.isDone()).isTrue();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);

        // Add a new node (1) to the cluster
        when(virtualClusterModel1.getBrokerAddress(0)).thenReturn(downstreamBroker0);

        var rcf = endpointRegistry.reconcile(virtualClusterModel1, Map.of(0, upstreamBroker0)).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(
                createTestNetworkBindRequest(Optional.empty(), downstreamBroker0.port(), false, CompletableFuture.failedFuture(new IOException("mocked port in use"))));
        assertThat(rcf.isDone()).isTrue();
        var executionException = assertThrows(ExecutionException.class, rcf::get);
        assertThat(executionException).hasRootCauseInstanceOf(IOException.class);
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);

        return virtualClusterModel1;
    }

    @Test
    void nextReconcileSucceedsAfterTransientPortConflict() throws Exception {
        var virtualCluster = doReconcileFailsDueToExternalPortConflict(DOWNSTREAM_BROKER_0, UPSTREAM_BROKER_0);

        var rcf = endpointRegistry.reconcile(virtualCluster, Map.of(0, UPSTREAM_BROKER_0)).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BROKER_0.port(), false));

        assertThat(rcf.isDone()).isTrue();
        assertThat(rcf.get()).isNull();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(2);
    }

    @Test
    void shouldResolveBootstrapPortAfterRegistration() {
        // Given
        configureVirtualClusterMock(virtualClusterModel1, DOWNSTREAM_BOOTSTRAP, UPSTREAM_BOOTSTRAP, false);
        endpointRegistry.registerVirtualCluster(virtualClusterModel1);
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BOOTSTRAP.port(), false));

        // When / Then
        assertThatBootstrapPortResolves(endpointRegistry, virtualClusterModel1).isEqualTo(DOWNSTREAM_BOOTSTRAP.port());
    }

    @Test
    void shouldResolveBrokerPortAfterReconciliation() {
        // Given
        configureVirtualClusterMock(virtualClusterModel1, DOWNSTREAM_BOOTSTRAP, UPSTREAM_BOOTSTRAP, false);
        endpointRegistry.registerVirtualCluster(virtualClusterModel1);
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BOOTSTRAP.port(), false));

        when(virtualClusterModel1.getBrokerAddress(0)).thenReturn(DOWNSTREAM_BROKER_0);
        endpointRegistry.reconcile(virtualClusterModel1, Map.of(0, UPSTREAM_BROKER_0));
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BROKER_0.port(), false));

        // When / Then
        assertThatBrokerPortResolves(endpointRegistry, virtualClusterModel1, 0).isEqualTo(DOWNSTREAM_BROKER_0.port());
    }

    @Test
    void shouldResolveOsAssignedBootstrapPort() {
        // Given
        int osAssignedPort = 58392;
        var bootstrapWithOsPort = new HostPort("bootstrap.kafka", 0);
        configureVirtualClusterMock(virtualClusterModel1, bootstrapWithOsPort, UPSTREAM_BOOTSTRAP, false);

        endpointRegistry.registerVirtualCluster(virtualClusterModel1);
        var channelWithActualPort = createMockNettyChannel(osAssignedPort);
        verifyAndProcessNetworkEventQueue(
                createTestNetworkBindRequest(Optional.empty(), 0, false, CompletableFuture.completedFuture(channelWithActualPort)));

        // When / Then
        assertThatBootstrapPortResolves(endpointRegistry, virtualClusterModel1).isEqualTo(osAssignedPort);
    }

    @Test
    void shouldResolveOsAssignedPortsForBootstrapAndBrokers() {
        // Given
        int bootstrapOsPort = 58392;
        int broker0OsPort = 58393;
        int broker1OsPort = 58394;
        var bootstrapAddress = new HostPort("bootstrap.kafka", 0);
        var broker0Address = new HostPort("broker0.kafka", 0);
        var broker1Address = new HostPort("broker1.kafka", 0);
        configureVirtualClusterMock(virtualClusterModel1, bootstrapAddress, UPSTREAM_BOOTSTRAP, false, false,
                Map.of(0, broker0Address, 1, broker1Address), new FixedBootstrapSelectionStrategy(0));

        endpointRegistry.registerVirtualCluster(virtualClusterModel1);
        verifyAndProcessNetworkEventQueue(
                createTestNetworkBindRequest(Optional.empty(), 0, false, CompletableFuture.completedFuture(createMockNettyChannel(bootstrapOsPort))),
                createTestNetworkBindRequest(Optional.empty(), 0, false, CompletableFuture.completedFuture(createMockNettyChannel(broker0OsPort))),
                createTestNetworkBindRequest(Optional.empty(), 0, false, CompletableFuture.completedFuture(createMockNettyChannel(broker1OsPort))));

        // When / Then
        assertThatBootstrapPortResolves(endpointRegistry, virtualClusterModel1).isEqualTo(bootstrapOsPort);
        assertThatBrokerPortResolves(endpointRegistry, virtualClusterModel1, 0).isEqualTo(broker0OsPort);
        assertThatBrokerPortResolves(endpointRegistry, virtualClusterModel1, 1).isEqualTo(broker1OsPort);
    }

    @Test
    void shouldShareChannelForSniGatewaysWithPortZero() {
        // Given
        var sniBootstrap1 = new HostPort("vc1.kafka", 0);
        var sniBootstrap2 = new HostPort("vc2.kafka", 0);
        configureVirtualClusterMock(virtualClusterModel1, sniBootstrap1, UPSTREAM_BOOTSTRAP, true);
        configureVirtualClusterMock(virtualClusterModel2, sniBootstrap2, UPSTREAM_BOOTSTRAP, true);

        int osAssignedPort = 58400;
        endpointRegistry.registerVirtualCluster(virtualClusterModel1);
        endpointRegistry.registerVirtualCluster(virtualClusterModel2);

        // When
        var sharedChannel = createMockNettyChannel(osAssignedPort);
        verifyAndProcessNetworkEventQueue(
                createTestNetworkBindRequest(Optional.empty(), 0, true, CompletableFuture.completedFuture(sharedChannel)));

        // Then
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);
        assertThatBootstrapPortResolves(endpointRegistry, virtualClusterModel1).isEqualTo(osAssignedPort);
        assertThatBootstrapPortResolves(endpointRegistry, virtualClusterModel2).isEqualTo(osAssignedPort);
    }

    @Test
    void shouldThrowWhenResolvingPortForUnregisteredGateway() {
        // Given - virtualClusterModel1 has never been registered

        // When / Then
        assertThatThrownBy(() -> endpointRegistry.resolvePort(vc1BootstrapNodeId))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void upstreamAddressReturnsEmptyForUnregisteredGateway() {
        // Given: nothing registered

        // When / Then
        assertThat(endpointRegistry.upstreamAddress(virtualClusterModel1, 0)).isEmpty();
    }

    @Test
    void upstreamAddressReturnsEmptyWhenNotYetReconciled() throws Exception {
        // Given
        configureVirtualClusterMock(virtualClusterModel1, DOWNSTREAM_BOOTSTRAP, UPSTREAM_BOOTSTRAP, false);
        var reg = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BOOTSTRAP.port(), false));
        assertThat(reg).isDone();

        // When / Then
        assertThat(endpointRegistry.upstreamAddress(virtualClusterModel1, 0)).isEmpty();
    }

    @Test
    void upstreamAddressReturnsEmptyForUnknownNodeId() throws Exception {
        // Given
        configureVirtualClusterMock(virtualClusterModel1, DOWNSTREAM_BOOTSTRAP, UPSTREAM_BOOTSTRAP, false);
        var reg = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BOOTSTRAP.port(), false));
        assertThat(reg).isDone();
        when(virtualClusterModel1.getBrokerAddress(0)).thenReturn(DOWNSTREAM_BROKER_0);
        var rec = endpointRegistry.reconcile(virtualClusterModel1, Map.of(0, UPSTREAM_BROKER_0)).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BROKER_0.port(), false));
        assertThat(rec).isDone();

        // When / Then
        assertThat(endpointRegistry.upstreamAddress(virtualClusterModel1, 1)).isEmpty();
    }

    @Test
    void upstreamAddressReturnsAddressForKnownNodeId() throws Exception {
        // Given
        configureVirtualClusterMock(virtualClusterModel1, DOWNSTREAM_BOOTSTRAP, UPSTREAM_BOOTSTRAP, false);
        var reg = endpointRegistry.registerVirtualCluster(virtualClusterModel1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BOOTSTRAP.port(), false));
        assertThat(reg).isDone();
        when(virtualClusterModel1.getBrokerAddress(0)).thenReturn(DOWNSTREAM_BROKER_0);
        var rec = endpointRegistry.reconcile(virtualClusterModel1, Map.of(0, UPSTREAM_BROKER_0)).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(DOWNSTREAM_BROKER_0.port(), false));
        assertThat(rec).isDone();

        // When / Then
        assertThat(endpointRegistry.upstreamAddress(virtualClusterModel1, 0))
                .hasValue(UPSTREAM_BROKER_0);
    }

    private Channel createMockNettyChannel(int port) {
        var channel = mock(Channel.class);
        var attr = createTestAttribute(EndpointRegistry.CHANNEL_BINDINGS);
        when(channel.attr(EndpointRegistry.CHANNEL_BINDINGS)).thenReturn(attr);
        var localAddress = InetSocketAddress.createUnresolved("localhost", port); // This is lenient because not all tests exercise the unbind path
        lenient().when(channel.localAddress()).thenReturn(localAddress);
        return channel;
    }

    private Channel createMockChildChannel(Channel acceptor) {
        var child = mock(Channel.class);
        when(child.parent()).thenReturn(acceptor);
        return child;
    }

    private Channel childChannelFor(int port, boolean tls) {
        return createMockChildChannel(acceptorChannels.get(Endpoint.createEndpoint(port, tls)));
    }

    private Channel childChannelFor(Optional<String> bindingAddress, int port, boolean tls) {
        return createMockChildChannel(acceptorChannels.get(Endpoint.createEndpoint(bindingAddress, port, tls)));
    }

    private NetworkBindRequest createTestNetworkBindRequest(int expectedPort, boolean expectedTls) {
        var channelMock = createMockNettyChannel(expectedPort);
        acceptorChannels.put(Endpoint.createEndpoint(expectedPort, expectedTls), channelMock);
        return createTestNetworkBindRequest(Optional.empty(), expectedPort, expectedTls, CompletableFuture.completedFuture(channelMock));
    }

    private NetworkBindRequest createTestNetworkBindRequest(Optional<String> expectedBindingAddress, int expectedPort, boolean expectedTls) {
        Objects.requireNonNull(expectedBindingAddress);
        var channelMock = createMockNettyChannel(expectedPort);
        acceptorChannels.put(Endpoint.createEndpoint(expectedBindingAddress, expectedPort, expectedTls), channelMock);
        return createTestNetworkBindRequest(expectedBindingAddress, expectedPort, expectedTls, CompletableFuture.completedFuture(channelMock));
    }

    private NetworkBindRequest createTestNetworkBindRequest(Optional<String> expectedBindingAddress, int expectedPort, boolean expectedTls,
                                                            CompletableFuture<Channel> channelFuture) {
        return new NetworkBindRequest(channelFuture, Endpoint.createEndpoint(expectedBindingAddress, expectedPort, expectedTls));
    }

    private NetworkUnbindRequest createTestNetworkUnbindRequest(int port, final boolean tls) {
        return createTestNetworkUnbindRequest(port, tls, CompletableFuture.completedFuture(null));
    }

    private NetworkUnbindRequest createTestNetworkUnbindRequest(int port, final boolean tls, final CompletableFuture<Void> future) {
        return new NetworkUnbindRequest(tls, null, future) {
            @Override
            public int port() {
                return port;
            }
        };
    }

    private void configureVirtualClusterMock(EndpointGateway cluster, HostPort downstreamBootstrap, HostPort upstreamBootstrap, boolean tls) {
        configureVirtualClusterMock(cluster, downstreamBootstrap, upstreamBootstrap, tls, tls, Map.of(), new FixedBootstrapSelectionStrategy(0), Optional.empty());
    }

    private void configureVirtualClusterMock(EndpointGateway gateway, HostPort downstreamBootstrap, HostPort upstreamBootstrap, boolean tls, boolean sni,
                                             Map<Integer, HostPort> discoveryAddressMap, BootstrapSelectionStrategy selectionStrategy) {
        configureVirtualClusterMock(gateway, downstreamBootstrap, upstreamBootstrap, tls, sni, discoveryAddressMap, selectionStrategy, Optional.empty());
    }

    private void configureVirtualClusterMock(EndpointGateway gateway, HostPort downstreamBootstrap, HostPort upstreamBootstrap, boolean tls, boolean sni,
                                             Map<Integer, HostPort> discoveryAddressMap, BootstrapSelectionStrategy selectionStrategy,
                                             Optional<String> bindAddress) {
        when(gateway.getClusterBootstrapAddress()).thenReturn(downstreamBootstrap);
        when(gateway.isUseTls()).thenReturn(tls);
        when(gateway.requiresServerNameIndication()).thenReturn(sni);
        when(gateway.discoveryAddressMap()).thenReturn(discoveryAddressMap);
        when(gateway.getBindAddress()).thenReturn(bindAddress);
        var targetCluster = new TargetCluster(upstreamBootstrap.toString(), Optional.empty(), selectionStrategy);
        when(gateway.targetCluster()).thenReturn(targetCluster);

        var bindingSpec = mock(BindingSpec.class);
        lenient().when(bindingSpec.getBootstrapBindAddress()).thenReturn(downstreamBootstrap);
        lenient().when(bindingSpec.nodeBindAddresses()).thenReturn(discoveryAddressMap);
        lenient().when(bindingSpec.getBindAddress()).thenReturn(bindAddress);
        lenient().when(bindingSpec.getExclusivePorts()).thenReturn(Set.of());
        lenient().when(bindingSpec.getSharedPorts()).thenReturn(Set.of());
        lenient().when(bindingSpec.requiresServerNameIndication()).thenReturn(sni);
        lenient().when(gateway.bindingSpec()).thenReturn(bindingSpec);

        var addressingSpec = mock(AddressingSpec.class);
        lenient().when(addressingSpec.identify(anyInt(), any())).thenReturn(new AddressingSpec.Target.NotRecognised());
        if (sni) {
            // an SNI gateway only recognises its own bootstrap hostname
            lenient().when(addressingSpec.identify(downstreamBootstrap.port(), downstreamBootstrap.host())).thenReturn(new AddressingSpec.Target.Bootstrap());
        }
        else {
            // a non-SNI gateway identifies purely by port, regardless of any SNI hostname presented
            lenient().when(addressingSpec.identify(eq(downstreamBootstrap.port()), any())).thenReturn(new AddressingSpec.Target.Bootstrap());
        }
        lenient().when(gateway.addressingSpec()).thenReturn(addressingSpec);
    }

    private void verifyVirtualClusterRegisterFuture(int expectedPort, boolean expectedTls, CompletableFuture<Endpoint> future) throws Exception {
        assertThat(future.isDone()).isTrue();
        assertThat(future.get()).isEqualTo(Endpoint.createEndpoint(expectedPort, expectedTls));
    }

    private void verifyAndProcessNetworkEventQueue(NetworkBindingOperation<?>... expectedEvents) {
        bindingOperationProcessor.verifyAndProcessNetworkEvents(expectedEvents);
    }

    private <U> Attribute<U> createTestAttribute(final AttributeKey<U> key) {
        return new Attribute<>() {
            final AtomicReference<U> map = new AtomicReference<>();

            @Override
            public AttributeKey<U> key() {
                return key;
            }

            @Override
            public U get() {
                return map.get();
            }

            @Override
            public void set(U value) {
                map.set(value);
            }

            @Override
            public U getAndSet(U value) {

                return map.getAndSet(value);
            }

            @Override
            public U setIfAbsent(U value) {
                return map.compareAndExchange(null, value);
            }

            @Override
            public U getAndRemove() {
                return map.compareAndExchange(map.get(), null);
            }

            @Override
            public boolean compareAndSet(U oldValue,
                                         U newValue) {
                return map.compareAndSet(oldValue, newValue);
            }

            @Override
            public void remove() {
                map.set(null);
            }
        };
    }

    private static class TestNetworkBindingOperationProcessor implements NetworkBindingOperationProcessor {
        private final BlockingQueue<NetworkBindingOperation<?>> queue = new LinkedBlockingQueue<>();

        @Override
        public void start(ServerBootstrap plain, ServerBootstrap tls) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void enqueueNetworkBindingEvent(NetworkBindingOperation<?> o) {
            queue.add(o);
        }

        private void verifyAndProcessNetworkEvents(NetworkBindingOperation... expectedEvents) {
            assertThat(queue).as("unexpected number of events").hasSize(expectedEvents.length);
            var expectedEventIterator = Arrays.stream(expectedEvents).iterator();
            while (expectedEventIterator.hasNext()) {
                var expectedEvent = expectedEventIterator.next();
                if (queue.isEmpty()) {
                    fail("No network event available, expecting one matching " + expectedEvent);
                }
                var event = queue.poll();
                if (event instanceof NetworkBindRequest bindEvent) {
                    assertThat(bindEvent.getBindingAddress()).isEqualTo(((NetworkBindRequest) expectedEvent).getBindingAddress());
                    assertThat(bindEvent.port()).isEqualTo(expectedEvent.port());
                    assertThat(bindEvent.tls()).isEqualTo(expectedEvent.tls());
                }
                else if (event instanceof NetworkUnbindRequest unbindEvent) {
                    assertThat(unbindEvent.port()).isEqualTo(expectedEvent.port());
                    assertThat(unbindEvent.tls()).isEqualTo(expectedEvent.tls());
                }
                else {
                    fail("unexpected event type received");
                }
                propagateFutureResult(expectedEvent.getFuture(), event.getFuture());
            }
        }

        private <U> void propagateFutureResult(CompletableFuture<U> source, CompletableFuture<U> dest) {
            source.handle((c, t) -> {
                if (t != null) {
                    dest.completeExceptionally(t);
                }
                else {
                    dest.complete(c);
                }
                return null;
            });
        }

        @Override
        public void close() {
            // nothing to do
        }
    }

    private static org.assertj.core.api.AbstractIntegerAssert<?> assertThatBootstrapPortResolves(EndpointRegistry registry, EndpointGateway gateway) {
        return assertThat(registry.resolvePort(new ProxyNodeId.Bootstrap(gateway)).toCompletableFuture().join());
    }

    private static org.assertj.core.api.AbstractIntegerAssert<?> assertThatBrokerPortResolves(EndpointRegistry registry, EndpointGateway gateway, int nodeId) {
        return assertThat(registry.resolvePort(new ProxyNodeId.Broker(gateway, nodeId)).toCompletableFuture().join());
    }
}
