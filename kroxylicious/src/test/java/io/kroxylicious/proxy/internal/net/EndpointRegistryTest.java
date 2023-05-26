/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.service.HostPort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mock.Strictness.LENIENT;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EndpointRegistryTest {
    private final TestNetworkBindingOperationProcessor bindingOperationProcessor = new TestNetworkBindingOperationProcessor();
    private final EndpointRegistry endpointRegistry = new EndpointRegistry(bindingOperationProcessor);
    @Mock(strictness = LENIENT)
    private VirtualCluster virtualCluster1;
    @Mock(strictness = LENIENT)
    private VirtualCluster virtualCluster2;

    @Test
    public void registerVirtualCluster() throws Exception {
        configureVirtualClusterMock(virtualCluster1, "mycluster1:9192", "upstream1:19192", false);

        var f = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(9192, false));
        assertThat(f.isDone()).isTrue();
        assertThat(f.get()).isEqualTo(Endpoint.createEndpoint(9192, false));

        assertThat(endpointRegistry.isRegistered(virtualCluster1)).isTrue();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);
    }

    @Test
    public void registerVirtualClusterTls() throws Exception {
        configureVirtualClusterMock(virtualCluster1, "mycluster1:9192", "upstream1:19192", true);

        var f = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(9192, true));
        assertThat(f.isDone()).isTrue();
        assertThat(f.get()).isEqualTo(Endpoint.createEndpoint(9192, true));
    }

    @Test
    public void registerSameVirtualClusterIsIdempotent() throws Exception {
        configureVirtualClusterMock(virtualCluster1, "mycluster1:9192", "upstream1:19192", false);

        var f1 = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        var f2 = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(9192, false));
        assertThat(CompletableFuture.allOf(f1, f2).isDone()).isTrue();
        assertThat(f2.get()).isEqualTo(Endpoint.createEndpoint(9192, false));

        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);
    }

    @Test
    public void registerTwoClustersThatShareSameNetworkEndpoint() throws Exception {
        configureVirtualClusterMock(virtualCluster1, "mycluster1:9192", "upstream1:19192", true);
        configureVirtualClusterMock(virtualCluster2, "mycluster2:9192", "upstream2:19192", true);

        var f1 = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        var f2 = endpointRegistry.registerVirtualCluster(virtualCluster2).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(9192, true));
        assertThat(CompletableFuture.allOf(f1, f2).isDone()).isTrue();
        assertThat(f1.get()).isEqualTo(Endpoint.createEndpoint(9192, true));
        assertThat(f2.get()).isEqualTo(Endpoint.createEndpoint(9192, true));

        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);
    }

    @Test
    public void registerTwoClustersThatUseDistinctNetworkEndpoints() throws Exception {
        configureVirtualClusterMock(virtualCluster1, "mycluster1:9191", "upstream1:19191", true);
        configureVirtualClusterMock(virtualCluster2, "mycluster2:9192", "upstream2:19192", true);

        var f1 = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        var f2 = endpointRegistry.registerVirtualCluster(virtualCluster2).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(9191, true),
                createTestNetworkBindRequest(9192, true));
        assertThat(CompletableFuture.allOf(f1, f2).isDone()).isTrue();
        assertThat(f1.get()).isEqualTo(Endpoint.createEndpoint(9191, true));
        assertThat(f2.get()).isEqualTo(Endpoint.createEndpoint(9192, true));

        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(2);
    }

    @Test
    public void registerRejectsDuplicatedBinding() throws Exception {
        configureVirtualClusterMock(virtualCluster1, "localhost:9191", "upstream1:19191", false);
        configureVirtualClusterMock(virtualCluster2, "localhost:9191", "upstream2:19191", false);

        var f1 = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(9191, false));
        assertThat(f1.isDone()).isTrue();

        assertThat(endpointRegistry.isRegistered(virtualCluster1)).isTrue();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);

        verifyAndProcessNetworkEventQueue();
        var executionException = assertThrows(ExecutionException.class,
                () -> endpointRegistry.registerVirtualCluster(virtualCluster2).toCompletableFuture().get());
        assertThat(executionException).hasCauseInstanceOf(EndpointBindingException.class);

        assertThat(endpointRegistry.isRegistered(virtualCluster1)).isTrue();
        assertThat(endpointRegistry.isRegistered(virtualCluster2)).isFalse();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);
    }

    @Test
    public void registerVirtualClusterFailsDueToExternalPortConflict() throws Exception {
        configureVirtualClusterMock(virtualCluster1, "localhost:9191", "upstream1:19191", false);

        var f = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(
                createTestNetworkBindRequest(Optional.empty(), 9191, false, CompletableFuture.failedFuture(new IOException("mocked port in use"))));
        assertThat(f.isDone()).isTrue();
        var executionException = assertThrows(ExecutionException.class, f::get);
        assertThat(executionException).hasRootCauseInstanceOf(IOException.class);

        assertThat(endpointRegistry.isRegistered(virtualCluster1)).isFalse();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(0);
    }

    @Test
    public void deregisterVirtualCluster() throws Exception {
        configureVirtualClusterMock(virtualCluster1, "mycluster1:9192", "upstream1:19192", true);

        var bindFuture = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(9192, true));
        assertThat(bindFuture.isDone()).isTrue();

        assertThat(endpointRegistry.isRegistered(virtualCluster1)).isTrue();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);

        var unbindFuture = endpointRegistry.deregisterVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkUnbindRequest(9192, true));
        assertThat(unbindFuture.isDone()).isTrue();

        assertThat(endpointRegistry.isRegistered(virtualCluster1)).isFalse();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(0);
    }

    @Test
    public void deregisterSameVirtualClusterIsIdempotent() throws Exception {
        configureVirtualClusterMock(virtualCluster1, "mycluster1:9192", "upstream1:19192", true);

        var bindFuture = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(9192, true));
        assertThat(bindFuture.isDone()).isTrue();

        var unbindFuture = endpointRegistry.deregisterVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkUnbindRequest(9192, true));
        assertThat(unbindFuture.isDone()).isTrue();

        var unbindFuture2 = endpointRegistry.deregisterVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue();
        assertThat(unbindFuture2.isDone()).isTrue();
    }

    @Test
    public void deregisterClusterThatSharesEndpoint() throws Exception {
        configureVirtualClusterMock(virtualCluster1, "mycluster1:9191", "upstream1:19192", true);
        configureVirtualClusterMock(virtualCluster2, "mycluster2:9191", "upstream2:19192", true);

        var f1 = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        var f2 = endpointRegistry.registerVirtualCluster(virtualCluster2).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(9191, true));
        assertThat(CompletableFuture.allOf(f1, f2).isDone()).isTrue();

        var unbindFuture1 = endpointRegistry.deregisterVirtualCluster(virtualCluster1).toCompletableFuture();
        // Port 9191 is shared by the second virtualcluster, so it can't be unbound yet
        verifyAndProcessNetworkEventQueue();
        assertThat(unbindFuture1.isDone()).isTrue();

        var unbindFuture2 = endpointRegistry.deregisterVirtualCluster(virtualCluster2).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkUnbindRequest(9191, true));
        assertThat(unbindFuture2.isDone()).isTrue();
    }

    @Test
    public void reregisterClusterWhilstDeregisterIsInProgress() throws Exception {
        configureVirtualClusterMock(virtualCluster1, "mycluster1:9192", "upstream1:19192", true);

        var r1 = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(9192, true));
        assertThat(r1.isDone()).isTrue();

        var d1 = endpointRegistry.deregisterVirtualCluster(virtualCluster1).toCompletableFuture();
        // The de-registration for cluster1 is queued up so the future won't be completed.
        assertThat(d1.isDone()).isFalse();

        var rereg = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        assertThat(rereg.isDone()).isFalse();

        // we expect an unbind for 9192
        verifyAndProcessNetworkEventQueue(createTestNetworkUnbindRequest(9192, true));
        assertThat(d1.isDone()).isTrue();

        // followed by an immediate rebind of the same port.
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(9192, true));

        assertThat(rereg.isDone()).isTrue();
        assertThat(rereg.get()).isEqualTo(Endpoint.createEndpoint(9192, true));
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);
    }

    @Test
    public void registerClusterWhileAnotherIsDeregistering() throws Exception {
        configureVirtualClusterMock(virtualCluster1, "mycluster1:9192", "upstream1:19192", true);
        configureVirtualClusterMock(virtualCluster2, "mycluster2:9192", "upstream2:19192", true);

        var r1 = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(9192, true));
        assertThat(r1.isDone()).isTrue();

        var d1 = endpointRegistry.deregisterVirtualCluster(virtualCluster1).toCompletableFuture();
        // The de-registration for cluster1 is queued up so the future won't be completed.
        assertThat(d1.isDone()).isFalse();

        var r2 = endpointRegistry.registerVirtualCluster(virtualCluster2).toCompletableFuture();
        assertThat(r2.isDone()).isFalse();

        // we expect an unbind for 9192 followed by an immediate rebind of the same port.
        verifyAndProcessNetworkEventQueue(createTestNetworkUnbindRequest(9192, true));
        assertThat(d1.isDone()).isTrue();

        // followed by an immediate rebind of the same port.
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(9192, true));
        assertThat(r2.isDone()).isTrue();

        assertThat(CompletableFuture.allOf(d1, r2).isDone()).isTrue();
        assertThat(r2.get()).isEqualTo(Endpoint.createEndpoint(9192, true));
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);
    }

    @ParameterizedTest
    @CsvSource({ "mycluster1:9192,upstream1:9192,true,true", "mycluster1:9192,upstream1:9192,true,false", "localhost:9192,upstream1:9192,false,false" })
    public void resolveBootstrap(@ConvertWith(HostPortConverter.class) HostPort downstreamBootstrap, @ConvertWith(HostPortConverter.class) HostPort upstreamBootstrap,
                                 boolean tls, boolean sni)
            throws Exception {
        configureVirtualClusterMock(virtualCluster1, downstreamBootstrap.toString(), upstreamBootstrap.toString(), tls, sni);

        var f = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(downstreamBootstrap.port(), tls));
        assertThat(f.isDone()).isTrue();

        var binding = endpointRegistry.resolve(Endpoint.createEndpoint(downstreamBootstrap.port(), tls), tls ? downstreamBootstrap.host() : null).toCompletableFuture()
                .get();
        assertThat(binding).isEqualTo(new VirtualClusterBootstrapBinding(virtualCluster1, upstreamBootstrap));
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource({ "mismatching host,mycluster1:9192,upstream1:9192,mycluster2:9192", "mistmatching port,mycluster1:9192,upstream1:9192,mycluster1:9191" })
    public void resolveBootstrapResolutionFailures(String name,
                                                   @ConvertWith(HostPortConverter.class) HostPort downstreamBootstrap,
                                                   @ConvertWith(HostPortConverter.class) HostPort upstreamBootstrap,
                                                   @ConvertWith(HostPortConverter.class) HostPort resolveAddress)
            throws Exception {
        configureVirtualClusterMock(virtualCluster1, downstreamBootstrap.toString(), upstreamBootstrap.toString(), true);

        var f = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(9192, true));
        assertThat(f.isDone()).isTrue();

        var executionException = assertThrows(ExecutionException.class,
                () -> endpointRegistry.resolve(Endpoint.createEndpoint(resolveAddress.port(), true), resolveAddress.host()).toCompletableFuture().get());
        assertThat(executionException).hasCauseInstanceOf(EndpointResolutionException.class);
    }

    @ParameterizedTest
    @CsvSource({ "mycluster1:9192,upstream1:9192,MyClUsTeR1:9192",
            "69.2.0.192.in-addr.arpa:9192,upstream1:9192,69.2.0.192.in-ADDR.ARPA:9192" })
    public void resolveRespectsCaseInsensitivityRfc4343(@ConvertWith(HostPortConverter.class) HostPort downstreamBootstrap,
                                                        @ConvertWith(HostPortConverter.class) HostPort upstreamBootstrap,
                                                        @ConvertWith(HostPortConverter.class) HostPort resolveAddress)
            throws Exception {
        configureVirtualClusterMock(virtualCluster1, downstreamBootstrap.toString(), upstreamBootstrap.toString(), true);

        var f = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(downstreamBootstrap.port(), true));
        assertThat(f.isDone()).isTrue();

        var binding = endpointRegistry.resolve(Endpoint.createEndpoint(resolveAddress.port(), true), resolveAddress.host()).toCompletableFuture().get();
        assertThat(binding).isEqualTo(new VirtualClusterBootstrapBinding(virtualCluster1, upstreamBootstrap));
    }

    @Test
    public void bindingAddressEndpointSeparation() throws Exception {
        var bindingAddress1 = Optional.of("127.0.0.1");
        configureVirtualClusterMock(virtualCluster1, "localhost:9192", "upstream1:9192", false);
        when(virtualCluster1.getBindAddress()).thenReturn(bindingAddress1);

        var bindingAddress2 = Optional.of("192.168.0.1");
        configureVirtualClusterMock(virtualCluster2, "myhost:9192", "upstream2:9192", false);
        when(virtualCluster2.getBindAddress()).thenReturn(bindingAddress2);

        var f1 = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        var f2 = endpointRegistry.registerVirtualCluster(virtualCluster2).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(bindingAddress1, 9192, false),
                createTestNetworkBindRequest(bindingAddress2, 9192, false));
        assertThat(CompletableFuture.allOf(f1, f2).isDone()).isTrue();

        var b1 = endpointRegistry.resolve(Endpoint.createEndpoint(bindingAddress1, 9192, false), null).toCompletableFuture().get();
        assertThat(b1).isNotNull();
        assertThat(b1.virtualCluster()).isEqualTo(virtualCluster1);

        var b2 = endpointRegistry.resolve(Endpoint.createEndpoint(bindingAddress2, 9192, false), null).toCompletableFuture().get();
        assertThat(b2).isNotNull();
        assertThat(b2.virtualCluster()).isEqualTo(virtualCluster2);

        var executionException = assertThrows(ExecutionException.class,
                () -> endpointRegistry.resolve(Endpoint.createEndpoint(9192, false), null).toCompletableFuture().get());
        assertThat(executionException).hasCauseInstanceOf(EndpointResolutionException.class);
    }

    @Test
    public void reconcileAddsNewBrokerEndpoint() throws Exception {
        var downstreamBroker0 = HostPort.parse("localhost:9193");
        var upstreamBroker0 = HostPort.parse("upstream:19193");
        configureVirtualClusterMock(virtualCluster1, "localhost:9192", "upstream1:9192", false);

        var rgf = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(9192, false));
        assertThat(rgf.isDone()).isTrue();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);

        // Add a new node (1) to the cluster
        when(virtualCluster1.getBrokerAddress(0)).thenReturn(downstreamBroker0);

        var rcf = endpointRegistry.reconcile(virtualCluster1, Map.of(0, upstreamBroker0)).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(9193, false));
        assertThat(rcf.isDone()).isTrue();
        assertThat(rcf.get()).isNull();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(2);
    }

    @Test
    public void resolveReconciledBrokerAddress() throws Exception {
        var downstreamBroker0 = HostPort.parse("localhost:9193");
        var upstreamBroker0 = HostPort.parse("upstream:19193");
        configureVirtualClusterMock(virtualCluster1, "localhost:9192", "upstream1:9192", false);

        var rgf = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(9192, false));
        assertThat(rgf.isDone()).isTrue();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);

        // Add a new node (1) to the cluster
        when(virtualCluster1.getBrokerAddress(0)).thenReturn(downstreamBroker0);

        var rcf = endpointRegistry.reconcile(virtualCluster1, Map.of(0, upstreamBroker0)).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(9193, false));
        assertThat(rcf.isDone()).isTrue();
        assertThat(rcf.get()).isNull();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(2);

        var binding = endpointRegistry.resolve(Endpoint.createEndpoint(9193, false), null).toCompletableFuture().get();
        assertThat(binding).isEqualTo(new VirtualClusterBrokerBinding(virtualCluster1, upstreamBroker0, 0));
    }

    @Test
    public void reconcileRemovesBrokerEndpoint() throws Exception {
        var downstreamBroker0 = HostPort.parse("localhost:9193");
        var downstreamBroker1 = HostPort.parse("localhost:9194");
        var upstreamBroker0 = HostPort.parse("upstream:19193");
        var upstreamBroker1 = HostPort.parse("upstream:19194");

        configureVirtualClusterMock(virtualCluster1, "localhost:9192", "upstream1:9192", false);

        var rgf = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(9192, false));
        assertThat(rgf.isDone()).isTrue();

        // Add brokers (0,1) to the cluster
        when(virtualCluster1.getBrokerAddress(0)).thenReturn(downstreamBroker0);

        var rcf1 = endpointRegistry.reconcile(virtualCluster1, Map.of(0, upstreamBroker0)).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(9193, false));
        assertThat(rcf1.isDone()).isTrue();

        when(virtualCluster1.getBrokerAddress(1)).thenReturn(downstreamBroker1);
        var rcf2 = endpointRegistry.reconcile(virtualCluster1, Map.of(0, upstreamBroker0, 1, upstreamBroker1)).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(9194, false));
        assertThat(rcf2.isDone()).isTrue();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(3);

        // Removal of node (0) from the cluster
        var rcf3 = endpointRegistry.reconcile(virtualCluster1, Map.of(1, upstreamBroker1)).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkUnbindRequest(9193, false));
        assertThat(rcf3.isDone()).isTrue();
        assertThat(rcf3.get()).isNull();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(2);
    }

    @Test
    public void reconcileChangesTargetClusterBrokerAddress() throws Exception {
        var downstreamBroker0 = HostPort.parse("localhost:9193");
        var upstreamBroker0 = HostPort.parse("upstream:19193");
        var upstreamBrokerUpdated0 = HostPort.parse("upstreamupd:29193");

        configureVirtualClusterMock(virtualCluster1, "localhost:9192", "upstream1:9192", false);

        var rgf = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(9192, false));
        assertThat(rgf.isDone()).isTrue();

        when(virtualCluster1.getBrokerAddress(0)).thenReturn(downstreamBroker0);
        var rcf1 = endpointRegistry.reconcile(virtualCluster1, Map.of(0, upstreamBroker0)).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(downstreamBroker0.port(), false));
        assertThat(rcf1.isDone()).isTrue();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(2);

        var resolvedBindingBeforeChange = endpointRegistry.resolve(Endpoint.createEndpoint(downstreamBroker0.port(), false), null).toCompletableFuture().get();
        assertThat(resolvedBindingBeforeChange).isEqualTo(new VirtualClusterBrokerBinding(virtualCluster1, upstreamBroker0, 0));

        // Target cluster updates the address for broker 0
        var rcf3 = endpointRegistry.reconcile(virtualCluster1, Map.of(0, upstreamBrokerUpdated0)).toCompletableFuture();
        verifyAndProcessNetworkEventQueue();
        assertThat(rcf3.isDone()).isTrue();
        assertThat(rcf3.get()).isNull();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(2);

        var resolvedBindingAfterChange = endpointRegistry.resolve(Endpoint.createEndpoint(downstreamBroker0.port(), false), null).toCompletableFuture().get();
        assertThat(resolvedBindingAfterChange).isEqualTo(new VirtualClusterBrokerBinding(virtualCluster1, upstreamBrokerUpdated0, 0));
    }

    @Test
    public void reconcileNoOp() throws Exception {
        var downstreamBroker0 = HostPort.parse("localhost:9193");
        var downstreamBroker1 = HostPort.parse("localhost:9194");
        var upstreamBroker0 = HostPort.parse("upstream:19193");
        var upstreamBroker1 = HostPort.parse("upstream:19194");

        configureVirtualClusterMock(virtualCluster1, "localhost:9192", "upstream1:9192", false);

        var rgf = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(9192, false));
        assertThat(rgf.isDone()).isTrue();

        // Add broker to the cluster
        when(virtualCluster1.getBrokerAddress(0)).thenReturn(downstreamBroker0);
        var rcf1 = endpointRegistry.reconcile(virtualCluster1, Map.of(0, upstreamBroker0)).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(9193, false));
        assertThat(rcf1.isDone()).isTrue();

        // Add 2nd broker to the cluster
        when(virtualCluster1.getBrokerAddress(1)).thenReturn(downstreamBroker1);
        var rcf2 = endpointRegistry.reconcile(virtualCluster1, Map.of(0, upstreamBroker0, 1, upstreamBroker1)).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(9194, false));
        assertThat(rcf2.isDone()).isTrue();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(3);

        var rcf3 = endpointRegistry.reconcile(virtualCluster1, Map.of(0, upstreamBroker0, 1, upstreamBroker1)).toCompletableFuture();
        assertThat(rcf3.isDone()).isTrue();
        assertThat(rcf3.get()).isNull();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(3);
    }

    @Test
    public void reconcileDeleteWhilstPreviousAddInFlight() throws Exception {
        var downstreamBroker0 = HostPort.parse("localhost:9193");
        var downstreamBroker1 = HostPort.parse("localhost:9194");
        var upstreamBroker0 = HostPort.parse("upstream:19193");
        var upstreamBroker1 = HostPort.parse("upstream:19194");

        configureVirtualClusterMock(virtualCluster1, "localhost:9192", "upstream1:9192", false);
        when(virtualCluster1.getBrokerAddress(0)).thenReturn(downstreamBroker0);
        when(virtualCluster1.getBrokerAddress(1)).thenReturn(downstreamBroker1);

        var rgf = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(9192, false));
        assertThat(rgf.isDone()).isTrue();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);

        // reconcile adds a node
        when(virtualCluster1.getBrokerAddress(0)).thenReturn(downstreamBroker0);
        var add1 = endpointRegistry.reconcile(virtualCluster1, Map.of(0, upstreamBroker0)).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(9193, false));
        assertThat(add1.isDone()).isTrue();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(2);

        // reconcile adds a node, but organise so the network event is not processed yet so the future won't complete
        when(virtualCluster1.getBrokerAddress(1)).thenReturn(downstreamBroker1);
        var add2 = endpointRegistry.reconcile(virtualCluster1, Map.of(0, upstreamBroker0, 1, upstreamBroker1)).toCompletableFuture();
        assertThat(add2.isDone()).isFalse();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(3);

        // reconcile now removes a node, it cannot be processed because it is behind the add
        var remove = endpointRegistry.reconcile(virtualCluster1, Map.of(1, upstreamBroker1)).toCompletableFuture();
        assertThat(remove.isDone()).isFalse();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(3);

        // process add event
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(9194, false));
        assertThat(add2.isDone()).isTrue();
        assertThat(add2.get()).isNull();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(3);
        assertThat(remove.isDone()).isFalse();

        // process remove event
        verifyAndProcessNetworkEventQueue(createTestNetworkUnbindRequest(9193, false));
        assertThat(remove.isDone()).isTrue();
        assertThat(remove.get()).isNull();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(2);
    }

    @Test
    public void reconcileFailsDueToExternalPortConflict() {
        var downstreamBroker0 = HostPort.parse("localhost:9193");
        var upstreamBroker0 = HostPort.parse("upstream:19193");
        doReconcileFailsDueToExternalPortConflict(downstreamBroker0, upstreamBroker0);
    }

    private VirtualCluster doReconcileFailsDueToExternalPortConflict(HostPort downstreamBroker0, HostPort upstreamBroker0) {
        configureVirtualClusterMock(virtualCluster1, "localhost:9192", "upstream1:9192", false);

        var rgf = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(9192, false));
        assertThat(rgf.isDone()).isTrue();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);

        // Add a new node (1) to the cluster
        when(virtualCluster1.getBrokerAddress(0)).thenReturn(downstreamBroker0);

        var rcf = endpointRegistry.reconcile(virtualCluster1, Map.of(0, upstreamBroker0)).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(
                createTestNetworkBindRequest(Optional.empty(), downstreamBroker0.port(), false, CompletableFuture.failedFuture(new IOException("mocked port in use"))));
        assertThat(rcf.isDone()).isTrue();
        var executionException = assertThrows(ExecutionException.class, rcf::get);
        assertThat(executionException).hasRootCauseInstanceOf(IOException.class);
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);

        return virtualCluster1;
    }

    @Test
    public void nextReconcileSucceedsAfterTransientPortConflict() throws Exception {
        var downstreamBroker0 = HostPort.parse("localhost:9193");
        var upstreamBroker0 = HostPort.parse("upstream:19193");
        var virtualCluster = doReconcileFailsDueToExternalPortConflict(downstreamBroker0, upstreamBroker0);

        var rcf = endpointRegistry.reconcile(virtualCluster, Map.of(0, upstreamBroker0)).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(downstreamBroker0.port(), false));

        assertThat(rcf.isDone()).isTrue();
        assertThat(rcf.get()).isNull();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(2);
    }

    private Channel createMockNettyChannel(int port) {
        var channel = mock(Channel.class);
        var attr = createTestAttribute(EndpointRegistry.CHANNEL_BINDINGS);
        when(channel.attr(EndpointRegistry.CHANNEL_BINDINGS)).thenReturn(attr);
        var localAddress = InetSocketAddress.createUnresolved("localhost", port); // This is lenient because not all tests exercise the unbind path
        lenient().when(channel.localAddress()).thenReturn(localAddress);
        return channel;
    }

    private NetworkBindRequest createTestNetworkBindRequest(int expectedPort, boolean expectedTls) {
        var channelMock = createMockNettyChannel(expectedPort);
        return createTestNetworkBindRequest(Optional.empty(), expectedPort, expectedTls, CompletableFuture.completedFuture(channelMock));
    }

    private NetworkBindRequest createTestNetworkBindRequest(Optional<String> expectedBindingAddress, int expectedPort, boolean expectedTls) {
        Objects.requireNonNull(expectedBindingAddress);
        var channelMock = createMockNettyChannel(expectedPort);
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

    private void configureVirtualClusterMock(VirtualCluster cluster, String downstreamBootstrap, String upstreamBootstrap, boolean tls) {
        configureVirtualClusterMock(cluster, downstreamBootstrap, upstreamBootstrap, tls, tls);
    }

    private void configureVirtualClusterMock(VirtualCluster cluster, String downstreamBootstrap, String upstreamBootstrap, boolean tls, boolean sni) {
        when(cluster.getClusterBootstrapAddress()).thenReturn(HostPort.parse(downstreamBootstrap));
        when(cluster.isUseTls()).thenReturn(tls);
        when(cluster.requiresTls()).thenReturn(sni);
        when(cluster.targetCluster()).thenReturn(new TargetCluster(upstreamBootstrap));
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

        public void verifyAndProcessNetworkEvents(NetworkBindingOperation... expectedEvents) {
            assertThat(queue.size()).as("unexpected number of events").isEqualTo(expectedEvents.length);
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
            var unused = source.handle((c, t) -> {
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

        }
    }
}