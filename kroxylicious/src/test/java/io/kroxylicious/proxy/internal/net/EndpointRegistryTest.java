/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.converter.ConvertWith;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.netty.channel.Channel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

import io.kroxylicious.proxy.HostPortConverter;
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
    private final EndpointRegistry endpointRegistry = new EndpointRegistry();
    @Mock(strictness = LENIENT)
    private VirtualCluster virtualCluster1;
    @Mock(strictness = LENIENT)
    private VirtualCluster virtualCluster2;

    @Test
    public void registerVirtualCluster() throws Exception {
        configureVirtualClusterMock(virtualCluster1, "mycluster1:9192", false);

        var f = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(null, 9192, false));
        assertThat(f.isDone()).isTrue();
        assertThat(f.get()).isEqualTo(Endpoint.createEndpoint(null, 9192, false));

        assertThat(endpointRegistry.isRegistered(virtualCluster1)).isTrue();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);
    }

    @Test
    public void registerVirtualClusterTls() throws Exception {
        configureVirtualClusterMock(virtualCluster1, "mycluster1:9192", true);

        var f = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(null, 9192, true));
        assertThat(f.isDone()).isTrue();
        assertThat(f.get()).isEqualTo(Endpoint.createEndpoint(null, 9192, true));
    }

    @Test
    public void registerSameVirtualClusterIsIdempotent() throws Exception {
        configureVirtualClusterMock(virtualCluster1, "mycluster1:9192", false);

        var f1 = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        var f2 = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(null, 9192, false));
        assertThat(CompletableFuture.allOf(f1, f2).isDone()).isTrue();
        assertThat(f2.get()).isEqualTo(Endpoint.createEndpoint(null, 9192, false));

        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);
    }

    @Test
    public void registerTwoClustersThatShareSameNetworkEndpoint() throws Exception {
        configureVirtualClusterMock(virtualCluster1, "mycluster1:9192", true);
        configureVirtualClusterMock(virtualCluster2, "mycluster2:9192", true);

        var f1 = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        var f2 = endpointRegistry.registerVirtualCluster(virtualCluster2).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(null, 9192, true));
        assertThat(CompletableFuture.allOf(f1, f2).isDone()).isTrue();
        assertThat(f1.get()).isEqualTo(Endpoint.createEndpoint(null, 9192, true));
        assertThat(f2.get()).isEqualTo(Endpoint.createEndpoint(null, 9192, true));

        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);
    }

    @Test
    public void registerTwoClustersThatUseDistinctNetworkEndpoints() throws Exception {
        configureVirtualClusterMock(virtualCluster1, "mycluster1:9191", true);
        configureVirtualClusterMock(virtualCluster2, "mycluster2:9192", true);

        var f1 = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        var f2 = endpointRegistry.registerVirtualCluster(virtualCluster2).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(null, 9191, true),
                createTestNetworkBindRequest(null, 9192, true));
        assertThat(CompletableFuture.allOf(f1, f2).isDone()).isTrue();
        assertThat(f1.get()).isEqualTo(Endpoint.createEndpoint(null, 9191, true));
        assertThat(f2.get()).isEqualTo(Endpoint.createEndpoint(null, 9192, true));

        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(2);
    }

    @Test
    public void registerVirtualClusterWithBrokerAddress() throws Exception {
        configureVirtualClusterMock(virtualCluster1, "localhost:9192", false);
        when(virtualCluster1.getNumberOfBrokerEndpointsToPrebind()).thenReturn(1);
        when(virtualCluster1.getBrokerAddress(0)).thenReturn(HostPort.parse("localhost:9193"));

        var f = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(null, 9192, false), createTestNetworkBindRequest(null, 9193, false));
        assertThat(f.isDone()).isTrue();
        assertThat(f.get()).isEqualTo(Endpoint.createEndpoint(null, 9192, false));
    }

    @Test
    public void registerRejectsDuplicatedBinding() throws Exception {
        configureVirtualClusterMock(virtualCluster1, "localhost:9191", false);
        configureVirtualClusterMock(virtualCluster2, "localhost:9191", false);

        var f1 = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(null, 9191, false));
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
        configureVirtualClusterMock(virtualCluster1, "localhost:9191", false);

        var f = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(null, 9191, false, CompletableFuture.failedFuture(new IOException("mocked port in use"))));
        assertThat(f.isDone()).isTrue();
        var executionException = assertThrows(ExecutionException.class, f::get);
        assertThat(executionException).hasRootCauseInstanceOf(IOException.class);

        assertThat(endpointRegistry.isRegistered(virtualCluster1)).isFalse();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(0);
    }

    @Test
    public void registerVirtualClusterFailsDueToExternalPortConflictOnSecondAddress() throws Exception {
        configureVirtualClusterMock(virtualCluster1, "localhost:9192", false);
        when(virtualCluster1.getNumberOfBrokerEndpointsToPrebind()).thenReturn(1);
        when(virtualCluster1.getBrokerAddress(0)).thenReturn(HostPort.parse("localhost:9193"));

        var f = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(null, 9192, false),
                createTestNetworkBindRequest(null, 9193, false, CompletableFuture.failedFuture(new IOException("mocked port in use"))));
        // it won't be done yet
        assertThat(f.isDone()).isFalse();

        // the port conflict on port 9193 should mean it tries to unbind 9192.
        verifyAndProcessNetworkEventQueue(createTestNetworkUnbindRequest(9192, false));

        assertThat(f.isDone()).isTrue();

        // now the unbinds are done, the future should be done.
        var executionException = assertThrows(ExecutionException.class, f::get);
        assertThat(executionException).hasRootCauseInstanceOf(IOException.class);

        assertThat(endpointRegistry.isRegistered(virtualCluster1)).isFalse();
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(0);
    }

    @Test
    public void deregisterVirtualCluster() throws Exception {
        configureVirtualClusterMock(virtualCluster1, "mycluster1:9192", true);

        var bindFuture = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(null, 9192, true));
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
        configureVirtualClusterMock(virtualCluster1, "mycluster1:9192", true);

        var bindFuture = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(null, 9192, true));
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
        configureVirtualClusterMock(virtualCluster1, "mycluster1:9191", true);
        configureVirtualClusterMock(virtualCluster2, "mycluster2:9191", true);

        var f1 = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        var f2 = endpointRegistry.registerVirtualCluster(virtualCluster2).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(null, 9191, true));
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
    public void deregisterVirtualClusterWithBrokerAddress() throws Exception {
        configureVirtualClusterMock(virtualCluster1, "localhost:9192", false);
        when(virtualCluster1.getNumberOfBrokerEndpointsToPrebind()).thenReturn(1);
        when(virtualCluster1.getBrokerAddress(0)).thenReturn(HostPort.parse("localhost:9193"));

        var f = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(null, 9192, false), createTestNetworkBindRequest(null, 9193, false));
        assertThat(f.isDone()).isTrue();
        assertThat(f.get()).isEqualTo(Endpoint.createEndpoint(null, 9192, false));

        var unbindFuture = endpointRegistry.deregisterVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkUnbindRequest(9193, false), createTestNetworkUnbindRequest(9192, false));
        assertThat(unbindFuture.isDone()).isTrue();
    }

    @Test
    public void reregisterClusterWhilstDeregisterIsInProgress() throws Exception {
        configureVirtualClusterMock(virtualCluster1, "mycluster1:9192", true);

        var r1 = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(null, 9192, true));
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
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(null, 9192, true));

        assertThat(rereg.isDone()).isTrue();
        assertThat(rereg.get()).isEqualTo(Endpoint.createEndpoint(null, 9192, true));
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);
    }

    @Test
    public void registerClusterWhileAnotherIsDeregistering() throws Exception {
        configureVirtualClusterMock(virtualCluster1, "mycluster1:9192", true);
        configureVirtualClusterMock(virtualCluster2, "mycluster2:9192", true);

        var r1 = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(null, 9192, true));
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
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(null, 9192, true));
        assertThat(r2.isDone()).isTrue();

        assertThat(CompletableFuture.allOf(d1, r2).isDone()).isTrue();
        assertThat(r2.get()).isEqualTo(Endpoint.createEndpoint(null, 9192, true));
        assertThat(endpointRegistry.listeningChannelCount()).isEqualTo(1);
    }

    @ParameterizedTest
    @CsvSource({ "mycluster1:9192,true,true", "mycluster1:9192,true,false", "localhost:9192,false,false" })
    public void resolveBootstrap(@ConvertWith(HostPortConverter.class) HostPort address, boolean tls, boolean sni) throws Exception {
        configureVirtualClusterMock(virtualCluster1, address.toString(), tls, sni);

        var f = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(null, address.port(), tls));
        assertThat(f.isDone()).isTrue();

        var binding = endpointRegistry.resolve(null, address.port(), tls ? address.host() : null, tls).toCompletableFuture().get();
        assertThat(binding).isEqualTo(VirtualClusterBinding.createBinding(virtualCluster1));
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource({ "mismatching host,mycluster1:9192,mycluster2:9192", "mistmatching port,mycluster1:9192,mycluster1:9191" })
    public void resolveBootstrapResolutionFailures(String name, @ConvertWith(HostPortConverter.class) HostPort address,
                                                   @ConvertWith(HostPortConverter.class) HostPort resolve)
            throws Exception {
        configureVirtualClusterMock(virtualCluster1, address.toString(), true);

        var f = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(null, 9192, true));
        assertThat(f.isDone()).isTrue();

        var executionException = assertThrows(ExecutionException.class,
                () -> endpointRegistry.resolve(null, resolve.port(), resolve.host(), true).toCompletableFuture().get());
        assertThat(executionException).hasCauseInstanceOf(EndpointResolutionException.class);
    }

    @ParameterizedTest
    @CsvSource({ "mycluster1:9192,MyClUsTeR1", "69.2.0.192.in-addr.arpa:9192,69.2.0.192.in-ADDR.ARPA" })
    public void resolveRespectsCaseInsensitivityRfc4343(@ConvertWith(HostPortConverter.class) HostPort address, String sniHostname) throws Exception {
        configureVirtualClusterMock(virtualCluster1, address.toString(), true);

        var f = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(null, address.port(), true));
        assertThat(f.isDone()).isTrue();

        var binding = endpointRegistry.resolve(null, address.port(), sniHostname, true).toCompletableFuture().get();
        assertThat(binding).isEqualTo(VirtualClusterBinding.createBinding(virtualCluster1));
    }

    @Test
    public void resolveBrokerAddress() throws Exception {
        configureVirtualClusterMock(virtualCluster1, "localhost:9192", false);
        when(virtualCluster1.getNumberOfBrokerEndpointsToPrebind()).thenReturn(1);
        when(virtualCluster1.getBrokerAddress(0)).thenReturn(HostPort.parse("localhost:9193"));

        var f = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(null, 9192, false), createTestNetworkBindRequest(null, 9193, false));
        assertThat(f.isDone()).isTrue();

        var binding = endpointRegistry.resolve(null, 9193, null, false).toCompletableFuture().get();
        assertThat(binding).isEqualTo(VirtualClusterBinding.createBinding(virtualCluster1, 0));
    }

    @Test
    public void bindingAddressEndpointSeparation() throws Exception {
        var bindingAddress1 = "127.0.0.1";
        configureVirtualClusterMock(virtualCluster1, "localhost:9192", false);
        when(virtualCluster1.getBindAddress()).thenReturn(Optional.of(bindingAddress1));

        var bindingAddress2 = "192.168.0.1";
        configureVirtualClusterMock(virtualCluster2, "myhost:9192", false);
        when(virtualCluster2.getBindAddress()).thenReturn(Optional.of(bindingAddress2));

        var f1 = endpointRegistry.registerVirtualCluster(virtualCluster1).toCompletableFuture();
        var f2 = endpointRegistry.registerVirtualCluster(virtualCluster2).toCompletableFuture();
        verifyAndProcessNetworkEventQueue(createTestNetworkBindRequest(bindingAddress1, 9192, false),
                createTestNetworkBindRequest(bindingAddress2, 9192, false));
        assertThat(CompletableFuture.allOf(f1, f2).isDone()).isTrue();

        var b1 = endpointRegistry.resolve(bindingAddress1, 9192, null, false).toCompletableFuture().get();
        assertThat(b1).isNotNull();
        assertThat(b1.virtualCluster()).isEqualTo(virtualCluster1);

        var b2 = endpointRegistry.resolve(bindingAddress2, 9192, null, false).toCompletableFuture().get();
        assertThat(b2).isNotNull();
        assertThat(b2.virtualCluster()).isEqualTo(virtualCluster2);

        var executionException = assertThrows(ExecutionException.class, () -> endpointRegistry.resolve(null, 9192, null, false).toCompletableFuture().get());
        assertThat(executionException).hasCauseInstanceOf(EndpointResolutionException.class);
    }

    private Channel createMockNettyChannel(int port) {
        var channel = mock(Channel.class);
        var attr = createTestAttribute(EndpointRegistry.CHANNEL_BINDINGS);
        when(channel.attr(EndpointRegistry.CHANNEL_BINDINGS)).thenReturn(attr);
        var localAddress = InetSocketAddress.createUnresolved("localhost", port); // This is lenient because not all tests exercise the unbind path
        lenient().when(channel.localAddress()).thenReturn(localAddress);
        return channel;
    }

    private NetworkBindRequest createTestNetworkBindRequest(String expectedBindingAddress, int expectedPort, boolean expectedTls) {
        var channelMock = createMockNettyChannel(expectedPort);
        return createTestNetworkBindRequest(expectedBindingAddress, expectedPort, expectedTls, CompletableFuture.completedFuture(channelMock));
    }

    private NetworkBindRequest createTestNetworkBindRequest(String expectedBindingAddress, int expectedPort, boolean expectedTls,
                                                            CompletableFuture<Channel> channelFuture) {
        return new NetworkBindRequest(expectedBindingAddress, expectedPort, expectedTls, channelFuture);
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

    private void configureVirtualClusterMock(VirtualCluster cluster, String address, boolean tls) {
        configureVirtualClusterMock(cluster, address, tls, tls);
    }

    private void configureVirtualClusterMock(VirtualCluster cluster, String address, boolean tls, boolean sni) {
        when(cluster.getClusterBootstrapAddress()).thenReturn(HostPort.parse(address));
        when(cluster.isUseTls()).thenReturn(tls);
        when(cluster.requiresTls()).thenReturn(sni);
    }

    @SuppressWarnings("unchecked")
    private void verifyAndProcessNetworkEventQueue(NetworkBindingOperation... expectedEvents) throws Exception {
        assertThat(endpointRegistry.countNetworkEvents()).as("unexpected number of events").isEqualTo(expectedEvents.length);
        var expectedEventIterator = Arrays.stream(expectedEvents).iterator();
        while (expectedEventIterator.hasNext()) {
            var expectedEvent = expectedEventIterator.next();
            if (endpointRegistry.countNetworkEvents() == 0) {
                fail("No network event available, expecting one matching " + expectedEvent);
            }
            var event = endpointRegistry.takeNetworkBindingEvent();
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
}