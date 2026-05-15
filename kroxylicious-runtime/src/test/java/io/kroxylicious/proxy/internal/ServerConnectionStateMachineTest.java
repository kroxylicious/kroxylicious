/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.channel.embedded.EmbeddedChannel;

import io.kroxylicious.proxy.internal.codec.KafkaRequestEncoder;
import io.kroxylicious.proxy.internal.codec.KafkaResponseDecoder;
import io.kroxylicious.proxy.internal.tls.ServerTlsCredentialSupplierContextImpl;
import io.kroxylicious.proxy.internal.tls.TestCertificateUtil;
import io.kroxylicious.proxy.internal.tls.TlsCredentialsImpl;
import io.kroxylicious.proxy.internal.util.ActivationToken;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.tls.ServerTlsCredentialSupplier;
import io.kroxylicious.proxy.tls.TlsCredentials;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ServerConnectionStateMachine}.
 */
class ServerConnectionStateMachineTest {

    private static final HostPort REMOTE = new HostPort("broker", 9092);
    private static final String CLUSTER_NAME = "test-cluster";

    private ServerConnectionStateMachine createScsm() {
        var ccsm = mock(ClientConnectionStateMachine.class);
        when(ccsm.sessionId()).thenReturn("test-session");
        when(ccsm.clusterName()).thenReturn(CLUSTER_NAME);
        var virtualCluster = mock(VirtualClusterModel.class);
        when(virtualCluster.getUpstreamSslContext()).thenReturn(Optional.empty());
        return new ServerConnectionStateMachine(
                REMOTE,
                ccsm,
                virtualCluster,
                CLUSTER_NAME,
                null,
                mock(Counter.class),
                mock(Timer.class),
                mock(ActivationToken.class));
    }

    @Test
    void sendRequestWhileConnectingShouldBuffer() {
        var scsm = createScsm();
        assertThat(scsm.state()).isInstanceOf(ServerConnectionState.Connecting.class);

        Object msg = new Object();
        scsm.sendRequest(msg);

        assertThat(scsm.serverMessagesInFlightCount).isZero();
    }

    @Test
    void sendRequestWhileActiveShouldForwardImmediately() {
        var scsm = createScsm();
        var channel = new EmbeddedChannel(scsm.backendHandler());
        assertThat(scsm.state()).isInstanceOf(ServerConnectionState.Active.class);

        Object msg = "test-request";
        scsm.sendRequest(msg);

        assertThat(scsm.serverMessagesInFlightCount).isEqualTo(1);
        assertThat(channel.<Object> readOutbound()).isEqualTo(msg);
    }

    @Test
    void onServerActiveShouldFlushPendingRequests() {
        var scsm = createScsm();

        scsm.sendRequest("req-1");
        scsm.sendRequest("req-2");
        assertThat(scsm.serverMessagesInFlightCount).isZero();

        // Registering the handler triggers channelActive → onServerActive → flush
        var channel = new EmbeddedChannel(scsm.backendHandler());
        assertThat(scsm.state()).isInstanceOf(ServerConnectionState.Active.class);
        assertThat(scsm.serverMessagesInFlightCount).isEqualTo(2);

        assertThat(channel.<Object> readOutbound()).isEqualTo("req-1");
        assertThat(channel.<Object> readOutbound()).isEqualTo("req-2");
        assertThat(channel.<Object> readOutbound()).isNull();
    }

    @Test
    void onServerActiveShouldFlushBeforePcsmCallback() {
        var ccsm = mock(ClientConnectionStateMachine.class);
        when(ccsm.sessionId()).thenReturn("test-session");
        when(ccsm.clusterName()).thenReturn(CLUSTER_NAME);
        var virtualCluster = mock(VirtualClusterModel.class);
        when(virtualCluster.getUpstreamSslContext()).thenReturn(Optional.empty());
        var scsm = new ServerConnectionStateMachine(
                REMOTE, ccsm, virtualCluster, CLUSTER_NAME, null,
                mock(Counter.class),
                mock(Timer.class), mock(ActivationToken.class));

        scsm.sendRequest("req-1");

        // channelActive → onServerActive → flush pending → ccsm callback
        new EmbeddedChannel(scsm.backendHandler());

        assertThat(scsm.serverMessagesInFlightCount).isEqualTo(1);
        verify(ccsm).onServerConnectionActive(scsm);
    }

    @Test
    void closedShouldReleasePendingRequests() {
        var scsm = createScsm();

        ByteBuf buf = Unpooled.buffer(4).writeInt(42);
        assertThat(buf.refCnt()).isEqualTo(1);
        scsm.sendRequest(buf);

        scsm.close();

        assertThat(buf.refCnt()).isZero();
    }

    @Test
    void closedWithNoPendingRequestsShouldNotFail() {
        var scsm = createScsm();

        scsm.close();

        assertThat(scsm.state()).isInstanceOf(ServerConnectionState.Closed.class);
    }

    @Test
    void exceptionWhileConnectingShouldReleasePendingRequests() {
        var scsm = createScsm();

        ByteBuf buf = Unpooled.buffer(4).writeInt(99);
        scsm.sendRequest(buf);
        assertThat(buf.refCnt()).isEqualTo(1);

        scsm.onServerException(new RuntimeException("connection failed"));

        assertThat(buf.refCnt()).isZero();
        assertThat(scsm.state()).isInstanceOf(ServerConnectionState.Closed.class);
    }

    @Test
    void onServerActiveWithNoPendingRequestsShouldNotFail() {
        var scsm = createScsm();
        assertThat(scsm.state()).isInstanceOf(ServerConnectionState.Connecting.class);

        new EmbeddedChannel(scsm.backendHandler());

        assertThat(scsm.state()).isInstanceOf(ServerConnectionState.Active.class);
        assertThat(scsm.serverMessagesInFlightCount).isZero();
    }

    @Test
    void pendingRequestsPreserveOrder() {
        var scsm = createScsm();

        for (int i = 0; i < 5; i++) {
            scsm.sendRequest("req-" + i);
        }

        var channel = new EmbeddedChannel(scsm.backendHandler());

        for (int i = 0; i < 5; i++) {
            assertThat(channel.<Object> readOutbound()).isEqualTo("req-" + i);
        }
        assertThat(channel.<Object> readOutbound()).isNull();
        assertThat(scsm.serverMessagesInFlightCount).isEqualTo(5);
    }

    // === connect() tests ===

    private ServerConnectionStateMachine createConnectableScsm(ClientConnectionStateMachine ccsm,
                                                               VirtualClusterModel virtualCluster,
                                                               EmbeddedChannel[] outboundHolder) {
        when(ccsm.sessionId()).thenReturn("test-session");
        when(ccsm.clusterName()).thenReturn(CLUSTER_NAME);
        when(virtualCluster.getUpstreamSslContext()).thenReturn(Optional.empty());
        when(virtualCluster.usesDynamicTlsCredentials()).thenReturn(false);
        when(virtualCluster.socketFrameMaxSizeBytes()).thenReturn(
                io.kroxylicious.proxy.model.VirtualClusterModel.DEFAULT_SOCKET_FRAME_MAX_SIZE_BYTES);
        return new ServerConnectionStateMachine(
                REMOTE, ccsm, virtualCluster, CLUSTER_NAME, null,
                mock(Counter.class), mock(Timer.class), mock(ActivationToken.class)) {
            @Override
            Bootstrap configureBootstrap(KafkaProxyBackendHandler backendHandler,
                                         Channel inboundChannel) {
                outboundHolder[0] = new EmbeddedChannel();
                Bootstrap bootstrap = new Bootstrap();
                bootstrap.group(outboundHolder[0].eventLoop())
                        .channel(outboundHolder[0].getClass())
                        .handler(backendHandler)
                        .option(ChannelOption.AUTO_READ, true)
                        .option(ChannelOption.TCP_NODELAY, true);
                return bootstrap;
            }

            @Override
            ChannelFuture initConnection(String remoteHost, int remotePort, Bootstrap bootstrap) {
                outboundHolder[0].pipeline().addFirst(backendHandler());
                return outboundHolder[0].newSucceededFuture();
            }
        };
    }

    @Test
    void connectInWrongStateShouldCallIllegalState() {
        var ccsm = mock(ClientConnectionStateMachine.class);
        var virtualCluster = mock(VirtualClusterModel.class);
        var scsm = createScsmWithMocks(ccsm, virtualCluster);

        // Force to Active state
        new EmbeddedChannel(scsm.backendHandler());
        assertThat(scsm.state()).isInstanceOf(ServerConnectionState.Active.class);

        // Calling connect() in Active state should trigger illegalState
        scsm.connect(mock(Channel.class));

        verify(ccsm).illegalState("connect() called while not in Connecting state");
    }

    @Test
    void connectShouldAssemblePipelineInCorrectOrder() {
        var ccsm = mock(ClientConnectionStateMachine.class);
        var virtualCluster = mock(VirtualClusterModel.class);
        var outboundHolder = new EmbeddedChannel[1];
        var scsm = createConnectableScsm(ccsm, virtualCluster, outboundHolder);

        scsm.connect(new EmbeddedChannel());

        var pipeline = outboundHolder[0].pipeline();
        List<String> handlerNames = pipeline.names().stream()
                .filter(n -> !n.contains("DefaultChannelPipeline"))
                .toList();

        // Pipeline uses addFirst, so the order in the list is the reverse of insertion order.
        // Expected from head to tail: networkLogger (absent), requestEncoder, responseDecoder,
        // frameLogger (absent), backendHandler, then tail sentinel.
        assertThat(handlerNames)
                .filteredOn(n -> !n.equals("DefaultChannelPipeline$TailContext#0"))
                .containsSubsequence("requestEncoder", "responseDecoder");
        assertThat(pipeline.get(KafkaRequestEncoder.class)).isNotNull();
        assertThat(pipeline.get(KafkaResponseDecoder.class)).isNotNull();
    }

    @Test
    void connectTcpFailureShouldCallOnServerException() {
        var ccsm = mock(ClientConnectionStateMachine.class);
        var virtualCluster = mock(VirtualClusterModel.class);
        when(ccsm.sessionId()).thenReturn("test-session");
        when(ccsm.clusterName()).thenReturn(CLUSTER_NAME);
        when(virtualCluster.getUpstreamSslContext()).thenReturn(Optional.empty());
        when(virtualCluster.usesDynamicTlsCredentials()).thenReturn(false);
        when(virtualCluster.socketFrameMaxSizeBytes()).thenReturn(
                io.kroxylicious.proxy.model.VirtualClusterModel.DEFAULT_SOCKET_FRAME_MAX_SIZE_BYTES);
        var tcpFailure = new RuntimeException("Connection refused");
        var scsm = new ServerConnectionStateMachine(
                REMOTE, ccsm, virtualCluster, CLUSTER_NAME, null,
                mock(Counter.class), mock(Timer.class), mock(ActivationToken.class)) {
            @Override
            Bootstrap configureBootstrap(KafkaProxyBackendHandler backendHandler,
                                         Channel inboundChannel) {
                var ch = new EmbeddedChannel();
                Bootstrap bootstrap = new Bootstrap();
                bootstrap.group(ch.eventLoop())
                        .channel(ch.getClass())
                        .handler(backendHandler)
                        .option(ChannelOption.AUTO_READ, true);
                return bootstrap;
            }

            @Override
            ChannelFuture initConnection(String remoteHost, int remotePort, Bootstrap bootstrap) {
                var ch = new EmbeddedChannel();
                return ch.newFailedFuture(tcpFailure);
            }
        };

        scsm.connect(new EmbeddedChannel());

        verify(ccsm).onServerConnectionException(scsm, tcpFailure);
        assertThat(scsm.state()).isInstanceOf(ServerConnectionState.Closed.class);
    }

    // === TLS credential tests ===

    private ServerConnectionStateMachine createScsmWithMocks(ClientConnectionStateMachine ccsm,
                                                             VirtualClusterModel virtualCluster) {
        when(ccsm.sessionId()).thenReturn("test-session");
        when(ccsm.clusterName()).thenReturn(CLUSTER_NAME);
        when(virtualCluster.getUpstreamSslContext()).thenReturn(Optional.empty());
        return new ServerConnectionStateMachine(
                REMOTE,
                ccsm,
                virtualCluster,
                CLUSTER_NAME,
                null,
                mock(Counter.class),
                mock(Timer.class),
                mock(ActivationToken.class));
    }

    @Test
    void invokeTlsCredentialSupplierReportsSynchronousFailure() throws Exception {
        var ccsm = mock(ClientConnectionStateMachine.class);
        var virtualCluster = mock(VirtualClusterModel.class);
        RuntimeException failure = new RuntimeException("manager failed");
        when(virtualCluster.getTlsCredentialSupplierManager()).thenThrow(failure);
        var scsm = createScsmWithMocks(ccsm, virtualCluster);

        Channel channel = mock(Channel.class);
        ChannelPipeline pipeline = mock(ChannelPipeline.class);
        Method method = ServerConnectionStateMachine.class.getDeclaredMethod(
                "invokeTlsCredentialSupplier", HostPort.class, Channel.class, ChannelPipeline.class);
        method.setAccessible(true);

        method.invoke(scsm, REMOTE, channel, pipeline);

        verify(ccsm).onServerConnectionException(scsm, failure);
    }

    @Test
    void requestTlsCredentialsAppliesCredentialsOnEventLoop() {
        var ccsm = mock(ClientConnectionStateMachine.class);
        var virtualCluster = mock(VirtualClusterModel.class);
        var scsm = createScsmWithMocks(ccsm, virtualCluster);

        TlsCredentials badCreds = mock(TlsCredentials.class);
        ServerTlsCredentialSupplier supplier = context -> CompletableFuture.completedFuture(badCreds);
        var supplierContext = new ServerTlsCredentialSupplierContextImpl(null);
        Channel channel = mock(Channel.class);
        EventLoop eventLoop = mock(EventLoop.class);
        when(channel.eventLoop()).thenReturn(eventLoop);
        doAnswer(invocation -> {
            invocation.getArgument(0, Runnable.class).run();
            return null;
        }).when(eventLoop).execute(any(Runnable.class));

        scsm.requestTlsCredentials(supplier, supplierContext, REMOTE, channel, mock(ChannelPipeline.class));

        ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        verify(ccsm).onServerConnectionException(any(ServerConnectionStateMachine.class), captor.capture());
        assertThat(captor.getValue())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Unexpected TlsCredentials implementation");
    }

    @Test
    void requestTlsCredentialsReportsSupplierFailureOnEventLoop() {
        var ccsm = mock(ClientConnectionStateMachine.class);
        var virtualCluster = mock(VirtualClusterModel.class);
        var scsm = createScsmWithMocks(ccsm, virtualCluster);

        RuntimeException failure = new RuntimeException("boom");
        ServerTlsCredentialSupplier supplier = context -> CompletableFuture.failedFuture(failure);
        var supplierContext = new ServerTlsCredentialSupplierContextImpl(null);
        Channel channel = mock(Channel.class);
        EventLoop eventLoop = mock(EventLoop.class);
        when(channel.eventLoop()).thenReturn(eventLoop);
        doAnswer(invocation -> {
            invocation.getArgument(0, Runnable.class).run();
            return null;
        }).when(eventLoop).execute(any(Runnable.class));

        scsm.requestTlsCredentials(supplier, supplierContext, REMOTE, channel, mock(ChannelPipeline.class));

        ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        verify(ccsm).onServerConnectionException(any(ServerConnectionStateMachine.class), captor.capture());
        assertThat(captor.getValue())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to obtain TLS credentials")
                .hasCause(failure);
    }

    @Test
    void handleTlsCredentialSupplierResultReportsNullCredentials() {
        var ccsm = mock(ClientConnectionStateMachine.class);
        var virtualCluster = mock(VirtualClusterModel.class);
        var scsm = createScsmWithMocks(ccsm, virtualCluster);

        Channel channel = mock(Channel.class);
        ChannelPipeline pipeline = mock(ChannelPipeline.class);

        scsm.handleTlsCredentialSupplierResult(null, null, REMOTE, channel, pipeline);

        ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        verify(ccsm).onServerConnectionException(any(ServerConnectionStateMachine.class), captor.capture());
        assertThat(captor.getValue())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("TLS credential supplier returned null");
    }

    @Test
    void handleTlsCredentialSupplierResultAppliesCredentials() {
        var ccsm = mock(ClientConnectionStateMachine.class);
        var virtualCluster = mock(VirtualClusterModel.class);
        var scsm = createScsmWithMocks(ccsm, virtualCluster);

        TlsCredentials badCreds = mock(TlsCredentials.class);
        Channel channel = mock(Channel.class);
        ChannelPipeline pipeline = mock(ChannelPipeline.class);

        scsm.handleTlsCredentialSupplierResult(badCreds, null, REMOTE, channel, pipeline);

        ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        verify(ccsm).onServerConnectionException(any(ServerConnectionStateMachine.class), captor.capture());
        assertThat(captor.getValue())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Unexpected TlsCredentials implementation");
    }

    @Test
    void applyTlsContextToChannelRejectsNonTlsCredentialsImpl() {
        var ccsm = mock(ClientConnectionStateMachine.class);
        var virtualCluster = mock(VirtualClusterModel.class);
        var scsm = createScsmWithMocks(ccsm, virtualCluster);

        TlsCredentials badCreds = mock(TlsCredentials.class);
        Channel channel = mock(Channel.class);
        ChannelPipeline pipeline = mock(ChannelPipeline.class);

        scsm.applyTlsContextToChannel(badCreds, REMOTE, channel, pipeline);

        ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        verify(ccsm).onServerConnectionException(any(ServerConnectionStateMachine.class), captor.capture());
        assertThat(captor.getValue())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Unexpected TlsCredentials implementation");
    }

    @Test
    void applyTlsContextToChannelAddsSslHandlerWithValidCredentials() throws Exception {
        var ccsm = mock(ClientConnectionStateMachine.class);
        var virtualCluster = mock(VirtualClusterModel.class);
        var scsm = createScsmWithMocks(ccsm, virtualCluster);

        var keyAndCert = TestCertificateUtil.generateKeyStoreAndCert();
        var creds = new TlsCredentialsImpl(
                keyAndCert.privateKey(), new java.security.cert.X509Certificate[]{ keyAndCert.cert() });

        EmbeddedChannel channel = new EmbeddedChannel();

        when(virtualCluster.targetCluster()).thenReturn(mock(io.kroxylicious.proxy.config.TargetCluster.class));
        when(virtualCluster.targetCluster().tls()).thenReturn(Optional.empty());

        scsm.applyTlsContextToChannel(creds, REMOTE, channel, channel.pipeline());

        assertThat(channel.pipeline().get("ssl")).isNotNull();
        verify(ccsm, never()).onServerConnectionException(any(), any());

        channel.close();
    }
}
