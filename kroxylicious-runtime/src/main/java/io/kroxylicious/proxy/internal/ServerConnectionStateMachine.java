/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.event.Level;
import org.slf4j.spi.LoggingEventBuilder;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.ReferenceCountUtil;

import io.kroxylicious.proxy.config.IllegalConfigurationException;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.config.tls.TrustOptions;
import io.kroxylicious.proxy.config.tls.TrustProvider;
import io.kroxylicious.proxy.internal.codec.CorrelationManager;
import io.kroxylicious.proxy.internal.codec.KafkaRequestEncoder;
import io.kroxylicious.proxy.internal.codec.KafkaResponseDecoder;
import io.kroxylicious.proxy.internal.metrics.MetricEmittingKafkaMessageListener;
import io.kroxylicious.proxy.internal.tls.ServerTlsCredentialSupplierContextImpl;
import io.kroxylicious.proxy.internal.tls.TlsCredentialsImpl;
import io.kroxylicious.proxy.internal.util.ActivationToken;
import io.kroxylicious.proxy.internal.util.Metrics;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.tag.VisibleForTesting;
import io.kroxylicious.proxy.tls.ClientTlsContext;
import io.kroxylicious.proxy.tls.ServerTlsCredentialSupplier;
import io.kroxylicious.proxy.tls.TlsCredentials;

import edu.umd.cs.findbugs.annotations.Nullable;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Manages the lifecycle of a single upstream (proxy-to-broker) TCP connection.
 * <p>
 * Extracted from {@link ClientConnectionStateMachine} to separate server-side connection
 * concerns from the client session. The CCSM retains client-side state and delegates
 * server operations here.
 *
 * <pre>
 *     Connecting ──→ Active ────────────→ Closed
 *         │             │            │
 *         └─────────────┴────────────┴──→ Closed (on error)
 * </pre>
 */
class ServerConnectionStateMachine {

    private static final Logger LOGGER = getLogger(ServerConnectionStateMachine.class);

    private ServerConnectionState state;

    private final ClientConnectionStateMachine ccsm;
    private final KafkaProxyBackendHandler backendHandler;
    private final VirtualClusterModel virtualCluster;
    private final String clusterName;
    @Nullable
    private final Integer nodeId;

    @VisibleForTesting
    int serverMessagesInFlightCount;

    int serverMessagesInFlightCount() {
        return serverMessagesInFlightCount;
    }

    @VisibleForTesting
    boolean serverReadsBlocked;

    @VisibleForTesting
    @Nullable
    Timer.Sample serverBackpressureTimer;

    @Nullable
    private List<Object> pendingRequests;

    private final Counter proxyToServerConnectionCounter;
    private final Counter proxyToServerErrorCounter;
    private final Timer serverToProxyBackpressureMeter;
    private final ActivationToken proxyToServerConnectionToken;

    @SuppressWarnings("java:S107")
    ServerConnectionStateMachine(
                                 HostPort remote,
                                 ClientConnectionStateMachine ccsm,
                                 VirtualClusterModel virtualCluster,
                                 String clusterName,
                                 @Nullable Integer nodeId,
                                 Counter proxyToServerConnectionCounter,
                                 Counter proxyToServerErrorCounter,
                                 Timer serverToProxyBackpressureMeter,
                                 ActivationToken proxyToServerConnectionToken) {
        this.state = new ServerConnectionState.Connecting(remote);
        this.virtualCluster = Objects.requireNonNull(virtualCluster);
        this.clusterName = Objects.requireNonNull(clusterName);
        this.nodeId = nodeId;
        this.ccsm = Objects.requireNonNull(ccsm);
        this.backendHandler = new KafkaProxyBackendHandler(this);
        this.proxyToServerConnectionCounter = proxyToServerConnectionCounter;
        this.proxyToServerErrorCounter = proxyToServerErrorCounter;
        this.serverToProxyBackpressureMeter = serverToProxyBackpressureMeter;
        this.proxyToServerConnectionToken = proxyToServerConnectionToken;
    }

    ServerConnectionState state() {
        return state;
    }

    KafkaProxyBackendHandler backendHandler() {
        return backendHandler;
    }

    boolean isUpstreamTls() {
        return virtualCluster.getUpstreamSslContext().isPresent();
    }

    /**
     * Initiates the TCP connection to the upstream broker.
     * Configures the backend channel pipeline (codecs, TLS, logging) and starts the connect.
     *
     * @param inboundChannel the client-side channel, used for event loop and channel class
     */
    void connect(Channel inboundChannel) {
        if (!(state instanceof ServerConnectionState.Connecting connecting)) {
            ccsm.illegalState("connect() called while not in Connecting state");
            return;
        }
        proxyToServerConnectionCounter.increment();
        HostPort remote = connecting.remote();
        final Bootstrap bootstrap = configureBootstrap(backendHandler, inboundChannel);

        log(Level.DEBUG)
                .addKeyValue("remote", remote)
                .log("Connecting to outbound");
        ChannelFuture serverTcpConnectFuture = initConnection(remote.host(), remote.port(), bootstrap);
        Channel outboundChannel = serverTcpConnectFuture.channel();
        ChannelPipeline pipeline = outboundChannel.pipeline();

        var correlationManager = new CorrelationManager();

        if (virtualCluster.isLogFrames()) {
            pipeline.addFirst("frameLogger",
                    new LoggingHandler("io.kroxylicious.proxy.internal.UpstreamFrameLogger", LogLevel.INFO));
        }

        var encoderListener = buildMetricsMessageListenerForEncode();
        var decoderListener = buildMetricsMessageListenerForDecode();

        pipeline.addFirst("responseDecoder",
                new KafkaResponseDecoder(correlationManager, virtualCluster.socketFrameMaxSizeBytes(), decoderListener));
        pipeline.addFirst("requestEncoder", new KafkaRequestEncoder(correlationManager, encoderListener));
        if (virtualCluster.isLogNetwork()) {
            pipeline.addFirst("networkLogger",
                    new LoggingHandler("io.kroxylicious.proxy.internal.UpstreamNetworkLogger", LogLevel.INFO));
        }

        if (virtualCluster.usesDynamicTlsCredentials()) {
            invokeTlsCredentialSupplier(remote, outboundChannel, pipeline);
        }
        else {
            virtualCluster.getUpstreamSslContext().ifPresent(sslContext -> {
                final SslHandler handler = sslContext.newHandler(outboundChannel.alloc(), remote.host(), remote.port());
                pipeline.addFirst("ssl", handler);
            });
        }

        log(Level.DEBUG)
                .addKeyValue("pipeline", pipeline)
                .log("Configured broker channel pipeline");

        serverTcpConnectFuture.addListener(future -> {
            if (future.isSuccess()) {
                log(Level.TRACE)
                        .log("Outbound connected");
            }
            else {
                onServerException(future.cause());
            }
        });
    }

    @VisibleForTesting
    Bootstrap configureBootstrap(
                                 KafkaProxyBackendHandler backendHandler,
                                 Channel inboundChannel) {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(inboundChannel.eventLoop())
                .channel(inboundChannel.getClass())
                .handler(backendHandler)
                .option(ChannelOption.AUTO_READ, true)
                .option(ChannelOption.TCP_NODELAY, true);
        return bootstrap;
    }

    @VisibleForTesting
    ChannelFuture initConnection(
                                 String remoteHost,
                                 int remotePort,
                                 Bootstrap bootstrap) {
        return bootstrap.connect(remoteHost, remotePort);
    }

    private void invokeTlsCredentialSupplier(
                                             HostPort remote,
                                             Channel outboundChannel,
                                             ChannelPipeline pipeline) {
        try {
            var manager = virtualCluster.getTlsCredentialSupplierManager();
            ServerTlsCredentialSupplier supplier = manager.getSupplier();

            ClientTlsContext clientCtx = ccsm.clientTlsContext().orElse(null);
            var supplierContext = new ServerTlsCredentialSupplierContextImpl(clientCtx);

            outboundChannel.eventLoop().execute(
                    () -> requestTlsCredentials(supplier, supplierContext, remote, outboundChannel, pipeline));
        }
        catch (Exception e) {
            log(Level.ERROR)
                    .addKeyValue("remote", remote)
                    .addKeyValue("error", e.getMessage())
                    .setCause(LOGGER.isDebugEnabled() ? e : null)
                    .log(LOGGER.isDebugEnabled()
                            ? "Error invoking TLS credential supplier"
                            : "Error invoking TLS credential supplier, increase log level to DEBUG for stacktrace");
            onServerException(e);
        }
    }

    @VisibleForTesting
    void requestTlsCredentials(
                               ServerTlsCredentialSupplier supplier,
                               ServerTlsCredentialSupplierContextImpl supplierContext,
                               HostPort remote,
                               Channel outboundChannel,
                               ChannelPipeline pipeline) {
        supplier.tlsCredentials(supplierContext).whenComplete(
                (credentials, throwable) -> outboundChannel.eventLoop().execute(
                        () -> handleTlsCredentialSupplierResult(credentials, throwable, remote, outboundChannel, pipeline)));
    }

    @VisibleForTesting
    void handleTlsCredentialSupplierResult(
                                           TlsCredentials credentials,
                                           Throwable throwable,
                                           HostPort remote,
                                           Channel outboundChannel,
                                           ChannelPipeline pipeline) {
        if (throwable != null) {
            handleTlsCredentialSupplierFailure(remote, throwable);
            return;
        }
        if (credentials == null) {
            onServerException(new IllegalStateException("TLS credential supplier returned null"));
            return;
        }
        applyTlsContextToChannel(credentials, remote, outboundChannel, pipeline);
    }

    private void handleTlsCredentialSupplierFailure(
                                                    HostPort remote,
                                                    Throwable throwable) {
        log(Level.ERROR)
                .addKeyValue("remote", remote)
                .addKeyValue("error", throwable.getMessage())
                .setCause(LOGGER.isDebugEnabled() ? throwable : null)
                .log(LOGGER.isDebugEnabled()
                        ? "TLS credential supplier failed"
                        : "TLS credential supplier failed, increase log level to DEBUG for stacktrace");
        onServerException(new IllegalStateException("Failed to obtain TLS credentials", throwable));
    }

    @VisibleForTesting
    void applyTlsContextToChannel(
                                  TlsCredentials credentials,
                                  HostPort remote,
                                  Channel outboundChannel,
                                  ChannelPipeline pipeline) {
        try {
            if (!(credentials instanceof TlsCredentialsImpl credentialsImpl)) {
                throw new IllegalStateException("Unexpected TlsCredentials implementation: " + credentials.getClass().getName());
            }

            SslContextBuilder sslContextBuilder = SslContextBuilder.forClient()
                    .keyManager(credentialsImpl.privateKey(), credentialsImpl.certificateChain());

            Optional.ofNullable(virtualCluster.targetCluster()).flatMap(TargetCluster::tls).ifPresent(tls -> {
                VirtualClusterModel.configureCipherSuites(sslContextBuilder, tls);
                VirtualClusterModel.configureEnabledProtocols(sslContextBuilder, tls);
                Optional.ofNullable(tls.trust())
                        .map(TrustProvider::trustOptions)
                        .filter(Predicate.not(TrustOptions::forClient))
                        .ifPresent(to -> {
                            throw new IllegalConfigurationException("Cannot apply trust options " + to + " to upstream (client) TLS.)");
                        });
                VirtualClusterModel.configureTrustProvider(tls).apply(sslContextBuilder);
            });

            SslContext sslContext = sslContextBuilder.build();

            final SslHandler handler = sslContext.newHandler(outboundChannel.alloc(), remote.host(), remote.port());
            pipeline.addFirst("ssl", handler);

            log(Level.DEBUG)
                    .addKeyValue("remote", remote)
                    .log("Successfully configured dynamic TLS credentials");
        }
        catch (Exception e) {
            log(Level.ERROR)
                    .addKeyValue("remote", remote)
                    .addKeyValue("error", e.getMessage())
                    .setCause(LOGGER.isDebugEnabled() ? e : null)
                    .log(LOGGER.isDebugEnabled()
                            ? "Error applying TLS credentials to channel"
                            : "Error applying TLS credentials to channel, increase log level to DEBUG for stacktrace");
            onServerException(e);
        }
    }

    private MetricEmittingKafkaMessageListener buildMetricsMessageListenerForEncode() {
        var proxyToServerMessageCounterProvider = Metrics.proxyToServerMessageCounterProvider(clusterName, nodeId);
        var proxyToServerMessageSizeDistributionProvider = Metrics.proxyToServerMessageSizeDistributionProvider(clusterName, nodeId);
        return new MetricEmittingKafkaMessageListener(proxyToServerMessageCounterProvider, proxyToServerMessageSizeDistributionProvider);
    }

    private io.kroxylicious.proxy.internal.codec.KafkaMessageListener buildMetricsMessageListenerForDecode() {
        var serverToProxyMessageCounterProvider = Metrics.serverToProxyMessageCounterProvider(clusterName, nodeId);
        var serverToProxyMessageSizeDistributionProvider = Metrics.serverToProxyMessageSizeDistributionProvider(clusterName, nodeId);
        return new MetricEmittingKafkaMessageListener(serverToProxyMessageCounterProvider, serverToProxyMessageSizeDistributionProvider);
    }

    void onServerActive() {
        if (state instanceof ServerConnectionState.Connecting connecting) {
            setState(connecting.toActive());
            proxyToServerConnectionToken.acquire();
            flushPendingRequests();
            ccsm.onServerConnectionActive();
        }
        else {
            ccsm.illegalState("Server became active while not in the connecting state");
        }
    }

    void onServerInactive() {
        if (!(state instanceof ServerConnectionState.Closed)) {
            toClosed();
            ccsm.onServerConnectionClosed(ClientConnectionStateMachine.DisconnectCause.SERVER_CLOSED);
        }
    }

    @SuppressWarnings("java:S5738")
    void onServerException(@Nullable Throwable cause) {
        if (!(state instanceof ServerConnectionState.Closed)) {
            log(Level.WARN)
                    .addKeyValue("error", cause != null ? cause.getMessage() : "")
                    .setCause(LOGGER.isDebugEnabled() ? cause : null)
                    .log(LOGGER.isDebugEnabled()
                            ? "exception from server channel"
                            : "exception from server channel, increase log level to DEBUG for stacktrace");
            proxyToServerErrorCounter.increment();
            toClosed();
            ccsm.onServerConnectionException(cause);
        }
    }

    void onMessageFromServer(Object msg) {
        serverMessagesInFlightCount = Math.max(0, serverMessagesInFlightCount - 1);
        ccsm.onResponseFromServer(msg);
    }

    void serverReadComplete() {
        ccsm.onServerReadComplete();
    }

    void onServerUnwritable() {
        ccsm.onServerUnwritable();
    }

    void onServerWritable() {
        ccsm.onServerWritable();
    }

    void sendRequest(Object msg) {
        if (state instanceof ServerConnectionState.Connecting) {
            if (pendingRequests == null) {
                pendingRequests = new ArrayList<>();
            }
            pendingRequests.add(msg);
            return;
        }
        serverMessagesInFlightCount++;
        backendHandler.forwardToServer(msg);
        backendHandler.flushToServer();
    }

    private void flushPendingRequests() {
        if (pendingRequests != null) {
            for (Object msg : pendingRequests) {
                serverMessagesInFlightCount++;
                backendHandler.forwardToServer(msg);
            }
            pendingRequests = null;
            backendHandler.flushToServer();
        }
    }

    void applyBackpressure() {
        if (!serverReadsBlocked) {
            serverReadsBlocked = true;
            serverBackpressureTimer = Timer.start();
            backendHandler.applyBackpressure();
        }
    }

    void relieveBackpressure() {
        if (serverReadsBlocked) {
            serverReadsBlocked = false;
            if (serverBackpressureTimer != null) {
                serverBackpressureTimer.stop(serverToProxyBackpressureMeter);
                serverBackpressureTimer = null;
            }
            backendHandler.relieveBackpressure();
        }
    }

    void close() {
        if (!(state instanceof ServerConnectionState.Closed)) {
            toClosed();
        }
    }

    private void toClosed() {
        releasePendingRequests();
        setState(new ServerConnectionState.Closed());
        backendHandler.inClosed();
        proxyToServerConnectionToken.release();
    }

    private void releasePendingRequests() {
        if (pendingRequests != null) {
            for (Object msg : pendingRequests) {
                ReferenceCountUtil.release(msg);
            }
            pendingRequests = null;
        }
    }

    private void setState(ServerConnectionState newState) {
        log(Level.TRACE)
                .addKeyValue("targetState", newState)
                .log("Server connection transitioning to state");
        this.state = newState;
    }

    private LoggingEventBuilder log(Level level) {
        LoggingEventBuilder builder = switch (level) {
            case ERROR -> LOGGER.atError();
            case WARN -> LOGGER.atWarn();
            case INFO -> LOGGER.atInfo();
            case DEBUG -> LOGGER.atDebug();
            case TRACE -> LOGGER.atTrace();
        };
        return builder
                .addKeyValue("sessionId", ccsm.sessionId())
                .addKeyValue("virtualCluster", clusterName);
    }

    @Override
    public String toString() {
        return "ServerConnectionStateMachine{" +
                "state=" + state +
                ", serverReadsBlocked=" + serverReadsBlocked +
                ", serverMessagesInFlightCount=" + serverMessagesInFlightCount +
                '}';
    }
}
