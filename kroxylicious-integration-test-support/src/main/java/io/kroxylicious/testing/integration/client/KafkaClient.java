/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.integration.client;

import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.X509TrustManager;

import org.apache.kafka.common.message.RequestHeaderData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyMessageEncoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;

import io.kroxylicious.testing.integration.Request;
import io.kroxylicious.testing.integration.Response;
import io.kroxylicious.testing.integration.ResponsePayload;
import io.kroxylicious.testing.integration.codec.DecodedRequestFrame;
import io.kroxylicious.testing.integration.codec.DecodedResponseFrame;
import io.kroxylicious.testing.integration.codec.KafkaRequestEncoder;
import io.kroxylicious.testing.integration.codec.KafkaResponseDecoder;
import io.kroxylicious.testing.integration.codec.RequestFrame;

import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A minimal Kafka-protocol client for integration testing.
 * <p>
 * Maintains a single persistent TCP connection to the target broker, established lazily
 * on the first {@link #get(Request)} call and reused for the lifetime of the client.
 * Multiple RPCs can be sent over the same connection in sequence.
 * </p>
 * <p>
 * Use {@link #withProxyProtocol(String, int, HAProxyMessage)} or
 * {@link #withProxyProtocol(String, int, HAProxyMessage, SslContext)} to obtain a client
 * that sends a PROXY protocol preamble as plain bytes before any Kafka (or TLS) traffic.
 * </p>
 * <p>
 * Must be closed via {@link #close()} when no longer needed to release the underlying
 * Netty event-loop resources.
 * </p>
 */
public final class KafkaClient implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaClient.class);

    /**
     * A client SSL context that trusts all server certificates. For use in tests only.
     */
    public static final SslContext TRUST_ALL_CLIENT_SSL_CONTEXT = buildTrustAllSslContext();
    private final String host;
    private final int port;
    private final SslContext sslContext;
    @Nullable
    private final HAProxyMessage proxyMessage;

    private final AtomicReference<CompletableFuture<Channel>> connected = new AtomicReference<>();

    private final EventGroupConfig eventGroupConfig;
    private final EventLoopGroup bossGroup;
    private final CorrelationManager correlationManager;
    private final KafkaClientHandler kafkaClientHandler;
    private final CertificateHandler certificateHandler;

    /**
     * create empty kafkaClient without TLS
     *
     * @param host host to connect to
     * @param port port to connect to
     */
    public KafkaClient(String host, int port) {
        this(host, port, null, null);
    }

    /**
     * create empty kafkaClient
     *
     * @param host host to connect to
     * @param port port to connect to
     * @param clientSslContext client ssl context or null if TLS should not be used.
     */
    public KafkaClient(String host, int port, SslContext clientSslContext) {
        this(host, port, clientSslContext, null);
    }

    private KafkaClient(String host, int port, @Nullable SslContext sslContext, @Nullable HAProxyMessage proxyMessage) {
        this.host = host;
        this.port = port;
        this.sslContext = sslContext;
        this.proxyMessage = proxyMessage;
        this.eventGroupConfig = EventGroupConfig.create();
        bossGroup = eventGroupConfig.newBossGroup();
        correlationManager = new CorrelationManager();
        kafkaClientHandler = new KafkaClientHandler();
        certificateHandler = new CertificateHandler();
    }

    /**
     * Creates a KafkaClient that emits a HAProxy PROXY protocol preamble before any Kafka traffic.
     *
     * @param host host to connect to
     * @param port port to connect to
     * @param proxyMessage the PROXY protocol message to send as the connection preamble
     * @return a new KafkaClient configured to send the preamble
     */
    public static KafkaClient withProxyProtocol(String host, int port, HAProxyMessage proxyMessage) {
        return new KafkaClient(host, port, null, proxyMessage);
    }

    /**
     * Creates a KafkaClient that emits a HAProxy PROXY protocol preamble and then negotiates TLS.
     * The preamble is sent as plain bytes before the TLS ClientHello, matching the wire order
     * that a proxy configured with {@code proxyProtocol.mode: required} expects.
     *
     * @param host host to connect to
     * @param port port to connect to
     * @param proxyMessage the PROXY protocol message to send as the connection preamble
     * @param sslContext client SSL context
     * @return a new KafkaClient configured to send the preamble then establish TLS
     */
    public static KafkaClient withProxyProtocol(String host, int port, HAProxyMessage proxyMessage,
                                                SslContext sslContext) {
        return new KafkaClient(host, port, sslContext, proxyMessage);
    }

    private static final AtomicInteger correlationId = new AtomicInteger(1);

    private static DecodedRequestFrame<?> toApiRequest(Request request) {
        var messageType = request.apiKeys().messageType;
        var header = new RequestHeaderData().setRequestApiKey(messageType.apiKey()).setRequestApiVersion(request.apiVersion());
        header.setClientId(request.clientIdHeader());
        header.setCorrelationId(correlationId.incrementAndGet());
        return new DecodedRequestFrame<>(header.requestApiVersion(), header.correlationId(), header, request.message(), request.responseApiVersion());
    }

    // TODO return a Response class with jsonObject() and frame() methods

    /**
     * Bootstrap and connect to a Kafka broker on a given host and port. Send
     * the request to it and inform the client when we have received a response.
     * The channel is closed after we have received the message.
     * @param request request to send to kafka
     * @return a future that will be completed with the response from the kafka broker (translated to JsonNode), or null if we sent a zero-ack produce request
     */
    public CompletableFuture<Response> get(Request request) {
        DecodedRequestFrame<?> decodedRequestFrame = toApiRequest(request);

        return ensureChannel(correlationManager)
                .thenApply(KafkaClient::checkChannelOpen)
                .thenCompose(u -> kafkaClientHandler.sendRequest(decodedRequestFrame))
                .thenApply(KafkaClient::toResponse);

    }

    /**
     * Bootstrap and connect to a Kafka broker on a given host and port. Send
     * the request to it and inform the client when we have received a response.
     * The channel is closed after we have received the message. Prefer {@link #get(Request)} for most cases,
     * this enables advanced cases like sending opaque frames.
     * @param frame to send to kafka
     * @return a future that will be completed with the response from the kafka broker (translated to JsonNode), or null if we sent a zero-ack produce request
     */
    public CompletableFuture<Response> get(RequestFrame frame) {
        return ensureChannel(correlationManager)
                .thenApply(KafkaClient::checkChannelOpen)
                .thenCompose(u -> kafkaClientHandler.sendRequest(frame))
                .thenApply(KafkaClient::toResponse);
    }

    /**
     * Sends the request as per {@link #get(Request)} and blocks awaiting the response.
     *
     * @param request request to send to kafka
     * @return the response from the kafka broker
     */
    public Response getSync(Request request) {
        try {
            return get(request).get(10, TimeUnit.SECONDS);
        }
        catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ie);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private CompletableFuture<Channel> ensureChannel(CorrelationManager correlationManager) {
        var candidate = new CompletableFuture<Channel>();

        if (connected.compareAndSet(null, candidate)) {
            var proxyPhaseFuture = new CompletableFuture<Void>();

            Bootstrap b = new Bootstrap();
            b.group(bossGroup)
                    .channel(eventGroupConfig.clientChannelClass())
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {
                            buildClientPipeline(ch, proxyPhaseFuture, candidate);
                        }
                    });

            ChannelFuture connect = b.connect(host, port);
            connect.addListener((ChannelFutureListener) channelFuture -> {
                if (!channelFuture.isSuccess()) {
                    proxyPhaseFuture.completeExceptionally(channelFuture.cause());
                }
                else if (proxyMessage == null) {
                    proxyPhaseFuture.complete(null);
                }
                // proxy case: ProxyPreambleSender completes proxyPhaseFuture after the write
            });
            connect.channel().closeFuture().addListener(future -> correlationManager.onChannelClose());
            return candidate;
        }
        else {
            return connected.get();
        }
    }

    private void buildClientPipeline(SocketChannel ch, CompletableFuture<Void> proxyPhaseFuture, CompletableFuture<Channel> candidate) {
        ChannelPipeline p = ch.pipeline();
        if (proxyMessage != null) {
            p.addLast(HAProxyMessageEncoder.INSTANCE);
            p.addLast(new ProxyPreambleSender(proxyPhaseFuture));
        }
        SslHandler sslHandler = null;
        if (sslContext != null) {
            sslHandler = sslContext.newHandler(ch.alloc(), host, port);
            p.addLast(sslHandler);
        }
        p.addLast(certificateHandler);
        wireReadySignal(proxyPhaseFuture, candidate, ch, sslHandler);
        p.addLast(new KafkaRequestEncoder(correlationManager));
        p.addLast(new KafkaResponseDecoder(correlationManager));
        p.addLast(kafkaClientHandler);
    }

    // FutureReturnValueIgnored: the whenComplete callback completes the candidate future on both
    // success and failure paths, so the returned future carries no additional information we need.
    @SuppressWarnings("FutureReturnValueIgnored")
    private static void wireReadySignal(CompletableFuture<Void> proxyPhaseFuture, CompletableFuture<Channel> candidate,
                                        Channel ch, @Nullable SslHandler sslHandler) {
        proxyPhaseFuture
                .thenCompose(v -> toHandshakeFuture(sslHandler))
                .whenComplete((v, ex) -> {
                    if (ex != null) {
                        candidate.completeExceptionally(ex);
                    }
                    else {
                        candidate.complete(ch);
                    }
                });
    }

    private static CompletableFuture<Void> toHandshakeFuture(@Nullable SslHandler sslHandler) {
        if (sslHandler == null) {
            return CompletableFuture.completedFuture(null);
        }
        var result = new CompletableFuture<Void>();
        sslHandler.handshakeFuture().addListener(hf -> {
            if (hf.isSuccess()) {
                result.complete(null);
            }
            else {
                result.completeExceptionally(hf.cause());
            }
        });
        return result;
    }

    /**
     * Whether the client's channel is connected and open.
     *
     * @return true if the channel is open, false otherwise
     */
    public boolean isOpen() {
        CompletableFuture<Channel> channelCompletableFuture = connected.get();
        if (channelCompletableFuture == null) {
            return false;
        }
        else {
            Channel now = channelCompletableFuture.getNow(null);
            return now != null && now.isOpen();
        }
    }

    /**
     * Certificates presented by the server when the client connected
     * @return one or more server certificates
     */
    public List<X509Certificate> getServerCertificateChain() {
        synchronized (certificateHandler.certificates) {
            return List.copyOf(certificateHandler.certificates);
        }
    }

    @Override
    public void close() {
        try {
            CompletableFuture<Channel> channelCompletableFuture = connected.get();
            if (channelCompletableFuture != null) {
                try {
                    channelCompletableFuture
                            .thenAccept(ch -> ch.close().syncUninterruptibly())
                            .get(5, TimeUnit.SECONDS);
                }
                catch (TimeoutException e) {
                    LOGGER.atWarn().log("Timed out after 5s waiting for Kafka test client channel to close; proceeding to group shutdown");
                }
                catch (ExecutionException e) {
                    LOGGER.atWarn().setCause(e.getCause()).log("Exception while closing Kafka test client channel");
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        finally {
            bossGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }

    private static Channel checkChannelOpen(Channel c) {
        if (!c.isOpen()) {
            throw new RuntimeException("Channel is already closed");
        }
        return c;
    }

    @Nullable
    private static Response toResponse(@Nullable SequencedResponse sequencedResponse) {
        if (sequencedResponse == null) {
            return null;
        }
        DecodedResponseFrame<?> frame = sequencedResponse.frame();
        return new Response(new ResponsePayload(frame.apiKey(), frame.apiVersion(), frame.body()), sequencedResponse.sequenceNumber());
    }

    private static SslContext buildTrustAllSslContext() {
        try {
            return SslContextBuilder.forClient().trustManager(new TrustingTrustManager()).build();
        }
        catch (SSLException e) {
            throw new RuntimeException("Failed to build SslContext", e);
        }
    }

    private class ProxyPreambleSender extends ChannelInboundHandlerAdapter {
        private final CompletableFuture<Void> phaseFuture;

        ProxyPreambleSender(CompletableFuture<Void> phaseFuture) {
            this.phaseFuture = phaseFuture;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            ctx.writeAndFlush(proxyMessage).addListener(writeFuture -> {
                ctx.pipeline().remove(HAProxyMessageEncoder.class);
                ctx.pipeline().remove(this);
                if (writeFuture.isSuccess()) {
                    ctx.fireChannelActive();
                    phaseFuture.complete(null);
                }
                else {
                    phaseFuture.completeExceptionally(writeFuture.cause());
                }
            });
        }
    }

    @SuppressWarnings("java:S4830") // TrustingTrustManager intentionally has a weak trust store
    @SuppressFBWarnings("WEAK_TRUST_MANAGER") // TrustingTrustManager intentionally has a weak trust store
    private static class TrustingTrustManager implements X509TrustManager {

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {
            // we are trust all - nothing to do.
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {
            // we are trust all - nothing to do.
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }

    // handler to allow server certificates to be inspected
    private static class CertificateHandler extends ChannelInboundHandlerAdapter {
        final List<X509Certificate> certificates = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) throws Exception {
            if (evt instanceof SslHandshakeCompletionEvent handshake && handshake.isSuccess()) {
                final SslHandler sslHandler = ctx.pipeline().get(SslHandler.class);
                if (Objects.nonNull(sslHandler)) { // ensure there is a SslHandler to work with
                    final SSLSession session = sslHandler.engine().getSession();
                    Arrays.stream(session.getPeerCertificates())
                            .filter(X509Certificate.class::isInstance)
                            .map(X509Certificate.class::cast)
                            .forEach(certificates::add);
                }
            }
        }
    }
}
