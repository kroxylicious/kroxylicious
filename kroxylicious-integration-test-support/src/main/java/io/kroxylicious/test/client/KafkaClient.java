/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.client;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.SSLException;
import javax.net.ssl.X509TrustManager;

import org.apache.kafka.common.message.RequestHeaderData;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import io.kroxylicious.test.Request;
import io.kroxylicious.test.Response;
import io.kroxylicious.test.ResponsePayload;
import io.kroxylicious.test.codec.DecodedRequestFrame;
import io.kroxylicious.test.codec.DecodedResponseFrame;
import io.kroxylicious.test.codec.KafkaRequestEncoder;
import io.kroxylicious.test.codec.KafkaResponseDecoder;
import io.kroxylicious.test.codec.RequestFrame;

/**
 * KafkaClient for testing.
 * <p>
 * The kafka client closes its channel after it gets a response. A new
 * connection is bootstrapped on every call to `get`.
 * </p>
 * <p>
 * The intention is that it should be used to fire a single RPC at a server
 * offering the Kafka protocol, read a response and inform the client of that
 * response. It currently translates the response to a normalised JsonObject.
 * </p>
 */
public final class KafkaClient implements AutoCloseable {

    public static final SslContext TRUST_ALL_CLIENT_SSL_CONTEXT = buildTrustAllSslContext();
    private final String host;
    private final int port;
    private final SslContext sslContext;

    private final AtomicReference<CompletableFuture<Channel>> connected = new AtomicReference<>();

    private final EventGroupConfig eventGroupConfig;
    private final EventLoopGroup bossGroup;
    private final CorrelationManager correlationManager;
    private final KafkaClientHandler kafkaClientHandler;

    public KafkaClient(String host, int port) {
        this(host, port, null);
    }

    /**
     * create empty kafkaClient
     *
     * @param host host to connect to
     * @param port port to connect to
     * @param clientSslContext client ssl context or null if TLS should not be used.
     */
    public KafkaClient(String host, int port, SslContext clientSslContext) {
        this.host = host;
        this.port = port;
        this.sslContext = clientSslContext;
        this.eventGroupConfig = EventGroupConfig.create();
        bossGroup = eventGroupConfig.newBossGroup();
        correlationManager = new CorrelationManager();
        kafkaClientHandler = new KafkaClientHandler();
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
     * @return a future that will be completed with the response from the kafka broker (translated to JsonNode)
     */
    public CompletableFuture<Response> get(Request request) {
        DecodedRequestFrame<?> decodedRequestFrame = toApiRequest(request);

        return ensureChannel(correlationManager, kafkaClientHandler)
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
     * @return a future that will be completed with the response from the kafka broker (translated to JsonNode)
     */
    public CompletableFuture<Response> get(RequestFrame frame) {
        return ensureChannel(correlationManager, kafkaClientHandler)
                .thenApply(KafkaClient::checkChannelOpen)
                .thenCompose(u -> kafkaClientHandler.sendRequest(frame))
                .thenApply(KafkaClient::toResponse);
    }

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

    private CompletableFuture<Channel> ensureChannel(CorrelationManager correlationManager, KafkaClientHandler kafkaClientHandler) {
        var candidate = new CompletableFuture<Channel>();

        if (connected.compareAndSet(null, candidate)) {
            Bootstrap b = new Bootstrap();
            b.group(bossGroup)
                    .channel(eventGroupConfig.clientChannelClass())
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {
                            ChannelPipeline p = ch.pipeline();
                            if (sslContext != null) {
                                p.addLast(sslContext.newHandler(ch.alloc(), host, port));
                            }
                            p.addLast(new KafkaRequestEncoder(correlationManager));
                            p.addLast(new KafkaResponseDecoder(correlationManager));
                            p.addLast(kafkaClientHandler);
                        }
                    });

            ChannelFuture connect = b.connect(host, port);
            connect.addListeners((ChannelFutureListener) channelFuture -> candidate.complete(channelFuture.channel()));
            connect.channel().closeFuture().addListener(future -> correlationManager.onChannelClose());
            return candidate;
        }
        else {
            return connected.get();
        }
    }

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

    @Override
    public void close() {
        CompletableFuture<Channel> channelCompletableFuture = connected.get();
        if (channelCompletableFuture != null) {
            channelCompletableFuture.thenApply(Channel::close);
        }
        bossGroup.shutdownGracefully();
    }

    private static Channel checkChannelOpen(Channel c) {
        if (!c.isOpen()) {
            throw new RuntimeException("Channel is already closed");
        }
        return c;
    }

    private static Response toResponse(SequencedResponse sequencedResponse) {
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

    private static class TrustingTrustManager implements X509TrustManager {

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {
            // we are trust all - nothing to do.
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            // we are trust all - nothing to do.
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }
}
