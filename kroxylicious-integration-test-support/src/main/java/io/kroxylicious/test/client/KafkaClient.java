/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.client;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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

import io.kroxylicious.test.Request;
import io.kroxylicious.test.Response;
import io.kroxylicious.test.ResponsePayload;
import io.kroxylicious.test.codec.DecodedRequestFrame;
import io.kroxylicious.test.codec.DecodedResponseFrame;
import io.kroxylicious.test.codec.KafkaRequestEncoder;
import io.kroxylicious.test.codec.KafkaResponseDecoder;

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

    private final String host;
    private final int port;

    private final AtomicReference<CompletableFuture<Channel>> connected = new AtomicReference<>();

    private final EventGroupConfig eventGroupConfig;
    private final EventLoopGroup bossGroup;
    private final CorrelationManager correlationManager;
    private final KafkaClientHandler kafkaClientHandler;

    /**
     * create empty kafkaClient
     *
     * @param host host to connect to
     * @param port port to connect to
     */
    public KafkaClient(String host, int port) {
        this.host = host;
        this.port = port;
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
        return new DecodedRequestFrame<>(header.requestApiVersion(), header.correlationId(), header, request.message());
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
                     p.addLast(new KafkaRequestEncoder(correlationManager));
                     p.addLast(new KafkaResponseDecoder(correlationManager));
                     p.addLast(kafkaClientHandler);
                 }
             });

            ChannelFuture connect = b.connect(host, port);
            connect.addListeners((ChannelFutureListener) channelFuture -> candidate.complete(channelFuture.channel()));
            connect.channel().closeFuture().addListener(future -> {
                correlationManager.onChannelClose();
            });
            return candidate;
        } else {
            return connected.get();
        }
    }

    public boolean isOpen() {
        CompletableFuture<Channel> channelCompletableFuture = connected.get();
        if (channelCompletableFuture == null) {
            return false;
        } else {
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

}
