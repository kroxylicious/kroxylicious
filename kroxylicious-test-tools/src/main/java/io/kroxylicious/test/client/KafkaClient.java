/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.client;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;

import io.kroxylicious.test.Request;
import io.kroxylicious.test.Response;
import io.kroxylicious.test.codec.DecodedRequestFrame;
import io.kroxylicious.test.codec.DecodedResponseFrame;
import io.kroxylicious.test.codec.KafkaRequestEncoder;
import io.kroxylicious.test.codec.KafkaResponseDecoder;

import static java.util.stream.Collectors.toList;

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

    private final EventGroupConfig eventGroupConfig;
    private final EventLoopGroup bossGroup;

    /**
     * create empty kafkaClient
     * @param host host to connect to
     * @param port port to connect to
     */
    public KafkaClient(String host, int port) {
        this.host = host;
        this.port = port;
        this.eventGroupConfig = EventGroupConfig.create();
        bossGroup = eventGroupConfig.newBossGroup();
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
        return getAll(request).thenApply(response -> {
            if (response.size() != 1) {
                throw new IllegalStateException("expected one response");
            }
            return response.get(0);
        });
    }

    /**
     * Bootstrap and connect to a Kafka broker on a given host and port. Send
     * the requests to it and inform the client when we have received all responses.
     * The channel is closed after we have received the message.
     * @param requests request to send to kafka
     * @return a future that will be completed with the response from the kafka broker (translated to JsonNode)
     */
    public CompletableFuture<List<Response>> getAll(Request... requests) {
        List<DecodedRequestFrame<?>> decodedRequestFrames = Arrays.stream(requests).map(KafkaClient::toApiRequest).collect(toList());
        CorrelationManager correlationManager = new CorrelationManager();
        // if we are sending multiple requests then we need to behave like the kafka client, sending an initial request and
        // awaiting response, this prevents blowing up the kroxylicious state machine when there is more data available to
        // read than expected. After the connection is established this way we can send larger batches
        DecodedRequestFrame<?> initialRequest = requests.length > 1
                ? toApiRequest(new Request(ApiKeys.API_VERSIONS, ApiKeys.API_VERSIONS.latestVersion(), "client", new ApiVersionsRequestData()))
                : null;
        KafkaClientHandler kafkaClientHandler = new KafkaClientHandler(initialRequest, decodedRequestFrames);
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

        b.connect(host, port);
        CompletableFuture<List<DecodedResponseFrame<?>>> onResponseFuture = kafkaClientHandler.getOnResponseFuture();
        return onResponseFuture.thenApply(frames -> frames.stream().map(KafkaClient::toResponse).toList());
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

    public List<Response> getAllSync(Request... request) {
        try {
            return getAll(request).get(10, TimeUnit.SECONDS);
        }
        catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ie);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Response toResponse(DecodedResponseFrame<?> decodedResponseFrame) {
        return new Response(decodedResponseFrame.apiKey(), decodedResponseFrame.apiVersion(), decodedResponseFrame.body());
    }

    @Override
    public void close() {
        bossGroup.shutdownGracefully();
    }
}
