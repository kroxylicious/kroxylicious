/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.test.server;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;

import org.hamcrest.Matcher;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;

import io.kroxylicious.test.Request;
import io.kroxylicious.test.ResponsePayload;
import io.kroxylicious.test.client.EventGroupConfig;
import io.kroxylicious.test.codec.DecodedRequestFrame;
import io.kroxylicious.test.codec.KafkaRequestDecoder;
import io.kroxylicious.test.codec.KafkaResponseEncoder;

/**
 * MockServer. Provides a mock kafka broker that can respond with a single
 * fixed ApiMessage at a time. Intended for per-RPC testing of kroxylicious where
 * we fire one RPC through the proxy, respond with some known message and then
 * check the output from the proxy.
 */
public final class MockServer implements AutoCloseable {
    private Channel channel;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private final int port;
    private MockHandler serverHandler;

    private MockServer(ResponsePayload response, int port, SslContext serverSslContext) {
        this.port = start(port, response, serverSslContext);
    }

    /**
     * Adds a response to be served by the MockServer if the request api key matches the response api key
     * @param response the response (nullable)
     */
    public void addMockResponseForApiKey(ResponsePayload response) {
        Objects.requireNonNull(response);
        serverHandler.setMockResponseForApiKey(response.apiKeys(), response.message(), response.responseApiVersion());
    }

    /**
     * Set the response to be served by the MockServer if the request matches.
     * @param response the response (nullable)
     */
    public void addMockResponse(Matcher<Request> requestMatcher, ResponsePayload response) {
        Objects.requireNonNull(response);
        Objects.requireNonNull(requestMatcher);
        serverHandler.addMockResponse(requestMatcher, Action.respond(response.message(), response.responseApiVersion()));
    }

    public void dropWhen(Matcher<Request> requestMatcher) {
        Objects.requireNonNull(requestMatcher);
        serverHandler.addMockResponse(requestMatcher, Action.drop());
    }

    /**
     * Get the requests received by the mock.
     * @return list of requests converted into a normalised JsonNode
     */
    public List<Request> getReceivedRequests() {
        return serverHandler.getRequests().stream().map(MockServer::toRequest).toList();
    }

    static Request toRequest(DecodedRequestFrame<?> decodedRequestFrame) {
        return new Request(decodedRequestFrame.apiKey(), decodedRequestFrame.apiVersion(), decodedRequestFrame.header().clientId(), decodedRequestFrame.body());
    }

    /**
     * Start mock server on a random port. Note a response must be set on it before
     * it receives any requests or mocking will fail.
     * @return the created server
     */
    public static MockServer startOnRandomPort() {
        return new MockServer(null, 0, null);
    }

    /**
     * Start mock server on a random port and serve this response
     * @param response response to serve
     * @return the created server
     */
    public static MockServer startOnRandomPort(ResponsePayload response) {
        return new MockServer(response, 0, null);
    }

    /**
     * Start mock server on a random port and serve this response
     * @param response response to serve
     * @param serverSslContext server ssl context, if null server will be plain.
     * @return the created server
     */
    public static MockServer startOnRandomPort(ResponsePayload response, SslContext serverSslContext) {
        return new MockServer(response, 0, serverSslContext);
    }

    /**
     * Start the server
     *
     * @param port port to bind to (0 to bind to an ephemeral)
     * @param response response to serve (nullable)
     * @param serverSslContext server ssl context, if null server will be plain.
     * @return the port bound to
     */
    public int start(int port, ResponsePayload response, SslContext serverSslContext) {
        if (serverSslContext != null && !serverSslContext.isServer()) {
            throw new IllegalArgumentException("if using SSL, a SslContext configured for server required.");
        }
        // Configure the server.
        final EventGroupConfig eventGroupConfig = EventGroupConfig.create();
        bossGroup = eventGroupConfig.newBossGroup();
        workerGroup = eventGroupConfig.newWorkerGroup();
        serverHandler = new MockHandler(response);
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
                .channel(eventGroupConfig.serverChannelClass())
                .option(ChannelOption.SO_BACKLOG, 100)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        if (serverSslContext != null) {
                            p.addLast(serverSslContext.newHandler(ch.alloc()));
                        }
                        p.addLast(new KafkaRequestDecoder());
                        p.addLast(new KafkaResponseEncoder());
                        p.addLast(serverHandler);
                    }
                });

        // Start the server.
        ChannelFuture f;
        try {
            f = b.bind(port).sync();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        // Wait until the server socket is closed.
        channel = f.channel();
        InetSocketAddress localAddress = (InetSocketAddress) channel.localAddress();
        return localAddress.getPort();
    }

    @Override
    public void close() {
        ChannelFuture channelFuture = channel.close();
        try {
            channelFuture.sync();
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
        catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ie);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * port mock server is listening on
     * @return port
     */
    public int port() {
        return port;
    }

    /**
     * Clear the response and tell the serverHandler to clear its collection of received requests.
     */
    public void clear() {
        serverHandler.clear();
    }

    public void assertAllMockInteractionsInvoked() {
        this.serverHandler.assertAllMockInteractionsInvoked();
    }
}
