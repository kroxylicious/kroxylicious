/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.IntSupplier;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoop;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.internal.ClientConnectionStateMachine;
import io.kroxylicious.proxy.internal.FilterHandler;
import io.kroxylicious.proxy.internal.filter.FilterAndInvoker;
import io.kroxylicious.proxy.router.Response;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Manages a per-route filter pipeline backed by Netty's local (in-memory) transport.
 * A local channel pair provides a real Netty pipeline on the client connection's
 * event loop, so {@link FilterHandler} and its {@code InternalFilterContext} work
 * unchanged — including correct handling of deferred filter work.
 *
 * <p>Creation is asynchronous because local channel setup defers child channel
 * creation to the next event loop tick. Use {@link #create} to obtain a
 * {@code CompletionStage} that completes when the pipeline is ready.</p>
 *
 * <p>Lifecycle: one instance per route per client connection. Created lazily
 * on first use, closed when the client connection tears down.</p>
 */
class RouteFilterPipeline implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RouteFilterPipeline.class);
    private static final long FILTER_TIMEOUT_MS = 20_000;

    private final Channel serverChannel;
    private final Channel filterChannel;
    private final Channel peerChannel;
    private final RouteFilterCompletionHandler completionHandler;
    private final Map<Integer, CompletableFuture<Response>> pendingResponseFutures = new HashMap<>();

    private RouteFilterPipeline(
                                Channel serverChannel,
                                Channel filterChannel,
                                Channel peerChannel,
                                RouteFilterCompletionHandler completionHandler) {
        this.serverChannel = serverChannel;
        this.filterChannel = filterChannel;
        this.peerChannel = peerChannel;
        this.completionHandler = completionHandler;
    }

    /**
     * Creates a route filter pipeline asynchronously. The returned stage completes
     * on the next event loop tick when the local channel pair is fully established.
     */
    static CompletionStage<RouteFilterPipeline> create(
                                                       EventLoop eventLoop,
                                                       List<FilterAndInvoker> filters,
                                                       Channel inboundChannel,
                                                       @Nullable String sniHostname,
                                                       ClientConnectionStateMachine ccsm,
                                                       String routeName,
                                                       IntSupplier routingCorrelationIdAllocator,
                                                       AtomicInteger pendingResponseCount,
                                                       NodeIdMapping nodeIdMapping,
                                                       RouterDispatchHandler.MetadataAddressCacher metadataAddressCacher) {

        var completionHandler = new RouteFilterCompletionHandler(
                routeName, routingCorrelationIdAllocator, inboundChannel,
                pendingResponseCount, nodeIdMapping, metadataAddressCacher);

        LocalAddress address = new LocalAddress("route-filter-" + UUID.randomUUID());
        CompletableFuture<RouteFilterPipeline> result = new CompletableFuture<>();

        ServerBootstrap sb = new ServerBootstrap()
                .group(eventLoop)
                .channel(LocalServerChannel.class)
                .childHandler(new ChannelInitializer<LocalChannel>() {
                    @Override
                    protected void initChannel(LocalChannel ch) {
                        int i = 0;
                        for (FilterAndInvoker filter : filters) {
                            ch.pipeline().addLast(
                                    "route-filter-" + (++i) + "-" + filter.filterName(),
                                    new FilterHandler(filter, FILTER_TIMEOUT_MS, sniHostname,
                                            inboundChannel, ccsm));
                        }
                        ch.pipeline().addLast("routeFilterCompletionHandler", completionHandler);
                    }
                });

        sb.bind(address).addListener(bindFuture -> {
            if (!bindFuture.isSuccess()) {
                result.completeExceptionally(bindFuture.cause());
                return;
            }
            Channel serverChannel = ((io.netty.channel.ChannelFuture) bindFuture).channel();

            Bootstrap cb = new Bootstrap()
                    .group(eventLoop)
                    .channel(LocalChannel.class)
                    .handler(new ChannelInitializer<LocalChannel>() {
                        @Override
                        protected void initChannel(LocalChannel ch) {
                            ch.pipeline().addLast("responseCaptureHandler",
                                    new ResponseCaptureHandler(result));
                        }
                    });

            cb.connect(address).addListener(connectFuture -> {
                if (!connectFuture.isSuccess()) {
                    serverChannel.close();
                    result.completeExceptionally(connectFuture.cause());
                    return;
                }
                Channel peerChannel = ((io.netty.channel.ChannelFuture) connectFuture).channel();

                // The child channel is the server-accepted channel with FilterHandlers.
                // Find it via the completion handler's pipeline context.
                Channel filterChannel = completionHandler.channel();
                if (filterChannel == null) {
                    serverChannel.close();
                    peerChannel.close();
                    result.completeExceptionally(new IllegalStateException(
                            "Route filter child channel not available after connect"));
                    return;
                }

                var pipeline = new RouteFilterPipeline(
                        serverChannel, filterChannel, peerChannel, completionHandler);
                completionHandler.setOwningPipeline(pipeline);
                result.complete(pipeline);
            });
        });

        return result;
    }

    /**
     * Writes a request through the route filter pipeline. The request passes through
     * each FilterHandler (channelRead direction). When it reaches the completion handler,
     * the forwarder is invoked to send the filtered request to the backend.
     *
     * <p>If a filter short-circuits with a response, the response flows back through
     * the filter pipeline (write direction) and the {@code responseFuture} is completed
     * by the peer channel's capture handler.</p>
     */
    void writeRequest(
                      DecodedRequestFrame<?> frame,
                      CompletableFuture<Response> responseFuture,
                      Consumer<Object> forwarder) {
        pendingResponseFutures.put(frame.correlationId(), responseFuture);
        completionHandler.setCurrentForwarder(forwarder);
        filterChannel.pipeline().fireChannelRead(frame);
    }

    /**
     * Writes a fire-and-forget request through the route filter pipeline. The request
     * passes through request filters but no response is expected.
     */
    void writeFireAndForget(
                            DecodedRequestFrame<?> frame,
                            Consumer<Object> forwarder) {
        completionHandler.setCurrentForwarder(forwarder);
        filterChannel.pipeline().fireChannelRead(frame);
    }

    /**
     * Writes a backend response through the route filter pipeline (outbound/write direction).
     * Each FilterHandler processes the response in reverse declaration order. After filtering,
     * the response exits to the peer channel where the capture handler completes the pending
     * future.
     */
    void writeResponse(
                       ResponseHeaderData header,
                       ApiMessage body,
                       int correlationId,
                       short apiVersion) {
        DecodedResponseFrame<?> responseFrame = new DecodedResponseFrame<>(
                apiVersion, correlationId, header, body);
        filterChannel.writeAndFlush(responseFrame);
    }

    /**
     * Writes a response for an internal request (originated by a route filter's
     * {@code sendRequest()}) back through the filter pipeline. Creates an
     * {@code InternalResponseFrame} from the originating request frame so that
     * {@link FilterHandler} can match it to the filter's promise.
     */
    void writeInternalResponse(
                               DecodedRequestFrame<?> originatingRequestFrame,
                               ResponseHeaderData header,
                               ApiMessage body) {
        var responseFrame = originatingRequestFrame.responseFrame(header, body);
        filterChannel.writeAndFlush(responseFrame);
    }

    @Override
    public void close() {
        if (peerChannel != null) {
            peerChannel.close();
        }
        if (serverChannel != null) {
            serverChannel.close();
        }
        pendingResponseFutures.clear();
    }

    /**
     * Handles filtered responses arriving on the peer (client-side) local channel.
     * Looks up the pending future by correlation ID and completes it.
     */
    private static class ResponseCaptureHandler extends ChannelInboundHandlerAdapter {
        private final CompletableFuture<RouteFilterPipeline> pipelineReady;

        ResponseCaptureHandler(CompletableFuture<RouteFilterPipeline> pipelineReady) {
            this.pipelineReady = pipelineReady;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof DecodedResponseFrame<?> frame) {
                RouteFilterPipeline pipeline = pipelineReady.getNow(null);
                if (pipeline == null) {
                    LOGGER.atWarn()
                            .log("Response arrived before pipeline was ready");
                    return;
                }
                CompletableFuture<Response> future = pipeline.pendingResponseFutures.remove(
                        frame.correlationId());
                if (future != null) {
                    future.complete(new ResponseImpl(
                            (ResponseHeaderData) frame.header(), frame.body()));
                }
                else {
                    LOGGER.atWarn()
                            .addKeyValue("correlationId", frame.correlationId())
                            .log("Route filter pipeline received response with no pending future");
                }
            }
        }
    }
}
