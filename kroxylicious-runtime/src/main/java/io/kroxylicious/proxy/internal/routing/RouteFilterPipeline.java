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
import io.netty.channel.EventLoopGroup;
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
 *
 * <h2>Why local transport?</h2>
 *
 * <p>Route-level filters must honour the same API contract as VC-level filters. This
 * means we need a real Netty {@link io.netty.channel.ChannelPipeline} so that
 * {@link FilterHandler} and its {@code InternalFilterContext} work unchanged —
 * including {@code sendRequest()}, deferred (async) filter work, backpressure via
 * auto-read, and response ordering.</p>
 *
 * <h2>Why a separate event loop group?</h2>
 *
 * <p>The proxy's client connections use platform-specific I/O transports (epoll on
 * Linux, kqueue on macOS). These transports' event loops only support channels backed
 * by OS file descriptors. {@link LocalChannel} and {@link LocalServerChannel} are
 * purely in-memory and have no fd, so they cannot be registered on an epoll/kqueue
 * event loop. We therefore create a shared {@link DefaultEventLoopGroup} for the
 * local transport infrastructure.</p>
 *
 * <h2>How filter threading guarantees are preserved</h2>
 *
 * <p>The Filter API guarantees that filter methods and their chained computation stages
 * execute on the connection's event loop thread. Although the local channels run on the
 * {@link DefaultEventLoopGroup}, we add the {@link FilterHandler} instances to the
 * pipeline using {@code pipeline.addLast(clientEventLoop, handler)}. This causes Netty
 * to dispatch all handler callbacks (channelRead, write, etc.) on the client's event
 * loop rather than the local channel's event loop. As a result, {@code ctx.executor()}
 * inside each {@link FilterHandler} returns the client connection's event loop, and
 * deferred work scheduled via {@code thenApplyAsync(..., ctx.executor())} completes on
 * the correct thread.</p>
 *
 * <h2>Lifecycle</h2>
 *
 * <p>One instance per route per client connection. Created lazily on first use via
 * {@link #create}. Creation is asynchronous because {@link ServerBootstrap} defers
 * bind/connect to the next event loop tick. The returned {@link CompletionStage}
 * completes once the local channel pair is fully established. The stage is cached by
 * the caller, so only the first request to a filtered route pays the setup cost.</p>
 *
 * <h2>Data flow</h2>
 *
 * <pre>
 * Request path:
 *   writeRequest() → filterChannel.pipeline().fireChannelRead(frame)
 *     → FilterHandler-1.channelRead() [on client event loop]
 *     → FilterHandler-2.channelRead() [on client event loop]
 *     → RouteFilterCompletionHandler → forwarder callback → CCSM → backend
 *
 * Response path:
 *   writeResponse() → filterChannel.writeAndFlush(frame)
 *     → FilterHandler-2.write() [on client event loop]
 *     → FilterHandler-1.write() [on client event loop]
 *     → exits to peer channel → ResponseCaptureHandler → completes future
 *
 * Short-circuit:
 *   A filter's shortCircuitResponse() writes a response during the request path.
 *   The response flows outbound through the remaining filters and exits to the
 *   peer channel, completing the future. The forwarder is never called.
 * </pre>
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
     * once the local channel pair (server + peer) is established and the child
     * channel's pipeline is populated with {@link FilterHandler} instances.
     *
     * <p>The {@code clientEventLoop} parameter is the event loop of the downstream
     * client connection. Filter handlers are bound to this event loop so that
     * {@code ctx.executor()} returns it, preserving the filter threading guarantee.</p>
     */
    static CompletionStage<RouteFilterPipeline> create(
                                                       EventLoopGroup localEventLoopGroup,
                                                       EventLoop clientEventLoop,
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

        // Captures the child channel created by the server when the peer connects.
        // We read this in the connect listener rather than relying on a handler's
        // handlerAdded() callback, which is dispatched to clientEventLoop and may
        // not have fired by the time the connect future resolves.
        var childChannelRef = new java.util.concurrent.atomic.AtomicReference<Channel>();

        // The server and peer channels run on localEventLoopGroup (a
        // DefaultEventLoopGroup) because local transport is incompatible with
        // platform-specific I/O event loops. The child channel initializer adds
        // FilterHandlers bound to clientEventLoop so filter code executes on the
        // connection's thread.
        ServerBootstrap sb = new ServerBootstrap()
                .group(localEventLoopGroup)
                .channel(LocalServerChannel.class)
                .childHandler(new ChannelInitializer<LocalChannel>() {
                    @Override
                    protected void initChannel(LocalChannel ch) {
                        childChannelRef.set(ch);
                        int i = 0;
                        for (FilterAndInvoker filter : filters) {
                            // Bind each FilterHandler to the client's event loop.
                            // This ensures ctx.executor() returns clientEventLoop
                            // inside the handler, preserving the filter threading
                            // guarantee for deferred work.
                            ch.pipeline().addLast(
                                    clientEventLoop,
                                    "route-filter-" + (++i) + "-" + filter.filterName(),
                                    new FilterHandler(filter, FILTER_TIMEOUT_MS, sniHostname,
                                            inboundChannel, ccsm));
                        }
                        ch.pipeline().addLast(
                                clientEventLoop,
                                "routeFilterCompletionHandler",
                                completionHandler);
                    }
                });

        sb.bind(address).addListener(bindFuture -> {
            if (!bindFuture.isSuccess()) {
                result.completeExceptionally(bindFuture.cause());
                return;
            }
            Channel serverChannel = ((io.netty.channel.ChannelFuture) bindFuture).channel();

            Bootstrap cb = new Bootstrap()
                    .group(localEventLoopGroup)
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

                Channel filterChannel = childChannelRef.get();
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
        closeFuture();
    }

    io.netty.channel.ChannelFuture closeFuture() {
        pendingResponseFutures.clear();
        io.netty.channel.ChannelFuture last = peerChannel.close();
        if (serverChannel != null) {
            last = serverChannel.close();
        }
        return last;
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
