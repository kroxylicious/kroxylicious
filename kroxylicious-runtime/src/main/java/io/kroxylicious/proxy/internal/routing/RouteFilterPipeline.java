/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.IntSupplier;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.internal.ClientConnectionStateMachine;
import io.kroxylicious.proxy.internal.FilterHandler;
import io.kroxylicious.proxy.internal.filter.FilterAndInvoker;
import io.kroxylicious.proxy.router.Response;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Manages a per-route filter pipeline backed by an {@link EmbeddedChannel}.
 * This provides real Netty pipeline mechanics so that {@link FilterHandler}
 * and its {@code InternalFilterContext} work unchanged.
 *
 * <p>Requests are written inbound (channelRead direction), passing through
 * each FilterHandler in declaration order. Responses are written outbound
 * (write direction), passing through FilterHandlers in reverse order.
 * Filtered responses exit via the outbound buffer and are read by this class
 * to complete the pending future.</p>
 *
 * <p>Lifecycle: one instance per route per client connection. Created lazily
 * on first use, closed when the client connection tears down.</p>
 */
class RouteFilterPipeline implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RouteFilterPipeline.class);
    private static final long FILTER_TIMEOUT_MS = 20_000;

    private final EmbeddedChannel filterChannel;
    private final RouteFilterCompletionHandler completionHandler;
    private final Map<Integer, CompletableFuture<Response>> pendingResponseFutures = new HashMap<>();

    /**
     * Creates a route filter pipeline.
     *
     * @param filters the route's filter instances
     * @param inboundChannel the client-facing channel (passed to FilterHandler for backpressure)
     * @param sniHostname SNI hostname from the client connection, or null
     * @param ccsm the client connection state machine
     * @param routeName the name of the route this pipeline belongs to
     * @param routingCorrelationIdAllocator allocator for routing correlation IDs
     * @param pendingResponseCount shared counter for pending responses
     * @param nodeIdMapping the node ID mapping for this route's router level
     * @param metadataAddressCacher caches broker addresses from METADATA responses
     */
    RouteFilterPipeline(
                        List<FilterAndInvoker> filters,
                        Channel inboundChannel,
                        @Nullable String sniHostname,
                        ClientConnectionStateMachine ccsm,
                        String routeName,
                        IntSupplier routingCorrelationIdAllocator,
                        AtomicInteger pendingResponseCount,
                        NodeIdMapping nodeIdMapping,
                        RouterDispatchHandler.MetadataAddressCacher metadataAddressCacher) {
        this.completionHandler = new RouteFilterCompletionHandler(
                routeName, routingCorrelationIdAllocator, inboundChannel,
                pendingResponseCount, nodeIdMapping, metadataAddressCacher);

        var handlers = new io.netty.channel.ChannelHandler[filters.size() + 1];
        int i = 0;
        for (FilterAndInvoker filter : filters) {
            handlers[i++] = new FilterHandler(
                    filter, FILTER_TIMEOUT_MS, sniHostname, inboundChannel, ccsm);
        }
        handlers[i] = completionHandler;
        this.filterChannel = new EmbeddedChannel(handlers);
    }

    /**
     * Writes a request through the route filter pipeline. The request passes through
     * each FilterHandler (channelRead direction). When it reaches the completion handler,
     * the forwarder is invoked to send the filtered request to the backend.
     *
     * <p>If a filter short-circuits with a response, the response flows back through
     * the filter pipeline (write direction) and the {@code responseFuture} is completed
     * from the outbound buffer.</p>
     *
     * @param frame the request frame (with routing correlation ID already set)
     * @param responseFuture the future to complete when the filtered response is available
     * @param forwarder callback invoked by the completion handler to forward the filtered
     *                  request to the backend
     */
    void writeRequest(
                      DecodedRequestFrame<?> frame,
                      CompletableFuture<Response> responseFuture,
                      Consumer<Object> forwarder) {
        pendingResponseFutures.put(frame.correlationId(), responseFuture);
        completionHandler.setCurrentForwarder(forwarder);
        filterChannel.writeInbound(frame);
        drainOutboundResponses();
    }

    /**
     * Writes a backend response through the route filter pipeline (outbound/write direction).
     * Each FilterHandler processes the response in reverse declaration order. After filtering,
     * the response exits to the outbound buffer where it is read and used to complete the
     * pending future.
     *
     * @param header the response header
     * @param body the response body
     * @param correlationId the routing correlation ID that identifies the pending future
     * @param apiVersion the API version of the original request
     */
    void writeResponse(
                       ResponseHeaderData header,
                       ApiMessage body,
                       int correlationId,
                       short apiVersion) {
        DecodedResponseFrame<?> responseFrame = new DecodedResponseFrame<>(
                apiVersion, correlationId, header, body);
        filterChannel.writeOutbound(responseFrame);
        drainOutboundResponses();
    }

    /**
     * Writes a response for an internal request (originated by a route filter's
     * {@code sendRequest()}) back through the filter pipeline. Creates an
     * {@code InternalResponseFrame} from the originating request frame so that
     * {@link FilterHandler} can match it to the filter's promise.
     *
     * @param originatingRequestFrame the original InternalRequestFrame
     * @param header the response header
     * @param body the response body
     */
    void writeInternalResponse(
                               DecodedRequestFrame<?> originatingRequestFrame,
                               ResponseHeaderData header,
                               ApiMessage body) {
        var responseFrame = originatingRequestFrame.responseFrame(header, body);
        filterChannel.writeOutbound(responseFrame);
        filterChannel.runPendingTasks();
    }

    @Override
    public void close() {
        filterChannel.close();
        pendingResponseFutures.clear();
    }

    private void drainOutboundResponses() {
        Object msg;
        while ((msg = filterChannel.readOutbound()) != null) {
            if (msg instanceof DecodedResponseFrame<?> frame) {
                CompletableFuture<Response> future = pendingResponseFutures.remove(
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
