/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.IntSupplier;

import org.apache.kafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Timer;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.internal.InternalRequestFrame;
import io.kroxylicious.proxy.router.Response;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Tail handler in a per-route filter pipeline. Receives filtered request frames
 * and invokes a forwarder callback to send them to the backend.
 *
 * <p>For regular requests, the forwarder is set per-request via {@link #setCurrentForwarder}.
 * For {@link InternalRequestFrame}s (from filter {@code sendRequest()}), the handler
 * allocates a routing correlation ID, registers a {@code PendingResponse} with the
 * originating frame reference, and forwards via the same forwarder.</p>
 */
class RouteFilterCompletionHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(RouteFilterCompletionHandler.class);

    private final String routeName;
    private final IntSupplier routingCorrelationIdAllocator;
    private final Channel clientChannel;
    private final AtomicInteger pendingResponseCount;
    private final NodeIdMapping nodeIdMapping;
    private final RouterDispatchHandler.MetadataAddressCacher metadataAddressCacher;

    @Nullable
    private Consumer<Object> currentForwarder;

    RouteFilterCompletionHandler(
                                 String routeName,
                                 IntSupplier routingCorrelationIdAllocator,
                                 Channel clientChannel,
                                 AtomicInteger pendingResponseCount,
                                 NodeIdMapping nodeIdMapping,
                                 RouterDispatchHandler.MetadataAddressCacher metadataAddressCacher) {
        this.routeName = Objects.requireNonNull(routeName);
        this.routingCorrelationIdAllocator = Objects.requireNonNull(routingCorrelationIdAllocator);
        this.clientChannel = Objects.requireNonNull(clientChannel);
        this.pendingResponseCount = Objects.requireNonNull(pendingResponseCount);
        this.nodeIdMapping = Objects.requireNonNull(nodeIdMapping);
        this.metadataAddressCacher = Objects.requireNonNull(metadataAddressCacher);
    }

    void setCurrentForwarder(Consumer<Object> forwarder) {
        this.currentForwarder = Objects.requireNonNull(forwarder);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof InternalRequestFrame<?> irf) {
            handleInternalRequest(irf);
        }
        else {
            handleRegularRequest(msg);
        }
    }

    private void handleRegularRequest(Object msg) {
        Consumer<Object> forwarder = this.currentForwarder;
        if (forwarder == null) {
            LOGGER.atWarn()
                    .log("Route filter completion handler received message with no forwarder set");
            return;
        }
        this.currentForwarder = null;
        forwarder.accept(msg);
    }

    private void handleInternalRequest(InternalRequestFrame<?> irf) {
        Consumer<Object> forwarder = this.currentForwarder;
        if (forwarder == null) {
            LOGGER.atWarn()
                    .log("Route filter completion handler received internal request with no forwarder set");
            return;
        }

        ApiKeys apiKey = ApiKeys.forId(irf.header().requestApiKey());
        int routingCorrelationId = routingCorrelationIdAllocator.getAsInt();
        var wrappedFrame = new DecodedRequestFrame<>(
                irf.apiVersion(),
                routingCorrelationId,
                irf.hasResponse(),
                irf.header(),
                irf.body());

        CompletableFuture<Response> dummyFuture = new CompletableFuture<>();
        var pendingResponse = new RouterDispatchHandler.PendingResponse(
                dummyFuture,
                Timer.start(),
                routeName,
                apiKey,
                nodeIdMapping,
                metadataAddressCacher,
                irf);
        RouterDispatchHandler.registerPendingResponse(
                clientChannel, routingCorrelationId, pendingResponse);
        pendingResponseCount.incrementAndGet();

        forwarder.accept(wrappedFrame);
    }
}
