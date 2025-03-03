/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collector;
import java.util.stream.Stream;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.junit.jupiter.api.AfterEach;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;

import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.OpaqueRequestFrame;
import io.kroxylicious.proxy.frame.OpaqueResponseFrame;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.HostPort;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A test harness for {@link Filter} implementations.
 */
public abstract class FilterHarness {
    public static final String TEST_CLIENT = "test-client";
    public static final List<HostPort> TARGET_CLUSTER_BOOTSTRAP = List.of(HostPort.parse("targetCluster:9091"));
    protected EmbeddedChannel channel;
    private final AtomicInteger outboundCorrelationId = new AtomicInteger(1);
    private final Map<Integer, Correlation> pendingInternalRequestMap = new HashMap<>();
    private long timeoutMs = 1000L;

    /**
     * Sets the timeout for applied to the filters.
     *
     * @param timeoutMs timeout in millis
     * @return this
     */
    protected FilterHarness timeout(long timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }

    /**
     * Build a {@link #channel} containing a {@link FilterHandler} for each the given {@link Filter}.
     *
     * @param filters - the filters to associate with the channel.
     */
    protected void buildChannel(Filter... filters) {
        assertNull(channel, "Channel already built");

        final TargetCluster targetCluster = mock(TargetCluster.class);
        when(targetCluster.bootstrapServersList()).thenReturn(TARGET_CLUSTER_BOOTSTRAP);
        var testVirtualCluster = new VirtualClusterModel("TestVirtualCluster", targetCluster, false,
                false, List.of());
        testVirtualCluster.addGateway("default", mock(ClusterNetworkAddressConfigProvider.class), Optional.empty());
        var inboundChannel = new EmbeddedChannel();
        var channelProcessors = Stream.<ChannelHandler> of(new InternalRequestTracker(), new CorrelationIdIssuer());

        var filterHandlers = Arrays.stream(filters)
                .collect(Collector.of(ArrayDeque<Filter>::new, ArrayDeque::addFirst, (d1, d2) -> {
                    d2.addAll(d1);
                    return d2;
                })) // reverses order
                .stream()
                .map(f -> new FilterHandler(getOnlyElement(FilterAndInvoker.build(f)), timeoutMs, null, testVirtualCluster, inboundChannel))
                .map(ChannelHandler.class::cast);
        var handlers = Stream.concat(channelProcessors, filterHandlers);

        channel = new EmbeddedChannel(handlers.toArray(ChannelHandler[]::new));
    }

    /**
     * Write a client request to the pipeline.
     * @param data The request body.
     * @return The frame that was sent.
     * @param <B> The type of the request.
     */
    protected <B extends ApiMessage> DecodedRequestFrame<B> writeRequest(B data) {
        var apiKey = ApiKeys.forId(data.apiKey());
        var header = new RequestHeaderData();
        int correlationId = 42;
        header.setCorrelationId(correlationId);
        header.setRequestApiKey(apiKey.id);
        header.setRequestApiVersion(apiKey.latestVersion());
        header.setClientId(TEST_CLIENT);
        return writeRequest(header, data);
    }

    /**
     * Write a client request to the pipeline.
     * @param headerData The request header.
     * @param data The request body.
     * @return The frame that was sent.
     * @param <B> The type of the request.
     */
    protected <B extends ApiMessage> DecodedRequestFrame<B> writeRequest(RequestHeaderData headerData, B data) {
        var frame = new DecodedRequestFrame<>(headerData.requestApiVersion(), headerData.correlationId(), false, headerData, data);
        return writeRequest(frame);
    }

    /**
     * Write a client request frame to the pipeline.
     * @param frame The request header.
     * @return The frame that was sent.
     * @param <B> The type of the request.
     */
    protected <B extends ApiMessage> DecodedRequestFrame<B> writeRequest(DecodedRequestFrame<B> frame) {
        channel.writeOutbound(frame);
        return frame;
    }

    /**
     * Writes an opaque request, simulating the case where no Filter in the chain
     * is interested in this RPC, so it is not decoded
     * @return the opaque frame written to the channel
     */
    protected OpaqueRequestFrame writeArbitraryOpaqueRequest() {
        OpaqueRequestFrame frame = new OpaqueRequestFrame(Unpooled.buffer(), 55, false, 0, false);
        channel.writeOneOutbound(frame);
        return frame;
    }

    /**
     * Writes an opaque response, simulating the case where no Filter in the chain
     * is interested in this RPC, so it is not decoded
     * @return the opaque frame written to the channel
     */
    protected OpaqueResponseFrame writeArbitraryOpaqueResponse() {
        OpaqueResponseFrame frame = new OpaqueResponseFrame(Unpooled.buffer(), 55, 0);
        channel.writeOneInbound(frame);
        return frame;
    }

    /**
     * Write a normal client response, as if from the broker.
     * @param data The body of the response.
     * @return The frame that was written.
     * @param <B> The type of the response body.
     */
    protected <B extends ApiMessage> DecodedResponseFrame<B> writeResponse(B data) {
        var apiKey = ApiKeys.forId(data.apiKey());
        var header = new ResponseHeaderData();
        int correlationId = 42;
        header.setCorrelationId(correlationId);
        var frame = new DecodedResponseFrame<>(apiKey.latestVersion(), correlationId, header, data);
        channel.writeInbound(frame);
        return frame;
    }

    /**
     * Write a response for a filter-originated out-of-band request, as if from the broker. The caller must provide
     * a valid {@code requestCorrelationId} that corresponds to an internal request that is already pending.  This
     * is used to determine the recipient filter and the promise.
     *
     * @param <B>                  The type of the response body.
     * @param requestCorrelationId the correlation id of the internal request for which this response is being generated for.
     * @param data                 The body of the response.
     * @return The frame that was written.
     */
    protected <B extends ApiMessage> DecodedResponseFrame<B> writeInternalResponse(int requestCorrelationId, B data) {
        var apiKey = ApiKeys.forId(data.apiKey());
        var header = new ResponseHeaderData();
        header.setCorrelationId(requestCorrelationId);
        var correlation = pendingInternalRequestMap.remove(requestCorrelationId);
        if (correlation == null) {
            throw new IllegalStateException("No corresponding internal request known " + requestCorrelationId);
        }
        var frame = new InternalResponseFrame<>(correlation.recipient(), apiKey.latestVersion(), requestCorrelationId, header, data, correlation.promise());
        channel.writeInbound(frame);
        return frame;

    }

    /**
     * Shutdown the channel, asserting there were no further requests or responses to read.
     */
    @AfterEach
    public void assertFinish() {
        if (channel == null) {
            // enable mixing FilterHarness tests with other unit tests that don't create the channel
            return;
        }
        boolean finish = channel.finish();
        if (finish) {
            Object inbound = channel.readInbound();
            Object outbound = channel.readOutbound();
            if (inbound != null && outbound != null) {
                fail("Unexpected inbound and outbound messages: inbound: " + inbound + ", outbound: " + outbound);
            }
            else if (inbound != null) {
                fail("Unexpected inbound message: inbound: " + inbound);
            }
            else if (outbound != null) {
                fail("Unexpected outbound message: outbound: " + outbound);
            }
            else {
                fail("Logically this is impossible");
            }
        }
    }

    public record Correlation(Filter recipient, CompletableFuture<?> promise) {}

    /**
     * Tracks outstanding internal requests by associating the correlation id with the recipient/promise tuple.
     */
    private class InternalRequestTracker extends ChannelOutboundHandlerAdapter {
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            if (msg instanceof InternalRequestFrame<?> irf && irf.hasResponse()) {
                if (pendingInternalRequestMap.put(irf.header().correlationId(), new Correlation(irf.recipient(), irf.promise())) != null) {
                    throw new IllegalStateException("correlationId %d already has a promise associated with it".formatted(irf.correlationId()));
                }
            }
            super.write(ctx, msg, promise);
        }
    }

    /**
     * Issues a unique correlation id to every request.
     */
    private class CorrelationIdIssuer extends ChannelOutboundHandlerAdapter {
        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            if (msg instanceof DecodedRequestFrame<?> drf) {
                drf.header().setCorrelationId(outboundCorrelationId.getAndIncrement());
            }
            super.write(ctx, msg, promise);
        }
    }
}
