/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.embedded.EmbeddedChannel;

import io.kroxylicious.kafka.common.message.ApiVersionsRequestData;
import io.kroxylicious.kafka.common.message.ApiVersionsResponseData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.ResponseHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.proxy.config.CacheConfiguration;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.filter.ApiVersionsRequestFilter;
import io.kroxylicious.proxy.filter.ApiVersionsResponseFilter;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.OpaqueRequestFrame;
import io.kroxylicious.proxy.frame.OpaqueResponseFrame;
import io.kroxylicious.proxy.internal.filter.FilterAndInvoker;
import io.kroxylicious.proxy.internal.net.EndpointBinding;
import io.kroxylicious.proxy.internal.net.EndpointGateway;
import io.kroxylicious.proxy.internal.routing.DirectRouting;
import io.kroxylicious.proxy.internal.subject.DefaultSubjectBuilder;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.service.NodeIdentificationStrategy;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RouteFilterHandlerTest {

    private static final String ROUTE_A = "route-a";
    private static final String ROUTE_B = "route-b";

    private EmbeddedChannel channel;

    @AfterEach
    void tearDown() {
        if (channel != null) {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void matchingRouteDecodedRequestIsFiltered() {
        // Given
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> context.forwardRequest(header, request);
        buildChannel(filter, ROUTE_A);
        var frame = decodedRequest(new ApiVersionsRequestData());
        frame.setRouteName(ROUTE_A);

        // When
        channel.writeInbound(frame);

        // Then
        assertThat((Object) channel.readInbound()).isSameAs(frame);
    }

    @Test
    void nonMatchingRouteDecodedRequestPassesThrough() {
        // Given
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> {
            throw new AssertionError("Filter should not be invoked for non-matching route");
        };
        buildChannel(filter, ROUTE_A);
        var frame = decodedRequest(new ApiVersionsRequestData());
        frame.setRouteName(ROUTE_B);

        // When
        channel.writeInbound(frame);

        // Then
        assertThat((Object) channel.readInbound()).isSameAs(frame);
    }

    @Test
    void noRouteNameDecodedRequestPassesThrough() {
        // Given
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> {
            throw new AssertionError("Filter should not be invoked when no routing context");
        };
        buildChannel(filter, ROUTE_A);
        var frame = decodedRequest(new ApiVersionsRequestData());

        // When
        channel.writeInbound(frame);

        // Then
        assertThat((Object) channel.readInbound()).isSameAs(frame);
    }

    @Test
    void matchingRouteDecodedResponseIsFiltered() {
        // Given
        ApiVersionsResponseFilter filter = (apiVersion, header, response, context) -> context.forwardResponse(header, response);
        buildChannel(filter, ROUTE_A);
        var frame = decodedResponse(new ApiVersionsResponseData());
        frame.setRouteName(ROUTE_A);

        // When
        channel.writeOutbound(frame);

        // Then
        assertThat((Object) channel.readOutbound()).isSameAs(frame);
    }

    @Test
    void nonMatchingRouteDecodedResponsePassesThrough() {
        // Given
        ApiVersionsResponseFilter filter = (apiVersion, header, response, context) -> {
            throw new AssertionError("Filter should not be invoked for non-matching route");
        };
        buildChannel(filter, ROUTE_A);
        var frame = decodedResponse(new ApiVersionsResponseData());
        frame.setRouteName(ROUTE_B);

        // When
        channel.writeOutbound(frame);

        // Then
        assertThat((Object) channel.readOutbound()).isSameAs(frame);
    }

    @Test
    void internalRequestFrameWithMatchingRouteIsProcessed() {
        // Given
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> context.forwardRequest(header, request);
        buildChannel(filter, ROUTE_A);
        var header = requestHeader(new ApiVersionsRequestData());
        var internalFrame = new InternalRequestFrame<>(
                header.requestApiVersion(), header.correlationId(), false,
                filter, new CompletableFuture<>(), header, new ApiVersionsRequestData());
        internalFrame.setRouteName(ROUTE_A);

        // When
        channel.writeInbound(internalFrame);

        // Then
        assertThat((Object) channel.readInbound()).isSameAs(internalFrame);
    }

    @Test
    void internalRequestFrameWithNonMatchingRoutePassesThrough() {
        // Given
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> {
            throw new AssertionError("Filter should not be invoked for non-matching route");
        };
        buildChannel(filter, ROUTE_A);
        var header = requestHeader(new ApiVersionsRequestData());
        var internalFrame = new InternalRequestFrame<>(
                header.requestApiVersion(), header.correlationId(), false,
                filter, new CompletableFuture<>(), header, new ApiVersionsRequestData());
        internalFrame.setRouteName(ROUTE_B);

        // When
        channel.writeInbound(internalFrame);

        // Then
        assertThat((Object) channel.readInbound()).isSameAs(internalFrame);
    }

    @Test
    void onInternalRequestSetsRouteNameOnFrame() {
        // Given
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> {
            context.sendRequest(header, request);
            return context.forwardRequest(header, request);
        };
        buildChannel(filter, ROUTE_A);
        var frame = decodedRequest(new ApiVersionsRequestData());
        frame.setRouteName(ROUTE_A);

        // When
        channel.writeInbound(frame);

        // Then
        InternalRequestFrame<?> internalFrame = channel.readInbound();
        assertThat(internalFrame).isNotNull();
        assertThat(internalFrame.routeName()).isEqualTo(ROUTE_A);
    }

    @Test
    void internalResponseFrameWithMatchingRouteIsProcessed() {
        // Given
        ApiVersionsResponseFilter filter = (apiVersion, header, response, context) -> context.forwardResponse(header, response);
        buildChannel(filter, ROUTE_A);
        var header = new ResponseHeaderData().setCorrelationId(42);
        var future = new CompletableFuture<>();
        var internalFrame = new InternalResponseFrame<>(
                filter, ApiKeys.API_VERSIONS.latestVersion(), 42, header, new ApiVersionsResponseData(), future);
        internalFrame.setRouteName(ROUTE_A);

        // When
        channel.writeOutbound(internalFrame);

        // Then
        assertThat(future).isCompleted();
    }

    @Test
    void internalResponseFrameForRecipientIsCompletedEvenWhenRouteDoesNotMatch() {
        // Given
        ApiVersionsResponseFilter filter = (apiVersion, header, response, context) -> context.forwardResponse(header, response);
        buildChannel(filter, ROUTE_A);
        var header = new ResponseHeaderData().setCorrelationId(42);
        var future = new CompletableFuture<>();
        var internalFrame = new InternalResponseFrame<>(
                filter, ApiKeys.API_VERSIONS.latestVersion(), 42, header, new ApiVersionsResponseData(), future);
        internalFrame.setRouteName(ROUTE_B);

        // When
        channel.writeOutbound(internalFrame);

        // Then
        assertThat(future).isCompleted();
        assertThat((Object) channel.readOutbound()).isNull();
    }

    @Test
    void internalResponseFrameForAnotherFilterOnAnotherRoutePassesThrough() {
        // Given
        ApiVersionsResponseFilter filter = (apiVersion, header, response, context) -> {
            throw new AssertionError("Filter should not process an OOB response addressed to another filter on another route");
        };
        buildChannel(filter, ROUTE_A);
        Filter otherFilter = mock(Filter.class);
        var header = new ResponseHeaderData().setCorrelationId(42);
        var future = new CompletableFuture<>();
        var internalFrame = new InternalResponseFrame<>(
                otherFilter, ApiKeys.API_VERSIONS.latestVersion(), 42, header, new ApiVersionsResponseData(), future);
        internalFrame.setRouteName(ROUTE_B);

        // When
        channel.writeOutbound(internalFrame);

        // Then
        assertThat(future).isNotCompleted();
        assertThat((Object) channel.readOutbound()).isSameAs(internalFrame);
    }

    /**
     * All out-of-band requests share one reserved correlation id, so when two or more routes have
     * one in flight concurrently, {@code RoutingTerminalHandler}'s {@code correlationId -> routeName}
     * map collides and can restore the wrong route name on a returning response (see
     * {@code CorrelationIdSpace#RESERVED_OUT_OF_BAND_CORRELATION_ID}). This does not stop the response
     * reaching the filter that actually sent the request - that match is by recipient identity, not
     * route name (see {@link #internalResponseFrameForRecipientIsCompletedEvenWhenRouteDoesNotMatch}) -
     * but a sibling filter on the SAME (genuinely correct) route only ever gates on
     * {@code matchesRoute()}, so it silently misses the observation it is supposed to get, per C6 in
     * {@code RouteFilterCorrectnessIT}.
     */
    @Test
    void siblingFilterOnSameRouteObservesOobResponseEvenWhenRouteNameIsCorruptedByConcurrentOob() {
        // Given
        var observedBySibling = new AtomicBoolean(false);
        ApiVersionsResponseFilter filter = (apiVersion, header, response, context) -> context.forwardResponse(header, response);
        ApiVersionsResponseFilter siblingFilter = (apiVersion, header, response, context) -> {
            observedBySibling.set(true);
            return context.forwardResponse(header, response);
        };
        buildChannel(filter, siblingFilter, ROUTE_A);
        var header = new ResponseHeaderData().setCorrelationId(42);
        var future = new CompletableFuture<>();
        var internalFrame = new InternalResponseFrame<>(
                filter, ApiKeys.API_VERSIONS.latestVersion(), 42, header, new ApiVersionsResponseData(), future);
        // Simulates the route name RoutingTerminalHandler restores after its correlationId -> routeName
        // map collided with a second, concurrent out-of-band request from route-b.
        internalFrame.setRouteName(ROUTE_B);

        // When
        channel.writeOutbound(internalFrame);

        // Then
        assertThat(future)
                .as("the recipient's own promise still completes: delivery is matched by filter identity, not route name")
                .isCompleted();
        assertThat(observedBySibling)
                .as("route-a's own sibling filter must observe route-a's own out-of-band response via onResponse (C6), "
                        + "but the corrupted route name means matchesRoute() never fires for it")
                .isTrue();
    }

    @Test
    void opaqueRequestWithMatchingRoutePassesThrough() {
        // Given
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> {
            throw new AssertionError("Filter should not be invoked for opaque frames");
        };
        buildChannel(filter, ROUTE_A);
        ByteBuf buffer = Unpooled.buffer();
        var opaqueFrame = new OpaqueRequestFrame(buffer, ApiKeys.PRODUCE.id, ApiKeys.PRODUCE.latestVersion(), 55, false, buffer.readableBytes(), false);
        opaqueFrame.setRouteName(ROUTE_A);

        // When
        assertThat(channel.writeOneInbound(opaqueFrame).cause()).isNull();

        // Then
        assertThat((Object) channel.readInbound()).isSameAs(opaqueFrame);
    }

    @Test
    void opaqueResponseWithNonMatchingRoutePassesThrough() {
        // Given
        buildChannel((ApiVersionsResponseFilter) (apiVersion, header, response, context) -> {
            throw new AssertionError("Filter should not be invoked for opaque frames");
        }, ROUTE_A);
        ByteBuf buffer = Unpooled.buffer(4);
        buffer.writeInt(55);
        var opaqueFrame = new OpaqueResponseFrame(ApiKeys.PRODUCE.id, ApiKeys.PRODUCE.latestVersion(), buffer, 55, buffer.readableBytes());
        opaqueFrame.setRouteName(ROUTE_B);

        // When
        channel.writeOutbound(opaqueFrame);

        // Then
        assertThat((Object) channel.readOutbound()).isSameAs(opaqueFrame);
    }

    @Test
    void filterDescriptorIncludesRouteName() {
        // Given
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> context.forwardRequest(header, request);
        buildChannel(filter, ROUTE_A);

        // When
        RouteFilterHandler handler = (RouteFilterHandler) channel.pipeline().get("routeFilter");

        // Then
        assertThat(handler.filterDescriptor()).contains("[route=" + ROUTE_A + "]");
    }

    private void buildChannel(Filter filter, String routeName) {
        var ccsm = newClientConnectionStateMachine();
        FilterAndInvoker filterAndInvoker = getOnlyElement(FilterAndInvoker.build(filter.getClass().getSimpleName(), filter));
        ChannelHandler routeFilterHandler = new RouteFilterHandler(filterAndInvoker, 1000L, null, new EmbeddedChannel(), ccsm, routeName);

        channel = new EmbeddedChannel();
        channel.pipeline().addLast("routeFilter", routeFilterHandler);
    }

    /**
     * Builds a channel with two {@link RouteFilterHandler}s configured for the SAME route, one per
     * filter, sharing a single connection state machine - modelling two filters on one route, as
     * installed by {@code KafkaProxyFrontendHandler} for a route with more than one filter.
     */
    private void buildChannel(Filter filter, Filter siblingFilter, String routeName) {
        var ccsm = newClientConnectionStateMachine();
        FilterAndInvoker filterAndInvoker = getOnlyElement(FilterAndInvoker.build(filter.getClass().getSimpleName(), filter));
        FilterAndInvoker siblingFilterAndInvoker = getOnlyElement(FilterAndInvoker.build(siblingFilter.getClass().getSimpleName(), siblingFilter));
        ChannelHandler routeFilterHandler = new RouteFilterHandler(filterAndInvoker, 1000L, null, new EmbeddedChannel(), ccsm, routeName);
        ChannelHandler siblingRouteFilterHandler = new RouteFilterHandler(siblingFilterAndInvoker, 1000L, null, new EmbeddedChannel(), ccsm, routeName);

        channel = new EmbeddedChannel();
        channel.pipeline().addLast("siblingRouteFilter", siblingRouteFilterHandler);
        channel.pipeline().addLast("routeFilter", routeFilterHandler);
    }

    private ClientConnectionStateMachine newClientConnectionStateMachine() {
        final TargetCluster targetCluster = mock(TargetCluster.class);
        when(targetCluster.bootstrapServersList()).thenReturn(List.of(HostPort.parse("targetCluster:9091")));
        var testVirtualCluster = new VirtualClusterModel("TestVirtualCluster", new DirectRouting("upstream", targetCluster), false,
                false, List.of(), CacheConfiguration.DEFAULT, null, Duration.ofSeconds(10), null);
        testVirtualCluster.addGateway("default", mock(NodeIdentificationStrategy.class), Optional.empty());

        var endpointBinding = mock(EndpointBinding.class);
        when(endpointBinding.nodeId()).thenReturn(0);
        var gw = mock(EndpointGateway.class);
        when(gw.virtualCluster()).thenReturn(testVirtualCluster);
        when(endpointBinding.endpointGateway()).thenReturn(gw);

        var kafkaSession = new KafkaSession(KafkaSessionState.ESTABLISHING);
        var ccsm = new ClientConnectionStateMachine(endpointBinding, new DefaultSubjectBuilder(List.of()), kafkaSession);
        var forwarding = new ClientConnectionState.Forwarding();
        var mockScsm = mock(ServerConnectionStateMachine.class);
        ccsm.forceState(
                forwarding,
                mock(KafkaProxyFrontendHandler.class),
                java.util.Map.of(new HostPort("broker", 9092), mockScsm),
                kafkaSession,
                true);
        return ccsm;
    }

    private <B extends ApiMessage> DecodedRequestFrame<B> decodedRequest(B data) {
        var header = requestHeader(data);
        return new DecodedRequestFrame<>(header.requestApiVersion(), header.correlationId(), false, header, data);
    }

    private <B extends ApiMessage> RequestHeaderData requestHeader(B data) {
        var apiKey = ApiKeys.forId(data.apiKey());
        var header = new RequestHeaderData();
        header.setCorrelationId(42);
        header.setRequestApiKey(apiKey.id);
        header.setRequestApiVersion(apiKey.latestVersion());
        header.setClientId("test-client");
        return header;
    }

    private <B extends ApiMessage> DecodedResponseFrame<B> decodedResponse(B data) {
        var apiKey = ApiKeys.forId(data.apiKey());
        var header = new ResponseHeaderData();
        header.setCorrelationId(42);
        return new DecodedResponseFrame<>(apiKey.latestVersion(), 42, header, data);
    }
}
