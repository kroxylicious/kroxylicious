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

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.embedded.EmbeddedChannel;

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
    void internalRequestFrameAlwaysDelegatedRegardlessOfRoute() {
        // Given
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> context.forwardRequest(header, request);
        buildChannel(filter, ROUTE_A);
        var header = requestHeader(new ApiVersionsRequestData());
        var internalFrame = new InternalRequestFrame<>(
                header.requestApiVersion(), header.correlationId(), false,
                filter, new CompletableFuture<>(), header, new ApiVersionsRequestData());

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
    void internalResponseFrameAlwaysDelegatedRegardlessOfRoute() {
        // Given
        ApiVersionsResponseFilter filter = (apiVersion, header, response, context) -> context.forwardResponse(header, response);
        buildChannel(filter, ROUTE_A);
        var header = new ResponseHeaderData().setCorrelationId(42);
        var future = new CompletableFuture<>();
        var internalFrame = new InternalResponseFrame<>(
                filter, ApiKeys.API_VERSIONS.latestVersion(), 42, header, new ApiVersionsResponseData(), future);

        // When
        channel.writeOutbound(internalFrame);

        // Then
        assertThat(future).isCompleted();
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
        channel.writeOneInbound(opaqueFrame);

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
        final TargetCluster targetCluster = mock(TargetCluster.class);
        when(targetCluster.bootstrapServersList()).thenReturn(List.of(HostPort.parse("targetCluster:9091")));
        var testVirtualCluster = new VirtualClusterModel("TestVirtualCluster", new DirectRouting("upstream", targetCluster), false,
                false, List.of(), CacheConfiguration.DEFAULT, null, Duration.ofSeconds(10), null);
        testVirtualCluster.addGateway("default", mock(NodeIdentificationStrategy.class), Optional.empty());
        var inboundChannel = new EmbeddedChannel();

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

        FilterAndInvoker filterAndInvoker = getOnlyElement(FilterAndInvoker.build(filter.getClass().getSimpleName(), filter));
        ChannelHandler routeFilterHandler = new RouteFilterHandler(filterAndInvoker, 1000L, null, inboundChannel, ccsm, routeName);

        channel = new EmbeddedChannel();
        channel.pipeline().addLast("routeFilter", routeFilterHandler);
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
