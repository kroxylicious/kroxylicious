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
import io.kroxylicious.proxy.frame.PathElement;
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

    private static final PathElement.Route ROUTE_A = new PathElement.Route("route-a", PathElement.ClientOrigin.INSTANCE);
    private static final PathElement.Route ROUTE_B = new PathElement.Route("route-b", PathElement.ClientOrigin.INSTANCE);

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
        frame.setPath(ROUTE_A);

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
        frame.setPath(ROUTE_B);

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
        frame.setPath(ROUTE_A);

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
        frame.setPath(ROUTE_B);

        // When
        channel.writeOutbound(frame);

        // Then
        assertThat((Object) channel.readOutbound()).isSameAs(frame);
    }

    @Test
    void internalRequestFrameOnMatchingRouteIsProcessed() {
        // Given
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> context.forwardRequest(header, request);
        buildChannel(filter, ROUTE_A);
        var header = requestHeader(new ApiVersionsRequestData());
        var internalFrame = new InternalRequestFrame<>(
                header.requestApiVersion(), header.correlationId(), false, header, new ApiVersionsRequestData());
        // Some other filter on route-a sent this - route membership alone should be enough
        // for this handler to process it (see C1/C6 in RouteFilterCorrectnessIT).
        internalFrame.setPath(new PathElement.FilterOrigin("other-filter", 0, new CompletableFuture<>(), ROUTE_A));

        // When
        channel.writeInbound(internalFrame);

        // Then
        assertThat((Object) channel.readInbound()).isSameAs(internalFrame);
    }

    @Test
    void internalRequestFrameOnNonMatchingRoutePassesThrough() {
        // Given
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> {
            throw new AssertionError("Filter should not be invoked for non-matching route");
        };
        buildChannel(filter, ROUTE_A);
        var header = requestHeader(new ApiVersionsRequestData());
        var internalFrame = new InternalRequestFrame<>(
                header.requestApiVersion(), header.correlationId(), false, header, new ApiVersionsRequestData());
        internalFrame.setPath(new PathElement.FilterOrigin("other-filter", 0, new CompletableFuture<>(), ROUTE_B));

        // When
        channel.writeInbound(internalFrame);

        // Then
        assertThat((Object) channel.readInbound()).isSameAs(internalFrame);
    }

    @Test
    void sendRequestTagsFrameWithOwnRouteAndFilterIdentity() {
        // Given
        ApiVersionsRequestFilter filter = (apiVersion, header, request, context) -> {
            context.sendRequest(header, request);
            return context.forwardRequest(header, request);
        };
        buildChannel(filter, ROUTE_A);
        var frame = decodedRequest(new ApiVersionsRequestData());
        frame.setPath(ROUTE_A);

        // When
        channel.writeInbound(frame);

        // Then
        InternalRequestFrame<?> internalFrame = channel.readInbound();
        assertThat(internalFrame).isNotNull();
        assertThat(internalFrame.path()).isInstanceOfSatisfying(PathElement.FilterOrigin.class, f -> {
            assertThat(f.name()).isEqualTo(filter.getClass().getSimpleName());
            assertThat(f.ordinal()).isZero();
            assertThat(f.parent()).isEqualTo(ROUTE_A);
        });
    }

    @Test
    void internalResponseFrameForOwnFilterIsCompleted() {
        // Given
        ApiVersionsResponseFilter filter = (apiVersion, header, response, context) -> context.forwardResponse(header, response);
        buildChannel(filter, ROUTE_A);
        var header = new ResponseHeaderData().setCorrelationId(42);
        var future = new CompletableFuture<>();
        var internalFrame = new InternalResponseFrame<>(ApiKeys.API_VERSIONS.latestVersion(), 42, header, new ApiVersionsResponseData());
        internalFrame.setPath(new PathElement.FilterOrigin(filter.getClass().getSimpleName(), 0, future, ROUTE_A));

        // When
        channel.writeOutbound(internalFrame);

        // Then
        assertThat(future).isCompleted();
    }

    @Test
    void internalResponseFrameForAnotherFilterOnSameRouteIsObservedNotCompleted() {
        // Given
        var observed = new AtomicBoolean(false);
        ApiVersionsResponseFilter filter = (apiVersion, header, response, context) -> {
            observed.set(true);
            return context.forwardResponse(header, response);
        };
        buildChannel(filter, ROUTE_A);
        var header = new ResponseHeaderData().setCorrelationId(42);
        var future = new CompletableFuture<>();
        var internalFrame = new InternalResponseFrame<>(ApiKeys.API_VERSIONS.latestVersion(), 42, header, new ApiVersionsResponseData());
        // Addressed to a different filter, but genuinely on this handler's own route: per C6 in
        // RouteFilterCorrectnessIT, this handler's filter must still see it via onResponse, but
        // must not complete a promise that isn't its own.
        internalFrame.setPath(new PathElement.FilterOrigin("other-filter", 0, future, ROUTE_A));

        // When
        channel.writeOutbound(internalFrame);

        // Then
        assertThat(observed).as("sibling filter on the same route must observe the response").isTrue();
        assertThat(future).as("but must not complete a promise that isn't its own").isNotCompleted();
    }

    @Test
    void internalResponseFrameForAnotherFilterOnAnotherRoutePassesThrough() {
        // Given
        ApiVersionsResponseFilter filter = (apiVersion, header, response, context) -> {
            throw new AssertionError("Filter should not process an OOB response addressed to a filter on another route");
        };
        buildChannel(filter, ROUTE_A);
        var header = new ResponseHeaderData().setCorrelationId(42);
        var future = new CompletableFuture<>();
        var internalFrame = new InternalResponseFrame<>(ApiKeys.API_VERSIONS.latestVersion(), 42, header, new ApiVersionsResponseData());
        internalFrame.setPath(new PathElement.FilterOrigin("other-filter", 0, future, ROUTE_B));

        // When
        channel.writeOutbound(internalFrame);

        // Then
        assertThat(future).isNotCompleted();
        assertThat((Object) channel.readOutbound()).isSameAs(internalFrame);
    }

    /**
     * A route's own filters may be installed on it more than once (e.g. the same filter type
     * configured twice), disambiguated only by their position - see the {@code Risk} discussion
     * in the qualified-path redesign this class exercises.
     */
    @Test
    void internalResponseFrameForSameNameDifferentOrdinalIsObservedNotCompleted() {
        // Given
        var observed = new AtomicBoolean(false);
        ApiVersionsResponseFilter filter = (apiVersion, header, response, context) -> {
            observed.set(true);
            return context.forwardResponse(header, response);
        };
        buildChannel(filter, ROUTE_A, 1);
        var header = new ResponseHeaderData().setCorrelationId(42);
        var future = new CompletableFuture<>();
        var internalFrame = new InternalResponseFrame<>(ApiKeys.API_VERSIONS.latestVersion(), 42, header, new ApiVersionsResponseData());
        // Same filter name, same route, but ordinal 0 - a different position than this handler's (1).
        internalFrame.setPath(new PathElement.FilterOrigin(filter.getClass().getSimpleName(), 0, future, ROUTE_A));

        // When
        channel.writeOutbound(internalFrame);

        // Then
        assertThat(observed).as("still observed: it's genuinely on this handler's route").isTrue();
        assertThat(future).as("but not completed: it's addressed to a different position on the route").isNotCompleted();
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
        opaqueFrame.setPath(ROUTE_A);

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
        opaqueFrame.setPath(ROUTE_B);

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
        assertThat(handler.filterDescriptor()).contains("[route=" + ROUTE_A.describe() + "]");
    }

    private void buildChannel(Filter filter, PathElement.Route routePath) {
        buildChannel(filter, routePath, 0);
    }

    private void buildChannel(Filter filter, PathElement.Route routePath, int ordinal) {
        var ccsm = newClientConnectionStateMachine();
        FilterAndInvoker filterAndInvoker = getOnlyElement(FilterAndInvoker.build(filter.getClass().getSimpleName(), filter));
        ChannelHandler routeFilterHandler = new RouteFilterHandler(filterAndInvoker, 1000L, null, new EmbeddedChannel(), ccsm, routePath, ordinal);

        channel = new EmbeddedChannel();
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
