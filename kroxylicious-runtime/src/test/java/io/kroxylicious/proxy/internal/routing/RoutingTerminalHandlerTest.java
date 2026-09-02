/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.netty.channel.embedded.EmbeddedChannel;

import io.kroxylicious.kafka.common.message.FetchRequestData;
import io.kroxylicious.kafka.common.message.FetchResponseData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.ResponseHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.PathElement;
import io.kroxylicious.proxy.internal.ClientConnectionStateMachine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class RoutingTerminalHandlerTest {

    private static final int CORRELATION_ID = 42;
    private static final PathElement.Route ROUTE_A = new PathElement.Route("route-a", PathElement.ClientOrigin.INSTANCE);

    @Mock
    private ClientConnectionStateMachine ccsm;

    private EmbeddedChannel channel;

    @AfterEach
    void tearDown() {
        if (channel != null) {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void shouldForwardToRouteForRouteDefaultNode() {
        // Given
        var handler = new RoutingTerminalHandler(ccsm);
        channel = new EmbeddedChannel(handler);
        var frame = fetchRequest();
        frame.setPath(ROUTE_A);

        // When
        channel.writeInbound(frame);

        // Then
        verify(ccsm).forwardToRoute("route-a", frame);
    }

    @Test
    void shouldForwardToNodeForRouteTargetNode() {
        // Given
        var handler = new RoutingTerminalHandler(ccsm);
        channel = new EmbeddedChannel(handler);
        var frame = fetchRequest();
        frame.setPath(ROUTE_A);
        frame.setTargetVirtualNodeId(7);

        // When
        channel.writeInbound(frame);

        // Then
        verify(ccsm).forwardToNode(7, "route-a", frame);
    }

    @Test
    void shouldUnwrapFilterLeafToForwardOnItsOwnRoute() {
        // Given: an out-of-band request issued by a route filter - its path's leaf identifies the
        // filter, not a bare route, but it must still be forwarded on the route beneath that leaf.
        var handler = new RoutingTerminalHandler(ccsm);
        channel = new EmbeddedChannel(handler);
        var frame = fetchRequest();
        frame.setPath(new PathElement.FilterOrigin("marker-filter", 0, new CompletableFuture<>(), ROUTE_A));

        // When
        channel.writeInbound(frame);

        // Then
        verify(ccsm).forwardToRoute("route-a", frame);
    }

    @Test
    void shouldUnwrapRouterLeafToForwardOnItsOwnRoute() {
        // Given: a router-issued out-of-band request (RouterContext.sendRequest) - same shape.
        var handler = new RoutingTerminalHandler(ccsm);
        channel = new EmbeddedChannel(handler);
        var frame = fetchRequest();
        frame.setPath(new PathElement.RouterOrigin(new CompletableFuture<>(), ROUTE_A));

        // When
        channel.writeInbound(frame);

        // Then
        verify(ccsm).forwardToRoute("route-a", frame);
    }

    @Test
    void shouldFallBackToFilterChainCompleteWhenNoRouteName() {
        // Given
        var handler = new RoutingTerminalHandler(ccsm);
        channel = new EmbeddedChannel(handler);
        var frame = fetchRequest();

        // When
        channel.writeInbound(frame);

        // Then
        verify(ccsm).onClientFilterChainComplete(frame);
    }

    @Test
    void shouldFallBackToFilterChainCompleteForNonFrameMessage() {
        // Given
        var handler = new RoutingTerminalHandler(ccsm);
        channel = new EmbeddedChannel(handler);

        // When
        channel.writeInbound("not-a-frame");

        // Then
        verify(ccsm).onClientFilterChainComplete("not-a-frame");
    }

    @Test
    void writeIsAPureOutboundPassThrough() {
        // Given: the outbound path is now a pure pass-through - the correct path was already
        // restored directly from CorrelationManager by the time a response reaches this handler,
        // so there is no bookkeeping left to do here.
        var handler = new RoutingTerminalHandler(ccsm);
        channel = new EmbeddedChannel(handler);
        var response = new DecodedResponseFrame<>((short) 12, CORRELATION_ID,
                new ResponseHeaderData(), new FetchResponseData());
        response.setPath(ROUTE_A);

        // When
        channel.writeOutbound(response);

        // Then
        DecodedResponseFrame<?> out = channel.readOutbound();
        assertThat(out).isSameAs(response);
        assertThat(out.path()).isSameAs(ROUTE_A);
    }

    private DecodedRequestFrame<FetchRequestData> fetchRequest() {
        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.FETCH.id)
                .setRequestApiVersion((short) 12)
                .setCorrelationId(CORRELATION_ID);
        return new DecodedRequestFrame<>((short) 12, CORRELATION_ID, true, header, new FetchRequestData());
    }
}
