/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.netty.channel.embedded.EmbeddedChannel;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.internal.ClientConnectionStateMachine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RoutingTerminalHandlerTest {

    private static final int CORRELATION_ID = 42;
    private static final String ROUTE_A = "route-a";

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
        frame.setRouteName(ROUTE_A);

        // When
        channel.writeInbound(frame);

        // Then
        verify(ccsm).forwardToRoute(ROUTE_A, frame);
    }

    @Test
    void shouldForwardToNodeForRouteTargetNode() {
        // Given
        var handler = new RoutingTerminalHandler(ccsm);
        channel = new EmbeddedChannel(handler);
        var frame = fetchRequest();
        frame.setRouteName(ROUTE_A);
        frame.setTargetVirtualNodeId(7);

        // When
        channel.writeInbound(frame);

        // Then
        verify(ccsm).forwardToNode(7, ROUTE_A, frame);
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
    void writeShouldSetRouteNameForKnownCorrelationId() {
        // Given
        when(ccsm.sessionId()).thenReturn("test-session");
        var handler = new RoutingTerminalHandler(ccsm);
        channel = new EmbeddedChannel(handler);

        var request = fetchRequest();
        request.setRouteName(ROUTE_A);
        channel.writeInbound(request);

        var response = new DecodedResponseFrame<>((short) 12, CORRELATION_ID,
                new ResponseHeaderData(), new FetchResponseData());

        // When
        channel.writeOutbound(response);

        // Then
        DecodedResponseFrame<?> out = channel.readOutbound();
        assertThat(out).isNotNull();
        assertThat(out.routeName()).isEqualTo(ROUTE_A);
    }

    @Test
    void writeShouldNotSetRouteNameForUnknownCorrelationId() {
        // Given
        var handler = new RoutingTerminalHandler(ccsm);
        channel = new EmbeddedChannel(handler);

        var response = new DecodedResponseFrame<>((short) 12, 9999,
                new ResponseHeaderData(), new FetchResponseData());

        // When
        channel.writeOutbound(response);

        // Then
        DecodedResponseFrame<?> out = channel.readOutbound();
        assertThat(out).isNotNull();
        assertThat(out.routeName()).isNull();
    }

    @Test
    void writeShouldNotSetRouteNameForFireAndForgetRequest() {
        // Given
        when(ccsm.sessionId()).thenReturn("test-session");
        var handler = new RoutingTerminalHandler(ccsm);
        channel = new EmbeddedChannel(handler);

        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.PRODUCE.id)
                .setRequestApiVersion((short) 9)
                .setCorrelationId(CORRELATION_ID);
        var frame = new DecodedRequestFrame<>((short) 9, CORRELATION_ID, false, header, new ProduceRequestData().setAcks((short) 0));
        frame.setRouteName(ROUTE_A);
        channel.writeInbound(frame);

        var response = new DecodedResponseFrame<>((short) 9, CORRELATION_ID,
                new ResponseHeaderData(), new FetchResponseData());

        // When
        channel.writeOutbound(response);

        // Then
        DecodedResponseFrame<?> out = channel.readOutbound();
        assertThat(out).isNotNull();
        assertThat(out.routeName()).isNull();
    }

    private DecodedRequestFrame<FetchRequestData> fetchRequest() {
        var header = new RequestHeaderData()
                .setRequestApiKey(ApiKeys.FETCH.id)
                .setRequestApiVersion((short) 12)
                .setCorrelationId(CORRELATION_ID);
        return new DecodedRequestFrame<>((short) 12, CORRELATION_ID, true, header, new FetchRequestData());
    }
}
