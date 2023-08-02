/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.netty.channel.embedded.EmbeddedChannel;

import io.kroxylicious.proxy.frame.ByteBufAccessor;
import io.kroxylicious.proxy.frame.Frame;

import static org.assertj.core.api.Assertions.assertThat;

class ResponseOrdererTest {

    private EmbeddedChannel embeddedChannel;
    private ResponseOrderer orderer;

    record TestFrame(int correlationId) implements Frame {

    @Override
    public int estimateEncodedSize() {
        return 0;
    }

    @Override
    public void encode(ByteBufAccessor out) {
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public int correlationId() {
        return correlationId;
    }

    }

    @BeforeEach
    public void beforeEach() {
        embeddedChannel = new EmbeddedChannel();
        orderer = new ResponseOrderer();
        embeddedChannel.pipeline().addFirst(orderer);
    }

    @Test
    void testInboundFrameForwarded() {
        TestFrame request = new TestFrame(1);
        whenWriteInboundMessage(request);
        thenInboundContains(request);
        thenOrdererStateEquals(1, 0);
    }

    @Test
    void testInboundNonFrameForwarded() {
        Object request = new Object();
        whenWriteInboundMessage(request);
        thenInboundContains(request);
        thenOrdererStateEquals(0, 0);
    }

    @Test
    void testSingleRequestResponse() {
        TestFrame request = new TestFrame(1);
        whenWriteInboundMessage(request);
        thenInboundContains(request);
        thenOrdererStateEquals(1, 0);
        TestFrame response = new TestFrame(1);
        whenWriteOutboundMessage(response);
        thenOrdererStateEquals(0, 0);
        thenOutboundContains(response);
    }

    @Test
    void testMultipleInflightMessagesThatRespondInOrder() {
        whenWriteInboundMessage(new TestFrame(1));
        whenWriteInboundMessage(new TestFrame(2));
        thenOrdererStateEquals(2, 0);
        TestFrame response = new TestFrame(1);
        whenWriteOutboundMessage(response);
        thenOrdererStateEquals(1, 0);
        thenOutboundContains(response);
        TestFrame response2 = new TestFrame(2);
        whenWriteOutboundMessage(response2);
        thenOrdererStateEquals(0, 0);
        thenOutboundContains(response2);
    }

    @Test
    void testMultipleInflightMessagesThatRespondOutOfOrder() {
        whenWriteInboundMessage(new TestFrame(1));
        whenWriteInboundMessage(new TestFrame(2));
        thenOrdererStateEquals(2, 0);
        TestFrame response = new TestFrame(2);
        whenWriteOutboundMessage(response);
        thenOrdererStateEquals(2, 1);
        thenOutboundIsEmpty();
        TestFrame response2 = new TestFrame(1);
        whenWriteOutboundMessage(response2);
        thenOrdererStateEquals(0, 0);
        thenOutboundContains(response2);
        thenOutboundContains(response);
    }

    @Test
    void testComplexScenarioWherePendingResponsesCanBePartiallyDrained() {
        whenWriteInboundMessage(new TestFrame(1));
        whenWriteInboundMessage(new TestFrame(2));
        whenWriteInboundMessage(new TestFrame(3));
        whenWriteInboundMessage(new TestFrame(4));
        thenOrdererStateEquals(4, 0);
        TestFrame response = new TestFrame(2);
        whenWriteOutboundMessage(response);
        thenOrdererStateEquals(4, 1);
        thenOutboundIsEmpty();

        TestFrame response2 = new TestFrame(4);
        whenWriteOutboundMessage(response2);
        thenOrdererStateEquals(4, 2);
        thenOutboundIsEmpty();

        TestFrame response3 = new TestFrame(1);
        whenWriteOutboundMessage(response3);
        // the response for correlationId 2 should be drained after the response for correlationId 1 is written
        // but the response for correlationId 4 cannot be sent until we have written the response for correlationId 3
        thenOrdererStateEquals(2, 1);
        thenOutboundContains(response3);
        thenOutboundContains(response);
        thenOutboundIsEmpty();

        TestFrame response4 = new TestFrame(3);
        whenWriteOutboundMessage(response4);
        thenOrdererStateEquals(0, 0);
        thenOutboundContains(response4);
        thenOutboundContains(response2);
        thenOutboundIsEmpty();
    }

    @Test
    void testMultiplePendingAreDrained() {
        whenWriteInboundMessage(new TestFrame(1));
        whenWriteInboundMessage(new TestFrame(2));
        whenWriteInboundMessage(new TestFrame(3));
        thenOrdererStateEquals(3, 0);
        TestFrame response = new TestFrame(2);
        whenWriteOutboundMessage(response);
        thenOrdererStateEquals(3, 1);
        thenOutboundIsEmpty();

        TestFrame response2 = new TestFrame(3);
        whenWriteOutboundMessage(response2);
        thenOrdererStateEquals(3, 2);
        thenOutboundIsEmpty();

        TestFrame response3 = new TestFrame(1);
        whenWriteOutboundMessage(response3);
        thenOrdererStateEquals(0, 0);
        thenOutboundContains(response3);
        thenOutboundContains(response);
        thenOutboundContains(response2);
        thenOutboundIsEmpty();
    }

    @Test
    void testUnexpectedResponseWithNoMatchingRequest() {
        TestFrame response = new TestFrame(1);
        whenWriteOutboundMessage(response);
        thenOrdererStateEquals(0, 0);
        thenOutboundContains(response);
    }

    @Test
    void testOutboundNonFrameAreForwarded() {
        Object response = new Object();
        whenWriteOutboundMessage(response);
        thenOrdererStateEquals(0, 0);
        thenOutboundContains(response);
    }

    private void whenWriteOutboundMessage(Object object) {
        embeddedChannel.writeOneOutbound(object);
    }

    private void thenOrdererStateEquals(int expectedInFlightRequests, int expecteQueuedResponses) {
        assertThat(orderer.inFlightRequestCount()).isEqualTo(expectedInFlightRequests);
        assertThat(orderer.queuedResponseCount()).isEqualTo(expecteQueuedResponses);
    }

    private void thenInboundContains(Object object) {
        Object inbound = embeddedChannel.flushInbound().readInbound();
        assertThat(inbound).isSameAs(object);
    }

    private void thenOutboundContains(Object object) {
        Object o = embeddedChannel.flushOutbound().readOutbound();
        assertThat(o).isSameAs(object);
    }

    private void thenOutboundIsEmpty() {
        Object o = embeddedChannel.flushOutbound().readOutbound();
        assertThat(o).isNull();
    }

    private void whenWriteInboundMessage(Object object) {
        embeddedChannel.writeOneInbound(object);
    }

}