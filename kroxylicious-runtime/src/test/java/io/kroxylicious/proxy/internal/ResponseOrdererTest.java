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
import io.kroxylicious.proxy.frame.RequestFrame;

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

    record TestRequestFrame(
            int correlationId,
            boolean hasResponse
    ) implements RequestFrame {

        @Override
        public int estimateEncodedSize() {
            return 0;
        }

        @Override
        public void encode(ByteBufAccessor out) {

        }

        @Override
        public int correlationId() {
            return correlationId;
        }

        @Override
        public boolean hasResponse() {
            return hasResponse;
        }

        @Override
        public boolean decodeResponse() {
            return false;
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
        thenInFlightRequestCountEquals(1);
        thenQueuedResponseCountEquals(0);
    }

    @Test
    void testInboundNonFrameForwarded() {
        Object request = new Object();
        whenWriteInboundMessage(request);
        thenInboundContains(request);
        thenInFlightRequestCountEquals(0);
        thenQueuedResponseCountEquals(0);
    }

    @Test
    void testSingleRequestResponse() {
        TestFrame request = new TestFrame(1);
        whenWriteInboundMessage(request);
        thenInboundContains(request);
        thenInFlightRequestCountEquals(1);
        thenQueuedResponseCountEquals(0);
        TestFrame response = new TestFrame(1);
        whenWriteOutboundMessage(response);
        thenInFlightRequestCountEquals(0);
        thenQueuedResponseCountEquals(0);
        thenOutboundContains(response);
    }

    @Test
    void testSingleRequestWithResponseExpected() {
        TestRequestFrame request = new TestRequestFrame(1, true);
        whenWriteInboundMessage(request);
        thenInboundContains(request);
        thenInFlightRequestCountEquals(1);
        thenQueuedResponseCountEquals(0);
        TestFrame response = new TestFrame(1);
        whenWriteOutboundMessage(response);
        thenInFlightRequestCountEquals(0);
        thenQueuedResponseCountEquals(0);
        thenOutboundContains(response);
    }

    /**
     * Some specific requests do not have a corresponding response expected by the Kafka Client.
     * In this case we do not want these requests to affect response re-ordering. So their
     * correlation ids should not be added to the queue.
     */
    @Test
    void testSingleRequestResponseNotDelayedByRequestWithNoResponse() {
        TestRequestFrame noResponseRequest = new TestRequestFrame(1, false);
        whenWriteInboundMessage(noResponseRequest);
        thenInboundContains(noResponseRequest);

        TestFrame requestForFrame2 = new TestFrame(2);
        whenWriteInboundMessage(requestForFrame2);
        thenInboundContains(requestForFrame2);
        thenInFlightRequestCountEquals(1);
        thenQueuedResponseCountEquals(0);

        TestFrame responseForFrame2 = new TestFrame(2);
        whenWriteOutboundMessage(responseForFrame2);
        thenInFlightRequestCountEquals(0);
        thenQueuedResponseCountEquals(0);
        thenOutboundContains(responseForFrame2);
    }

    @Test
    void testThatResponseOrderIsPreserved() {
        whenWriteInboundMessage(new TestFrame(1));
        whenWriteInboundMessage(new TestFrame(2));

        TestFrame responseForFrame1 = new TestFrame(1);
        whenWriteOutboundMessage(responseForFrame1);
        thenInFlightRequestCountEquals(1);
        thenQueuedResponseCountEquals(0);
        thenOutboundContains(responseForFrame1);

        TestFrame responseForFrame2 = new TestFrame(2);
        whenWriteOutboundMessage(responseForFrame2);
        thenInFlightRequestCountEquals(0);
        thenQueuedResponseCountEquals(0);
        thenOutboundContains(responseForFrame2);
    }

    @Test
    void testMultipleInflightMessagesThatRespondOutOfOrder() {
        whenWriteInboundMessage(new TestFrame(1));
        whenWriteInboundMessage(new TestFrame(2));

        TestFrame responseForFrame2 = new TestFrame(2);
        whenWriteOutboundMessage(responseForFrame2);
        thenInFlightRequestCountEquals(2);
        thenQueuedResponseCountEquals(1);
        thenOutboundIsEmpty();

        TestFrame responseForFrame1 = new TestFrame(1);
        whenWriteOutboundMessage(responseForFrame1);
        thenInFlightRequestCountEquals(0);
        thenQueuedResponseCountEquals(0);
        thenOutboundContains(responseForFrame1);
        thenOutboundContains(responseForFrame2);
    }

    @Test
    void testComplexScenarioWherePendingResponsesCanBePartiallyDrained() {
        whenWriteInboundMessage(new TestFrame(1));
        whenWriteInboundMessage(new TestFrame(2));
        whenWriteInboundMessage(new TestFrame(3));
        whenWriteInboundMessage(new TestFrame(4));

        TestFrame responseForFrame2 = new TestFrame(2);
        whenWriteOutboundMessage(responseForFrame2);
        thenInFlightRequestCountEquals(4);
        thenQueuedResponseCountEquals(1);
        thenOutboundIsEmpty();

        TestFrame responseForFrame4 = new TestFrame(4);
        whenWriteOutboundMessage(responseForFrame4);
        thenInFlightRequestCountEquals(4);
        thenQueuedResponseCountEquals(2);
        thenOutboundIsEmpty();

        TestFrame responseForFrame1 = new TestFrame(1);
        whenWriteOutboundMessage(responseForFrame1);
        // the response for correlationId 2 should be drained after the response for correlationId 1 is written
        // but the response for correlationId 4 cannot be sent until we have written the response for correlationId 3
        thenInFlightRequestCountEquals(2);
        thenQueuedResponseCountEquals(1);
        thenOutboundContains(responseForFrame1);
        thenOutboundContains(responseForFrame2);
        thenOutboundIsEmpty();

        TestFrame responseForFrame3 = new TestFrame(3);
        whenWriteOutboundMessage(responseForFrame3);
        thenInFlightRequestCountEquals(0);
        thenQueuedResponseCountEquals(0);
        thenOutboundContains(responseForFrame3);
        thenOutboundContains(responseForFrame4);
        thenOutboundIsEmpty();
    }

    @Test
    void testMultiplePendingAreDrained() {
        whenWriteInboundMessage(new TestFrame(1));
        whenWriteInboundMessage(new TestFrame(2));
        whenWriteInboundMessage(new TestFrame(3));

        TestFrame responseForFrame2 = new TestFrame(2);
        whenWriteOutboundMessage(responseForFrame2);
        thenInFlightRequestCountEquals(3);
        thenQueuedResponseCountEquals(1);
        thenOutboundIsEmpty();

        TestFrame responseForFrame3 = new TestFrame(3);
        whenWriteOutboundMessage(responseForFrame3);
        thenInFlightRequestCountEquals(3);
        thenQueuedResponseCountEquals(2);
        thenOutboundIsEmpty();

        TestFrame responseForFrame1 = new TestFrame(1);
        whenWriteOutboundMessage(responseForFrame1);
        thenInFlightRequestCountEquals(0);
        thenQueuedResponseCountEquals(0);
        thenOutboundContains(responseForFrame1);
        thenOutboundContains(responseForFrame2);
        thenOutboundContains(responseForFrame3);
        thenOutboundIsEmpty();
    }

    @Test
    void testUnexpectedResponseWithNoMatchingRequest() {
        TestFrame response = new TestFrame(1);
        whenWriteOutboundMessage(response);
        thenInFlightRequestCountEquals(0);
        thenQueuedResponseCountEquals(0);
        thenOutboundContains(response);
    }

    @Test
    void testOutboundNonFrameAreForwarded() {
        Object response = new Object();
        whenWriteOutboundMessage(response);
        thenInFlightRequestCountEquals(0);
        thenQueuedResponseCountEquals(0);
        thenOutboundContains(response);
    }

    private void whenWriteOutboundMessage(Object object) {
        embeddedChannel.writeOneOutbound(object);
    }

    private void thenQueuedResponseCountEquals(int expecteQueuedResponses) {
        assertThat(orderer.queuedResponseCount()).isEqualTo(expecteQueuedResponses);
    }

    private void thenInFlightRequestCountEquals(int expectedInFlightRequests) {
        assertThat(orderer.inFlightRequestCount()).isEqualTo(expectedInFlightRequests);
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
