/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.netty.channel.embedded.EmbeddedChannel;

import static org.assertj.core.api.Assertions.assertThat;

class BufferingGatewayHandlerTest {

    private EmbeddedChannel channel;
    private BufferingGatewayHandler handler;
    private UserEventCollector userEventCollector;

    @BeforeEach
    void setUp() {
        handler = new BufferingGatewayHandler();
        userEventCollector = new UserEventCollector();
        channel = new EmbeddedChannel(handler, userEventCollector);
    }

    @AfterEach
    void tearDown() {
        if (channel.isActive()) {
            channel.finish();
        }
    }

    @Test
    void shouldBufferMessagesBeforeGateOpens() {
        Object msg1 = new Object();
        Object msg2 = new Object();

        channel.writeInbound(msg1);
        channel.writeInbound(msg2);

        Object read = channel.readInbound();
        assertThat(read).isNull();
    }

    @Test
    void shouldForwardMessagesDirectlyAfterGateOpens() {
        channel.pipeline().fireUserEventTriggered(new TransportSubjectBuilt());

        Object msg = new Object();
        channel.writeInbound(msg);

        Object read = channel.readInbound();
        assertThat(read).isSameAs(msg);
    }

    @Test
    void shouldFlushBufferWhenTransportSubjectBuiltEventReceived() {
        Object msg1 = new Object();
        Object msg2 = new Object();
        Object msg3 = new Object();

        channel.writeInbound(msg1);
        channel.writeInbound(msg2);
        channel.writeInbound(msg3);

        Object read = channel.readInbound();
        assertThat(read).isNull();

        channel.pipeline().fireUserEventTriggered(new TransportSubjectBuilt());

        Object read1 = channel.readInbound();
        assertThat(read1).isSameAs(msg1);
        Object read2 = channel.readInbound();
        assertThat(read2).isSameAs(msg2);
        Object read3 = channel.readInbound();
        assertThat(read3).isSameAs(msg3);
    }

    @Test
    void shouldRemoveItselfFromPipelineAfterFlushingBuffer() {
        channel.writeInbound(new Object());

        assertThat(channel.pipeline().get(BufferingGatewayHandler.class)).isSameAs(handler);

        channel.pipeline().fireUserEventTriggered(new TransportSubjectBuilt());

        assertThat(channel.pipeline().get(BufferingGatewayHandler.class)).isNull();
    }

    @Test
    void shouldForwardTransportSubjectBuiltEvent() {
        TransportSubjectBuilt event = new TransportSubjectBuilt();

        channel.pipeline().fireUserEventTriggered(event);

        Object receivedEvent = userEventCollector.readUserEvent();
        assertThat(receivedEvent).isSameAs(event);
    }

    @Test
    void shouldForwardOtherUserEvents() {
        Object customEvent = new Object();

        channel.pipeline().fireUserEventTriggered(customEvent);

        Object receivedEvent = userEventCollector.readUserEvent();
        assertThat(receivedEvent).isSameAs(customEvent);
    }

    @Test
    void shouldMaintainFifoOrderWhenFlushingBuffer() {
        Object msg1 = new Object();
        Object msg2 = new Object();
        Object msg3 = new Object();

        channel.writeInbound(msg1);
        channel.writeInbound(msg2);
        channel.writeInbound(msg3);

        channel.pipeline().fireUserEventTriggered(new TransportSubjectBuilt());

        Object read1 = channel.readInbound();
        assertThat(read1).isSameAs(msg1);
        Object read2 = channel.readInbound();
        assertThat(read2).isSameAs(msg2);
        Object read3 = channel.readInbound();
        assertThat(read3).isSameAs(msg3);
    }

    @Test
    void shouldHandleEmptyBufferWhenTransportSubjectBuiltReceived() {
        channel.pipeline().fireUserEventTriggered(new TransportSubjectBuilt());

        Object read = channel.readInbound();
        assertThat(read).isNull();
        assertThat(channel.pipeline().get(BufferingGatewayHandler.class)).isNull();
    }

    @Test
    void shouldNotBufferAfterGateOpens() {
        Object msg1 = new Object();
        Object msg2 = new Object();

        channel.writeInbound(msg1);
        channel.pipeline().fireUserEventTriggered(new TransportSubjectBuilt());

        Object read1 = channel.readInbound();
        assertThat(read1).isSameAs(msg1);

        channel.writeInbound(msg2);

        Object read2 = channel.readInbound();
        assertThat(read2).isSameAs(msg2);
    }
}
