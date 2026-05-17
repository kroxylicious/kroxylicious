/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.netty.channel.embedded.EmbeddedChannel;

import io.kroxylicious.proxy.frame.DecodedResponseFrame;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ResponseSequencer}, which ensures dynamically-routed
 * responses are flushed to the client channel in request-arrival order.
 */
class ResponseSequencerTest {

    private EmbeddedChannel channel;
    private ResponseSequencer sequencer;

    @BeforeEach
    void setUp() {
        channel = new EmbeddedChannel();
        sequencer = new ResponseSequencer(channel);
    }

    @Test
    void shouldFlushImmediatelyWhenInOrder() {
        long seq0 = sequencer.allocateSequence();
        long seq1 = sequencer.allocateSequence();

        var frame0 = responseFrame(0);
        var frame1 = responseFrame(1);

        sequencer.submit(seq0, frame0);
        assertThat(readAllOutbound()).containsExactly(frame0);

        sequencer.submit(seq1, frame1);
        assertThat(readAllOutbound()).containsExactly(frame1);
    }

    @Test
    void shouldBufferOutOfOrderAndFlushOnceGapFills() {
        long seq0 = sequencer.allocateSequence();
        long seq1 = sequencer.allocateSequence();
        long seq2 = sequencer.allocateSequence();

        var frame0 = responseFrame(0);
        var frame1 = responseFrame(1);
        var frame2 = responseFrame(2);

        sequencer.submit(seq2, frame2);
        assertThat(readAllOutbound()).isEmpty();

        sequencer.submit(seq1, frame1);
        assertThat(readAllOutbound()).isEmpty();

        sequencer.submit(seq0, frame0);
        assertThat(readAllOutbound()).containsExactly(frame0, frame1, frame2);
    }

    @Test
    void shouldFlushConsecutiveRunWhenMiddleArrives() {
        long seq0 = sequencer.allocateSequence();
        long seq1 = sequencer.allocateSequence();
        long seq2 = sequencer.allocateSequence();
        long seq3 = sequencer.allocateSequence();

        var frame0 = responseFrame(0);
        var frame1 = responseFrame(1);
        var frame2 = responseFrame(2);
        var frame3 = responseFrame(3);

        sequencer.submit(seq0, frame0);
        assertThat(readAllOutbound()).containsExactly(frame0);

        sequencer.submit(seq2, frame2);
        sequencer.submit(seq3, frame3);
        assertThat(readAllOutbound()).isEmpty();

        sequencer.submit(seq1, frame1);
        assertThat(readAllOutbound()).containsExactly(frame1, frame2, frame3);
    }

    @Test
    void shouldAllocateSequentialNumbers() {
        assertThat(sequencer.allocateSequence()).isEqualTo(0);
        assertThat(sequencer.allocateSequence()).isEqualTo(1);
        assertThat(sequencer.allocateSequence()).isEqualTo(2);
    }

    private DecodedResponseFrame<FetchResponseData> responseFrame(int correlationId) {
        return new DecodedResponseFrame<>(
                (short) 12,
                correlationId,
                new ResponseHeaderData().setCorrelationId(correlationId),
                new FetchResponseData());
    }

    private List<Object> readAllOutbound() {
        List<Object> out = new ArrayList<>();
        Object msg;
        while ((msg = channel.readOutbound()) != null) {
            out.add(msg);
        }
        return out;
    }
}
