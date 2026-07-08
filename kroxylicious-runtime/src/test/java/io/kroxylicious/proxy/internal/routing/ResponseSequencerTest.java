/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.netty.channel.Channel;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class ResponseSequencerTest {

    @Mock
    private Channel channel;

    private ResponseSequencer sequencer;

    @BeforeEach
    void setUp() {
        sequencer = new ResponseSequencer(channel);
    }

    @Test
    void shouldWriteAndFlushWhenFirstSequenceArrives() {
        // Given
        long seq = sequencer.allocateSequence();
        Object frame = new Object();

        // When
        sequencer.submit(seq, frame);

        // Then
        verify(channel).write(frame);
        verify(channel).flush();
    }

    @Test
    void shouldBufferOutOfOrderResponses() {
        // Given
        long seq0 = sequencer.allocateSequence();
        long seq1 = sequencer.allocateSequence();
        Object frame0 = new Object();
        Object frame1 = new Object();

        // When: seq1 arrives first
        sequencer.submit(seq1, frame1);

        // Then: nothing written yet
        verifyNoInteractions(channel);

        // When: seq0 arrives
        sequencer.submit(seq0, frame0);

        // Then: both written in order
        InOrder order = inOrder(channel);
        order.verify(channel).write(frame0);
        order.verify(channel).write(frame1);
        order.verify(channel).flush();
    }

    @Test
    void shouldFlushRunOfConsecutiveSequences() {
        // Given: allocate three sequences
        long seq0 = sequencer.allocateSequence();
        long seq1 = sequencer.allocateSequence();
        long seq2 = sequencer.allocateSequence();
        Object frame0 = new Object();
        Object frame1 = new Object();
        Object frame2 = new Object();

        // When: 2 and 1 buffered before 0 arrives
        sequencer.submit(seq2, frame2);
        sequencer.submit(seq1, frame1);
        sequencer.submit(seq0, frame0);

        // Then: all three written in order, one flush
        InOrder order = inOrder(channel);
        order.verify(channel).write(frame0);
        order.verify(channel).write(frame1);
        order.verify(channel).write(frame2);
        order.verify(channel).flush();
    }

    @Test
    void shouldHandleMultipleRoundsOfOrdering() {
        // Given: first wave — seq 0, 1, 2
        long seq0 = sequencer.allocateSequence();
        long seq1 = sequencer.allocateSequence();
        long seq2 = sequencer.allocateSequence();
        Object frame0 = new Object();
        Object frame1 = new Object();
        Object frame2 = new Object();

        // When: seq 0 arrives in order — immediately flushed
        sequencer.submit(seq0, frame0);

        // Second wave — seq 3
        long seq3 = sequencer.allocateSequence();
        Object frame3 = new Object();

        // seq 2 arrives before seq 1 — must buffer
        sequencer.submit(seq2, frame2);
        // seq 1 arrives — drains 1 and 2 together with a single flush
        sequencer.submit(seq1, frame1);
        // seq 3 arrives in order immediately after — its own flush
        sequencer.submit(seq3, frame3);

        // Then: flush after seq 0, then flush after draining seq 1+2, then flush after seq 3
        InOrder order = inOrder(channel);
        order.verify(channel).write(frame0);
        order.verify(channel).flush();
        order.verify(channel).write(frame1);
        order.verify(channel).write(frame2);
        order.verify(channel).flush();
        order.verify(channel).write(frame3);
        order.verify(channel).flush();
    }
}
