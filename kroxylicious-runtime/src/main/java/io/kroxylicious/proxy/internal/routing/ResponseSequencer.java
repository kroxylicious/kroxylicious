/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.HashMap;
import java.util.Map;

import io.netty.channel.Channel;

/**
 * Ensures dynamically-routed responses are flushed to the client channel
 * in the same order the corresponding requests arrived. Not thread-safe;
 * all callers must be on the same Netty event loop.
 */
class ResponseSequencer {

    private static final Object SKIP_SENTINEL = new Object();

    private final Channel clientChannel;
    private final Map<Long, Object> buffered = new HashMap<>();
    private long nextSequenceToWrite;
    private long nextSequenceToAllocate;

    ResponseSequencer(Channel clientChannel) {
        this.clientChannel = clientChannel;
    }

    /**
     * Allocates the next sequence number. Called once per dynamically-routed
     * client request, in request-arrival order.
     */
    long allocateSequence() {
        return nextSequenceToAllocate++;
    }

    /**
     * Submits a response for the given sequence number. If this is the next
     * expected sequence, it and any consecutively buffered successors are
     * flushed to the client channel immediately.
     */
    void submit(long sequence, Object responseFrame) {
        if (sequence == nextSequenceToWrite) {
            clientChannel.write(responseFrame);
            nextSequenceToWrite++;
            drainAndFlush();
        }
        else {
            buffered.put(sequence, responseFrame);
        }
    }

    /**
     * Marks the given sequence as having no response to write. If this is the
     * next expected sequence, any consecutively buffered successors are flushed
     * to the client channel immediately.
     */
    void skip(long sequence) {
        if (sequence == nextSequenceToWrite) {
            nextSequenceToWrite++;
            drainAndFlush();
        }
        else {
            buffered.put(sequence, SKIP_SENTINEL);
        }
    }

    private void drainAndFlush() {
        while (buffered.containsKey(nextSequenceToWrite)) {
            Object value = buffered.remove(nextSequenceToWrite);
            nextSequenceToWrite++;
            if (value != SKIP_SENTINEL) {
                clientChannel.write(value);
            }
        }
        clientChannel.flush();
    }
}
