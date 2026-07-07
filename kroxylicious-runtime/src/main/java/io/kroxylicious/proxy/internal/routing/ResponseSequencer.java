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
            while (buffered.containsKey(nextSequenceToWrite)) {
                clientChannel.write(buffered.remove(nextSequenceToWrite));
                nextSequenceToWrite++;
            }
            clientChannel.flush();
        }
        else {
            buffered.put(sequence, responseFrame);
        }
    }
}
