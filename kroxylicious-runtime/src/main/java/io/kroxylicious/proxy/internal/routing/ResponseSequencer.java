/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.util.concurrent.ScheduledFuture;

/**
 * Ensures dynamically-routed responses are flushed to the client channel
 * in the same order the corresponding requests arrived. Not thread-safe;
 * all callers must be on the same Netty event loop.
 */
class ResponseSequencer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResponseSequencer.class);
    private static final long STALL_DETECTION_SECONDS = 20;

    private final Channel clientChannel;
    private final Map<Long, Object> buffered = new HashMap<>();
    private long nextSequenceToWrite;
    private long nextSequenceToAllocate;
    private ScheduledFuture<?> stallDetector;

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
            cancelStallDetector();
            clientChannel.write(responseFrame);
            nextSequenceToWrite++;
            while (buffered.containsKey(nextSequenceToWrite)) {
                clientChannel.write(buffered.remove(nextSequenceToWrite));
                nextSequenceToWrite++;
            }
            clientChannel.flush();
            if (!buffered.isEmpty()) {
                scheduleStallDetector();
            }
        }
        else {
            buffered.put(sequence, responseFrame);
            scheduleStallDetector();
        }
    }

    private void scheduleStallDetector() {
        if (stallDetector == null || stallDetector.isDone()) {
            stallDetector = clientChannel.eventLoop().schedule(this::logStall,
                    STALL_DETECTION_SECONDS, TimeUnit.SECONDS);
        }
    }

    private void cancelStallDetector() {
        if (stallDetector != null) {
            stallDetector.cancel(false);
            stallDetector = null;
        }
    }

    private void logStall() {
        if (!buffered.isEmpty()) {
            LOGGER.atWarn()
                    .addKeyValue("nextSequenceToWrite", nextSequenceToWrite)
                    .addKeyValue("bufferedCount", buffered.size())
                    .addKeyValue("bufferedSequences", buffered.keySet())
                    .addKeyValue("nextSequenceToAllocate", nextSequenceToAllocate)
                    .log("Response sequencer stalled: buffered responses waiting for sequence that has not arrived."
                            + " This is likely a bug in the runtime — a sequence number was allocated but never submitted.");
        }
    }
}
