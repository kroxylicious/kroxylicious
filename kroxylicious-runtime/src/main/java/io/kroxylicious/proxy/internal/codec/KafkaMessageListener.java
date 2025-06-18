/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.codec;

import io.kroxylicious.proxy.frame.Frame;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Callback invoked by the {@link KafkaMessageEncoder} and {@link KafkaMessageDecoder}
 * on encode or decode of each message.
 */
@FunctionalInterface
public interface KafkaMessageListener {

    /**
     * Called each time a kafka message is encoded or decoded.
     * @param frame - frame object
     * @param wireLength this is the <a href="https://kafka.apache.org/protocol#protocol_common">message size</a> plus
     * the {@link Frame#FRAME_SIZE_LENGTH}.
     */
    void onMessage(@NonNull Frame frame, int wireLength);

    static KafkaMessageListener chainOf(KafkaMessageListener... listeners) {
        return (frame, wireLength) -> {
            for (var listener : listeners) {
                listener.onMessage(frame, wireLength);
            }
        };
    }
}
