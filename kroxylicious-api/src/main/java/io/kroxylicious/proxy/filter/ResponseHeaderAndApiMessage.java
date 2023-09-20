/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.Objects;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

/**
 * Tuple encapsulating a response message and its header.
 * @param header message header
 * @param message message
 * @param <M> The type of the message
 */
public record ResponseHeaderAndApiMessage<M extends ApiMessage>(ResponseHeaderData header, M message) {
    public ResponseHeaderAndApiMessage {
        Objects.requireNonNull(header);
        Objects.requireNonNull(message);
    }
}
