/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.router.topic;

import java.util.Objects;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.router.Response;

/**
 * Immutable {@link Response} implementation for router-synthesised responses.
 */
record SimpleResponse(ResponseHeaderData header,
                      ApiMessage body)
        implements Response {

    SimpleResponse {
        Objects.requireNonNull(header);
        Objects.requireNonNull(body);
    }
}
