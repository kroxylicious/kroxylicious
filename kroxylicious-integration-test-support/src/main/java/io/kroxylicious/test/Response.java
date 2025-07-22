/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test;

public record Response(ResponsePayload payload,
                       int sequenceNumber,
                       int correlationId,
                       byte[] rawHeaderAndBodyBytes) {}
