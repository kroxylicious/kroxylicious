/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.integration;

/**
 * A Kafka response received by the test client.
 *
 * @param payload the response payload
 * @param sequenceNumber the position of this response in the sequence of responses received on the connection
 */
public record Response(ResponsePayload payload,
                       int sequenceNumber) {}
