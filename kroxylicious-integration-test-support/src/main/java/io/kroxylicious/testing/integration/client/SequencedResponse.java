/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.integration.client;

import io.kroxylicious.testing.integration.codec.DecodedResponseFrame;

/**
 * A decoded response frame together with the position at which it was received on the connection.
 *
 * @param frame the decoded response frame
 * @param sequenceNumber the position of this response in the sequence of responses received on the connection
 */
public record SequencedResponse(DecodedResponseFrame<?> frame, int sequenceNumber) {}
