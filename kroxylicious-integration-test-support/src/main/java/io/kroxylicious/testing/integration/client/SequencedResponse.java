/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.integration.client;

import io.kroxylicious.testing.integration.codec.DecodedResponseFrame;

public record SequencedResponse(DecodedResponseFrame<?> frame, int sequenceNumber) {}
