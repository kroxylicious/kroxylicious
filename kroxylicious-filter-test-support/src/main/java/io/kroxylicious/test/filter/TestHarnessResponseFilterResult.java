/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.filter;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.ResponseFilterResult;

public record TestHarnessResponseFilterResult(
        ResponseHeaderData header,
        ApiMessage message,
        boolean closeConnection,
        boolean drop
) implements ResponseFilterResult {
}
