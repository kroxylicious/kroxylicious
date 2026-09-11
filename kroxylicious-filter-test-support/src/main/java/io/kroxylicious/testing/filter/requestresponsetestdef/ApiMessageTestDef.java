/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.filter.requestresponsetestdef;

import com.fasterxml.jackson.databind.JsonNode;

import io.kroxylicious.kafka.common.protocol.ApiMessage;

/**
 * A test definition for a single Kafka API message.
 *
 * @param message the API message to use as test input
 * @param expectedPatch a JSON patch describing the expected difference between the input message and the message after filtering
 */
public record ApiMessageTestDef(ApiMessage message, JsonNode expectedPatch) {}
