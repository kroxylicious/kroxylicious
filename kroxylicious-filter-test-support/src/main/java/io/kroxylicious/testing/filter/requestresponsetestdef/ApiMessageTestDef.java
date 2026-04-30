/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.filter.requestresponsetestdef;

import org.apache.kafka.common.protocol.ApiMessage;

import com.fasterxml.jackson.databind.JsonNode;

public record ApiMessageTestDef(ApiMessage message, JsonNode expectedPatch) {}
