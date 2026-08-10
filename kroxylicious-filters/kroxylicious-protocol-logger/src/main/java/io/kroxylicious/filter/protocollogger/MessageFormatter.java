/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.protocollogger;

import java.util.EnumSet;
import java.util.Set;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import io.kroxylicious.testing.filter.requestresponsetestdef.KafkaApiMessageConverter;

class MessageFormatter {

    static final String BODY_WITHHELD_MESSAGE = "<body withheld: credential-bearing API>";

    static final Set<ApiKeys> CREDENTIAL_BEARING_API_KEYS = EnumSet.of(
            ApiKeys.SASL_AUTHENTICATE,
            ApiKeys.CREATE_DELEGATION_TOKEN,
            ApiKeys.RENEW_DELEGATION_TOKEN,
            ApiKeys.EXPIRE_DELEGATION_TOKEN,
            ApiKeys.ALTER_USER_SCRAM_CREDENTIALS,
            ApiKeys.DESCRIBE_DELEGATION_TOKEN);

    private static final ObjectWriter PRETTY_WRITER = new ObjectMapper().writerWithDefaultPrettyPrinter();

    MessageFormatter() {
    }

    String formatRequest(ApiKeys apiKey, short apiVersion, ApiMessage message) {
        if (CREDENTIAL_BEARING_API_KEYS.contains(apiKey)) {
            return BODY_WITHHELD_MESSAGE;
        }
        KafkaApiMessageConverter.Converter converter = KafkaApiMessageConverter.requestConverterFor(apiKey.messageType);
        JsonNode json = converter.writer().apply(message, apiVersion);
        return prettyPrint(json);
    }

    String formatResponse(ApiKeys apiKey, short apiVersion, ApiMessage message) {
        if (CREDENTIAL_BEARING_API_KEYS.contains(apiKey)) {
            return BODY_WITHHELD_MESSAGE;
        }
        KafkaApiMessageConverter.Converter converter = KafkaApiMessageConverter.responseConverterFor(apiKey.messageType);
        JsonNode json = converter.writer().apply(message, apiVersion);
        return prettyPrint(json);
    }

    private static String prettyPrint(JsonNode json) {
        try {
            return PRETTY_WRITER.writeValueAsString(json);
        }
        catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize JsonNode", e);
        }
    }

}
