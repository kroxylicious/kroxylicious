/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.protocollogger;

import java.util.EnumSet;
import java.util.Set;

import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.ResponseHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.kroxylicious.kafka.message.json.KafkaApiMessageConverter;

class MessageFormatter {

    private static final String HEADER_FIELD = "header";
    private static final String PAYLOAD_FIELD = "payload";
    private static final String PAYLOAD_WITHHELD_FIELD = "payloadWithheld";
    private static final String TYPE_FIELD = "type";
    private static final String API_KEY_FIELD = "apiKey";
    private static final String API_VERSION_FIELD = "apiVersion";
    private static final String CORRELATION_ID_FIELD = "correlationId";
    private static final String CLIENT_ID_FIELD = "clientId";

    static final String PAYLOAD_WITHHELD_REASON = "credential-bearing API";

    static final Set<ApiKeys> CREDENTIAL_BEARING_API_KEYS = EnumSet.of(
            ApiKeys.SASL_AUTHENTICATE,
            ApiKeys.CREATE_DELEGATION_TOKEN,
            ApiKeys.RENEW_DELEGATION_TOKEN,
            ApiKeys.EXPIRE_DELEGATION_TOKEN,
            ApiKeys.ALTER_USER_SCRAM_CREDENTIALS,
            ApiKeys.DESCRIBE_DELEGATION_TOKEN);

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final ObjectWriter PRETTY_WRITER = MAPPER.writerWithDefaultPrettyPrinter();

    MessageFormatter() {
    }

    ObjectNode formatRequest(ApiKeys apiKey, short apiVersion, RequestHeaderData header, ApiMessage message) {
        ObjectNode entry = MAPPER.createObjectNode();
        entry.set(HEADER_FIELD, buildRequestHeader(apiKey, apiVersion, header));
        if (CREDENTIAL_BEARING_API_KEYS.contains(apiKey)) {
            entry.putNull(PAYLOAD_FIELD);
            entry.put(PAYLOAD_WITHHELD_FIELD, PAYLOAD_WITHHELD_REASON);
        }
        else {
            KafkaApiMessageConverter.Converter converter = KafkaApiMessageConverter.requestConverterFor(apiKey.messageType);
            JsonNode payload = converter.writer().apply(message, apiVersion);
            entry.set(PAYLOAD_FIELD, payload);
        }
        return entry;
    }

    ObjectNode formatResponse(ApiKeys apiKey, short apiVersion, ResponseHeaderData header, ApiMessage message) {
        ObjectNode entry = MAPPER.createObjectNode();
        entry.set(HEADER_FIELD, buildResponseHeader(apiKey, apiVersion, header));
        if (CREDENTIAL_BEARING_API_KEYS.contains(apiKey)) {
            entry.putNull(PAYLOAD_FIELD);
            entry.put(PAYLOAD_WITHHELD_FIELD, PAYLOAD_WITHHELD_REASON);
        }
        else {
            KafkaApiMessageConverter.Converter converter = KafkaApiMessageConverter.responseConverterFor(apiKey.messageType);
            JsonNode payload = converter.writer().apply(message, apiVersion);
            entry.set(PAYLOAD_FIELD, payload);
        }
        return entry;
    }

    private static ObjectNode buildRequestHeader(ApiKeys apiKey, short apiVersion, RequestHeaderData header) {
        ObjectNode h = MAPPER.createObjectNode();
        h.put(TYPE_FIELD, "REQUEST");
        h.put(API_KEY_FIELD, apiKey.name());
        h.put(API_VERSION_FIELD, (int) apiVersion);
        h.put(CORRELATION_ID_FIELD, header.correlationId());
        h.put(CLIENT_ID_FIELD, header.clientId());
        return h;
    }

    private static ObjectNode buildResponseHeader(ApiKeys apiKey, short apiVersion, ResponseHeaderData header) {
        ObjectNode h = MAPPER.createObjectNode();
        h.put(TYPE_FIELD, "RESPONSE");
        h.put(API_KEY_FIELD, apiKey.name());
        h.put(API_VERSION_FIELD, (int) apiVersion);
        h.put(CORRELATION_ID_FIELD, header.correlationId());
        return h;
    }

    static String prettyPrint(ObjectNode node) {
        try {
            return PRETTY_WRITER.writeValueAsString(node);
        }
        catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize JsonNode", e);
        }
    }

}
