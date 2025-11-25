/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.validators.bytebuf;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.Record;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;

import io.apicurio.registry.serde.BaseSerde;
import io.apicurio.registry.serde.headers.KafkaSerdeHeaders;

import io.kroxylicious.proxy.filter.validation.config.SchemaValidationConfig.WireFormatVersion;
import io.kroxylicious.proxy.filter.validation.validators.Result;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static io.kroxylicious.test.record.RecordTestUtils.record;
import static org.assertj.core.api.Assertions.assertThat;

public class JsonSchemaBytebufValidatorTest {

    private static final long CONTENT_ID = 1L;
    private static final byte[] VALID_JSON = """
            {"firstName":"a","lastName":"b"}""".getBytes(StandardCharsets.UTF_8);
    private static final byte[] INVALID_JSON = """
            {"firstName":"a","lastName":"b","age":-3}""".getBytes(StandardCharsets.UTF_8);
    private static final byte[] RECORD_KEY = "a".getBytes(StandardCharsets.UTF_8);

    private static WireMockServer registryServer;

    private static Map<String, Object> apicurioConfig;

    private static final String JSON_SCHEMA = """
            {
              "$id": "https://example.com/person.schema.json",
              "$schema": "http://json-schema.org/draft-07/schema#",
              "title": "Person",
              "type": "object",
              "properties": {
                "firstName": {
                  "type": "string",
                  "description": "The person's first name."
                },
                "lastName": {
                  "type": "string",
                  "description": "The person's last name."
                },
                "age": {
                  "description": "Age in years which must be equal to or greater than zero.",
                  "type": "integer",
                  "minimum": 0
                }
              }
            }
            """;

    @BeforeAll
    public static void initMockRegistry() {
        registryServer = new WireMockServer(
                wireMockConfig()
                        .dynamicPort());

        registryServer.start();

        registryServer.stubFor(
                get(urlEqualTo("/apis/registry/v3/ids/contentIds/1"))
                        .willReturn(WireMock.aResponse()
                                .withHeader("Content-Type", "application/json")
                                .withBody(JSON_SCHEMA)));

        registryServer.stubFor(
                get(urlEqualTo("/apis/registry/v3/ids/globalIds/1?dereference=false"))
                        .willReturn(WireMock.aResponse()
                                .withHeader("Content-Type", "application/json")
                                .withBody(JSON_SCHEMA)));

        registryServer.stubFor(
                get(urlEqualTo("/apis/registry/v3/ids/contentIds/1/references"))
                        .willReturn(WireMock.aResponse()
                                .withHeader("Content-Type", "application/json")
                                .withBody("[]")));

        apicurioConfig = Map.of("apicurio.registry.url", registryServer.baseUrl() + "/apis/registry/v3");
    }

    @AfterAll
    public static void shutdownMockRegistry() {
        registryServer.shutdown();
    }

    @Test
    void valuePassesSchemaValidation() {
        Record record = record(RECORD_KEY, VALID_JSON);
        BytebufValidator validator = BytebufValidators.jsonSchemaValidator(apicurioConfig, CONTENT_ID, WireFormatVersion.V3);
        var future = validator.validate(record.value(), record, false);

        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(true, Result::valid);
    }

    @Test
    void jsonValueFailsSchemaValidation() {
        Record record = record(RECORD_KEY, INVALID_JSON);
        BytebufValidator validator = BytebufValidators.jsonSchemaValidator(apicurioConfig, CONTENT_ID, WireFormatVersion.V3);
        var future = validator.validate(record.value(), record, false);

        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(false, Result::valid);
    }

    @Test
    void nonJsonValueFailsToParse() {
        Record record = record(RECORD_KEY, "not a json value".getBytes(StandardCharsets.UTF_8));
        BytebufValidator validator = BytebufValidators.jsonSchemaValidator(apicurioConfig, CONTENT_ID, WireFormatVersion.V3);
        var future = validator.validate(record.value(), record, false);
        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(false, Result::valid);
    }

    @Test
    void valueWithCorrectSchemaIdInHeaderPassesValidation() {
        Header[] headers = new Header[]{ new RecordHeader(KafkaSerdeHeaders.HEADER_VALUE_CONTENT_ID, toByteArray(CONTENT_ID)) };
        Record record = record(RECORD_KEY, VALID_JSON, headers);
        BytebufValidator validator = BytebufValidators.jsonSchemaValidator(apicurioConfig, CONTENT_ID, WireFormatVersion.V3);
        var future = validator.validate(record.value(), record, false);
        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(true, Result::valid);
    }

    @Test
    void valueWithCorrectSchemaIdInBodyPassesValidation() {
        var value = asSchemaIdPrefixBuf(CONTENT_ID, VALID_JSON);
        Record record = record(RECORD_KEY, value);
        BytebufValidator validator = BytebufValidators.jsonSchemaValidator(apicurioConfig, CONTENT_ID, WireFormatVersion.V3);
        var future = validator.validate(record.value(), record, false);
        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(true, Result::valid);
    }

    @Test
    void valueWithWrongSchemaIdInHeaderRejected() {
        Header[] headers = new Header[]{ new RecordHeader(KafkaSerdeHeaders.HEADER_VALUE_CONTENT_ID, toByteArray(CONTENT_ID + 1)) };
        Record record = record(RECORD_KEY, VALID_JSON, headers);
        BytebufValidator validator = BytebufValidators.jsonSchemaValidator(apicurioConfig, CONTENT_ID, WireFormatVersion.V3);
        var future = validator.validate(record.value(), record, false);
        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .isEqualTo(new Result(false, "Unexpected schema id in record (2), expecting 1"));
    }

    @Test
    void valueWithUnexpectedSchemaIdInBodyRejected() {
        var value = asSchemaIdPrefixBuf(CONTENT_ID + 1, VALID_JSON);
        Record record = record(RECORD_KEY, value);
        BytebufValidator validator = BytebufValidators.jsonSchemaValidator(apicurioConfig, CONTENT_ID, WireFormatVersion.V3);
        var future = validator.validate(record.value(), record, false);
        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .isEqualTo(new Result(false, "Unexpected schema id in record (2), expecting 1"));
    }

    @Test
    void keyWithCorrectSchemaIdInHeaderPassesValidation() {
        Header[] headers = new Header[]{ new RecordHeader(KafkaSerdeHeaders.HEADER_VALUE_CONTENT_ID, toByteArray(CONTENT_ID)) };
        Record record = record(VALID_JSON, null, headers);
        BytebufValidator validator = BytebufValidators.jsonSchemaValidator(apicurioConfig, CONTENT_ID, WireFormatVersion.V3);
        var future = validator.validate(record.key(), record, true);
        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(true, Result::valid);
    }

    @Test
    void keyWithUnexpectedSchemaIdInBodyRejected() {
        var key = asSchemaIdPrefixBuf(CONTENT_ID + 1, VALID_JSON);
        Record record = record(key, null, new Header[]{});
        BytebufValidator validator = BytebufValidators.jsonSchemaValidator(apicurioConfig, CONTENT_ID, WireFormatVersion.V3);
        var future = validator.validate(record.key(), record, true);
        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .isEqualTo(new Result(false, "Unexpected schema id in record (2), expecting 1"));
    }

    @Test
    void schemaNotFoundReturnsError() {
        // Configure validator with a global ID that doesn't exist in the mock registry
        long nonExistentContentId = 999L;
        registryServer.stubFor(
                get(urlEqualTo("/apis/registry/v3/ids/contentIds/999"))
                        .willReturn(WireMock.aResponse()
                                .withStatus(404)
                                .withHeader("Content-Type", "application/json")
                                .withBody("{\"message\":\"No artifact with ID '999' was found.\",\"error_code\":404}")));

        registryServer.stubFor(
                get(urlEqualTo("/apis/registry/v3/ids/contentIds/999?dereference=false"))
                        .willReturn(WireMock.aResponse()
                                .withStatus(404)
                                .withHeader("Content-Type", "application/json")
                                .withBody("{\"message\":\"No artifact with ID '999' was found.\",\"error_code\":404}")));

        Record record = record(RECORD_KEY, VALID_JSON);
        BytebufValidator validator = BytebufValidators.jsonSchemaValidator(apicurioConfig, nonExistentContentId, WireFormatVersion.V3);
        var future = validator.validate(record.value(), record, false);

        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(false, Result::valid);
    }

    @Test
    void bufferTooSmallForSchemaIdHandledGracefully() {
        // Create a buffer that's too small to contain a schema ID (less than 1 + 4 bytes)
        byte[] tinyBuffer = new byte[]{ BaseSerde.MAGIC_BYTE, 0x01 }; // Only 2 bytes
        Record record = record(RECORD_KEY, tinyBuffer);
        BytebufValidator validator = BytebufValidators.jsonSchemaValidator(apicurioConfig, CONTENT_ID, WireFormatVersion.V3);
        var future = validator.validate(record.value(), record, false);

        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(false, Result::valid);
    }

    @Test
    void bufferWithoutMagicByteHandledGracefully() {
        // Create a buffer without the magic byte prefix
        byte[] bufferWithoutMagic = new byte[20]; // Large enough but no magic byte
        bufferWithoutMagic[0] = 0x00; // Not the magic byte
        Record record = record(RECORD_KEY, bufferWithoutMagic);
        BytebufValidator validator = BytebufValidators.jsonSchemaValidator(apicurioConfig, CONTENT_ID, WireFormatVersion.V3);
        var future = validator.validate(record.value(), record, false);

        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(false, Result::valid);
    }

    @Test
    void v2WireFormatUsesLegacy8ByteIdHandler() {
        // V2 wire format uses 8-byte global IDs
        var value = asV2SchemaIdPrefixBuf(CONTENT_ID, VALID_JSON);
        Record record = record(RECORD_KEY, value);
        BytebufValidator validator = BytebufValidators.jsonSchemaValidator(apicurioConfig, CONTENT_ID, WireFormatVersion.V2);
        var future = validator.validate(record.value(), record, false);
        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(true, Result::valid);
    }

    @Test
    void v3WireFormatUsesDefault4ByteIdHandler() {
        // V3 wire format uses 4-byte content IDs (Confluent-compatible)
        var value = asV3SchemaIdPrefixBuf(CONTENT_ID, VALID_JSON);
        Record record = record(RECORD_KEY, value);
        BytebufValidator validator = BytebufValidators.jsonSchemaValidator(apicurioConfig, CONTENT_ID, WireFormatVersion.V3);
        var future = validator.validate(record.value(), record, false);
        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(true, Result::valid);
    }

    @Test
    void v2ValidatorRejectsV3WireFormat() {
        // V2 validator should reject V3 format (4-byte content ID)
        var value = asV3SchemaIdPrefixBuf(CONTENT_ID, VALID_JSON);
        Record record = record(RECORD_KEY, value);
        BytebufValidator validator = BytebufValidators.jsonSchemaValidator(apicurioConfig, CONTENT_ID, WireFormatVersion.V2);
        var future = validator.validate(record.value(), record, false);
        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(false, Result::valid);
    }

    @Test
    void v3ValidatorRejectsV2WireFormat() {
        // V3 validator should reject V2 format (8-byte global ID)
        var value = asV2SchemaIdPrefixBuf(CONTENT_ID, VALID_JSON);
        Record record = record(RECORD_KEY, value);
        BytebufValidator validator = BytebufValidators.jsonSchemaValidator(apicurioConfig, CONTENT_ID, WireFormatVersion.V3);
        var future = validator.validate(record.value(), record, false);
        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(false, Result::valid);
    }

    private byte[] toByteArray(long contentId) {
        // Headers still use 8 bytes (Long) even in v3, regardless of wire format
        var buf = ByteBuffer.allocate(Long.BYTES);
        buf.putLong(contentId);
        return buf.array();
    }

    private byte[] asSchemaIdPrefixBuf(long contentId, byte[] content) {
        // Default to V3 format (4-byte content ID) for backward compatibility with existing tests
        return asV3SchemaIdPrefixBuf(contentId, content);
    }

    private byte[] asV3SchemaIdPrefixBuf(long contentId, byte[] content) {
        // V3 wire format: 1 byte magic + 4 bytes content ID (Confluent-compatible)
        ByteBuffer buf = ByteBuffer.allocate(1 /* magic */ + Integer.BYTES /* content id */ + content.length);
        buf.put(BaseSerde.MAGIC_BYTE);
        buf.putInt((int) contentId);
        buf.put(content);
        return buf.array();
    }

    private byte[] asV2SchemaIdPrefixBuf(long contentId, byte[] content) {
        // V2 wire format: 1 byte magic + 8 bytes global ID
        ByteBuffer buf = ByteBuffer.allocate(1 /* magic */ + Long.BYTES /* global id */ + content.length);
        buf.put(BaseSerde.MAGIC_BYTE);
        buf.putLong(contentId);
        buf.put(content);
        return buf.array();
    }
}
