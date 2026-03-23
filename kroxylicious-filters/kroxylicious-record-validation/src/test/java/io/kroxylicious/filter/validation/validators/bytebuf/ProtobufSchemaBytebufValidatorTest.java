/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.validation.validators.bytebuf;

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
import io.apicurio.registry.serde.kafka.headers.KafkaSerdeHeaders;

import io.kroxylicious.filter.validation.config.SchemaValidationConfig.WireFormatVersion;
import io.kroxylicious.filter.validation.validators.Result;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static io.kroxylicious.test.record.RecordTestUtils.record;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtobufSchemaBytebufValidatorTest {

    private static final long CONTENT_ID = 1L;
    private static final byte[] RECORD_KEY = "a".getBytes(StandardCharsets.UTF_8);

    private static WireMockServer registryServer;
    private static Map<String, Object> apicurioConfig;

    private static final String PROTOBUF_SCHEMA = """
            syntax = "proto3";

            message Person {
              string first_name = 1;
              string last_name = 2;
              int32 age = 3;
            }
            """;

    // Valid protobuf encoding of Person with first_name="John", last_name="Doe", age=25
    private static final byte[] VALID_PROTOBUF = new byte[]{
            0x0A, 0x04, 'J', 'o', 'h', 'n',
            0x12, 0x03, 'D', 'o', 'e',
            0x18, 25
    };

    @BeforeAll
    static void initMockRegistry() {
        registryServer = new WireMockServer(
                wireMockConfig()
                        .dynamicPort());

        registryServer.start();

        registryServer.stubFor(
                get(urlEqualTo("/apis/registry/v3/ids/contentIds/1"))
                        .willReturn(WireMock.aResponse()
                                .withHeader("Content-Type", "application/x-protobuf")
                                .withBody(PROTOBUF_SCHEMA)));

        registryServer.stubFor(
                get(urlEqualTo("/apis/registry/v3/ids/contentIds/1/references"))
                        .willReturn(WireMock.aResponse()
                                .withHeader("Content-Type", "application/json")
                                .withBody("[]")));

        apicurioConfig = Map.of(
                "apicurio.registry.url", registryServer.baseUrl() + "/apis/registry/v3");
    }

    @AfterAll
    static void shutdownMockRegistry() {
        registryServer.shutdown();
    }

    @Test
    void valuePassesSchemaValidation() {
        Record record = record(RECORD_KEY, VALID_PROTOBUF);
        BytebufValidator validator = BytebufValidators.protobufSchemaValidator(apicurioConfig, CONTENT_ID, WireFormatVersion.V3);
        var future = validator.validate(record.value(), record, false);

        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(true, Result::valid);
    }

    @Test
    void emptyMessagePassesValidation() {
        // In proto3, all fields are optional, so an empty message is valid
        Record record = record(RECORD_KEY, new byte[0]);
        BytebufValidator validator = BytebufValidators.protobufSchemaValidator(apicurioConfig, CONTENT_ID, WireFormatVersion.V3);
        var future = validator.validate(record.value(), record, false);

        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(true, Result::valid);
    }

    @Test
    void corruptProtobufFailsParsing() {
        // Length-delimited field claims length 16 but only 1 byte follows
        byte[] corruptData = new byte[]{ 0x0A, 0x10, 0x01 };
        Record record = record(RECORD_KEY, corruptData);
        BytebufValidator validator = BytebufValidators.protobufSchemaValidator(apicurioConfig, CONTENT_ID, WireFormatVersion.V3);
        var future = validator.validate(record.value(), record, false);

        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(result -> {
                    assertThat(result.valid()).isFalse();
                    assertThat(result.errorMessage()).contains("Failed to parse Protobuf message");
                });
    }

    @Test
    void valueWithCorrectSchemaIdInHeaderPassesValidation() {
        Header[] headers = new Header[]{ new RecordHeader(KafkaSerdeHeaders.HEADER_VALUE_CONTENT_ID, toByteArray(CONTENT_ID)) };
        // When schema ID is in headers, the Apicurio ProtobufSerde writes a Ref delimited message
        // before the protobuf payload in the body.
        Record record = record(RECORD_KEY, withRefPrefix("Person", VALID_PROTOBUF), headers);
        BytebufValidator validator = BytebufValidators.protobufSchemaValidator(apicurioConfig, CONTENT_ID, WireFormatVersion.V3);
        var future = validator.validate(record.value(), record, false);

        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(true, Result::valid);
    }

    @Test
    void valueWithWrongSchemaIdInHeaderRejected() {
        Header[] headers = new Header[]{ new RecordHeader(KafkaSerdeHeaders.HEADER_VALUE_CONTENT_ID, toByteArray(CONTENT_ID + 1)) };
        Record record = record(RECORD_KEY, VALID_PROTOBUF, headers);
        BytebufValidator validator = BytebufValidators.protobufSchemaValidator(apicurioConfig, CONTENT_ID, WireFormatVersion.V3);
        var future = validator.validate(record.value(), record, false);

        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .isEqualTo(new Result(false, "Unexpected schema id in record (2), expecting 1"));
    }

    @Test
    void valueWithCorrectSchemaIdInBodyPassesValidation() {
        // Apicurio protobuf body wire format: magic + contentId + Ref + protobuf
        var value = asV3SchemaIdPrefixBuf(CONTENT_ID, withRefPrefix("Person", VALID_PROTOBUF));
        Record record = record(RECORD_KEY, value);
        BytebufValidator validator = BytebufValidators.protobufSchemaValidator(apicurioConfig, CONTENT_ID, WireFormatVersion.V3);
        var future = validator.validate(record.value(), record, false);

        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(true, Result::valid);
    }

    @Test
    void valueWithUnexpectedSchemaIdInBodyRejected() {
        var value = asV3SchemaIdPrefixBuf(CONTENT_ID + 1, withRefPrefix("Person", VALID_PROTOBUF));
        Record record = record(RECORD_KEY, value);
        BytebufValidator validator = BytebufValidators.protobufSchemaValidator(apicurioConfig, CONTENT_ID, WireFormatVersion.V3);
        var future = validator.validate(record.value(), record, false);

        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .isEqualTo(new Result(false, "Unexpected schema id in record (2), expecting 1"));
    }

    @Test
    void schemaNotFoundThrows() {
        long nonExistentContentId = 999L;
        registryServer.stubFor(
                get(urlEqualTo("/apis/registry/v3/ids/contentIds/999"))
                        .willReturn(WireMock.aResponse()
                                .withStatus(404)
                                .withHeader("Content-Type", "application/json")
                                .withBody("{\"message\":\"No artifact with ID '999' was found.\",\"error_code\":404}")));

        registryServer.stubFor(
                get(urlEqualTo("/apis/registry/v3/ids/contentIds/999/references"))
                        .willReturn(WireMock.aResponse()
                                .withHeader("Content-Type", "application/json")
                                .withBody("[]")));

        registryServer.stubFor(
                get(urlEqualTo("/apis/registry/v3/ids/contentIds/999?dereference=false"))
                        .willReturn(WireMock.aResponse()
                                .withStatus(404)
                                .withHeader("Content-Type", "application/json")
                                .withBody("{\"message\":\"No artifact with ID '999' was found.\",\"error_code\":404}")));

        assertThatThrownBy(() -> BytebufValidators.protobufSchemaValidator(apicurioConfig, nonExistentContentId, WireFormatVersion.V3))
                .isInstanceOf(RuntimeException.class);
    }

    private byte[] toByteArray(long contentId) {
        var buf = ByteBuffer.allocate(Long.BYTES);
        buf.putLong(contentId);
        return buf.array();
    }

    private byte[] asV3SchemaIdPrefixBuf(long contentId, byte[] content) {
        ByteBuffer buf = ByteBuffer.allocate(1 + Integer.BYTES + content.length);
        buf.put(BaseSerde.MAGIC_BYTE);
        buf.putInt((int) contentId);
        buf.put(content);
        return buf.array();
    }

    private byte[] withRefPrefix(String messageTypeName, byte[] protobufData) {
        // Apicurio ProtobufSerde writes a delimited Ref message (containing the message type name)
        // before protobuf data. The Ref is encoded as: tag(0x0A) + length + name_bytes
        byte[] nameBytes = messageTypeName.getBytes(StandardCharsets.UTF_8);
        byte[] refMessage = new byte[2 + nameBytes.length];
        refMessage[0] = 0x0A; // field 1, wire type 2 (length-delimited)
        refMessage[1] = (byte) nameBytes.length;
        System.arraycopy(nameBytes, 0, refMessage, 2, nameBytes.length);
        // Delimited format: varint size + ref message bytes
        ByteBuffer buf = ByteBuffer.allocate(1 + refMessage.length + protobufData.length);
        buf.put((byte) refMessage.length);
        buf.put(refMessage);
        buf.put(protobufData);
        return buf.array();
    }
}
