/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.validation.validators.bytebuf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
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
import static io.kroxylicious.testing.filter.record.RecordTestUtils.record;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AvroSchemaBytebufValidatorTest {

    private static final long CONTENT_ID = 1L;
    private static final byte[] RECORD_KEY = "a".getBytes(StandardCharsets.UTF_8);

    private static WireMockServer registryServer;
    private static Map<String, Object> apicurioConfig;
    private static Schema avroSchema;
    private static byte[] validAvroBytes;

    private static final String AVRO_SCHEMA_JSON = """
            {
              "type": "record",
              "name": "Person",
              "namespace": "io.kroxylicious.test",
              "fields": [
                {"name": "firstName", "type": "string"},
                {"name": "lastName", "type": "string"},
                {"name": "age", "type": "int"}
              ]
            }
            """;

    @BeforeAll
    static void initMockRegistry() throws IOException {
        avroSchema = new Schema.Parser().parse(AVRO_SCHEMA_JSON);

        GenericRecord person = new GenericData.Record(avroSchema);
        person.put("firstName", "John");
        person.put("lastName", "Doe");
        person.put("age", 25);
        validAvroBytes = serializeAvro(avroSchema, person);

        registryServer = new WireMockServer(
                wireMockConfig()
                        .dynamicPort());

        registryServer.start();

        registryServer.stubFor(
                get(urlEqualTo("/apis/registry/v3/ids/contentIds/1"))
                        .willReturn(WireMock.aResponse()
                                .withHeader("Content-Type", "application/json")
                                .withBody(AVRO_SCHEMA_JSON)));

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
        Record record = record(RECORD_KEY, validAvroBytes);
        BytebufValidator validator = BytebufValidators.avroSchemaValidator(apicurioConfig, CONTENT_ID, WireFormatVersion.V3);
        var future = validator.validate(record.value(), record, false);

        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(true, Result::valid);
    }

    @Test
    void nonAvroValueFailsDeserialization() {
        Record record = record(RECORD_KEY, "not avro data".getBytes(StandardCharsets.UTF_8));
        BytebufValidator validator = BytebufValidators.avroSchemaValidator(apicurioConfig, CONTENT_ID, WireFormatVersion.V3);
        var future = validator.validate(record.value(), record, false);

        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(result -> {
                    assertThat(result.valid()).isFalse();
                    assertThat(result.errorMessage()).contains("Failed to deserialize Avro record");
                });
    }

    @Test
    void valueWithCorrectSchemaIdInHeaderPassesValidation() {
        Header[] headers = new Header[]{ new RecordHeader(KafkaSerdeHeaders.HEADER_VALUE_CONTENT_ID, toByteArray(CONTENT_ID)) };
        Record record = record(RECORD_KEY, validAvroBytes, headers);
        BytebufValidator validator = BytebufValidators.avroSchemaValidator(apicurioConfig, CONTENT_ID, WireFormatVersion.V3);
        var future = validator.validate(record.value(), record, false);

        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(true, Result::valid);
    }

    @Test
    void valueWithWrongSchemaIdInHeaderRejected() {
        Header[] headers = new Header[]{ new RecordHeader(KafkaSerdeHeaders.HEADER_VALUE_CONTENT_ID, toByteArray(CONTENT_ID + 1)) };
        Record record = record(RECORD_KEY, validAvroBytes, headers);
        BytebufValidator validator = BytebufValidators.avroSchemaValidator(apicurioConfig, CONTENT_ID, WireFormatVersion.V3);
        var future = validator.validate(record.value(), record, false);

        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .isEqualTo(new Result(false, "Unexpected schema id in record (2), expecting 1"));
    }

    @Test
    void valueWithCorrectSchemaIdInBodyPassesValidation() {
        var value = asV3SchemaIdPrefixBuf(CONTENT_ID, validAvroBytes);
        Record record = record(RECORD_KEY, value);
        BytebufValidator validator = BytebufValidators.avroSchemaValidator(apicurioConfig, CONTENT_ID, WireFormatVersion.V3);
        var future = validator.validate(record.value(), record, false);

        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(true, Result::valid);
    }

    @Test
    void valueWithUnexpectedSchemaIdInBodyRejected() {
        var value = asV3SchemaIdPrefixBuf(CONTENT_ID + 1, validAvroBytes);
        Record record = record(RECORD_KEY, value);
        BytebufValidator validator = BytebufValidators.avroSchemaValidator(apicurioConfig, CONTENT_ID, WireFormatVersion.V3);
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

        assertThatThrownBy(() -> BytebufValidators.avroSchemaValidator(apicurioConfig, nonExistentContentId, WireFormatVersion.V3))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void bufferTooSmallForSchemaIdHandledGracefully() {
        byte[] tinyBuffer = new byte[]{ BaseSerde.MAGIC_BYTE, 0x01 };
        Record record = record(RECORD_KEY, tinyBuffer);
        BytebufValidator validator = BytebufValidators.avroSchemaValidator(apicurioConfig, CONTENT_ID, WireFormatVersion.V3);
        var future = validator.validate(record.value(), record, false);

        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(false, Result::valid);
    }

    private static byte[] serializeAvro(Schema schema, GenericRecord record) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        var encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(record, encoder);
        encoder.flush();
        return out.toByteArray();
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
}
