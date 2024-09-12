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

import io.apicurio.registry.serde.AbstractKafkaSerDe;
import io.apicurio.registry.serde.SerdeHeaders;

import io.kroxylicious.proxy.filter.validation.validators.Result;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static io.kroxylicious.test.record.RecordTestUtils.record;
import static org.assertj.core.api.Assertions.assertThat;

public class JsonSchemaBytebufValidatorTest {

    private static final long GLOBAL_ID = 1L;
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
                                .dynamicPort()
        );

        registryServer.start();

        registryServer.stubFor(
                get(urlEqualTo("/apis/registry/v2/ids/globalIds/1?dereference=false"))
                                                                                      .willReturn(
                                                                                              WireMock.aResponse()
                                                                                                      .withHeader("Content-Type", "application/json")
                                                                                                      .withBody(JSON_SCHEMA)
                                                                                      )
        );

        registryServer.stubFor(
                get(urlEqualTo("/apis/registry/v2/ids/globalIds/1/references"))
                                                                               .willReturn(
                                                                                       WireMock.aResponse()
                                                                                               .withHeader("Content-Type", "application/json")
                                                                                               .withBody("[]")
                                                                               )
        );

        apicurioConfig = Map.of("apicurio.registry.url", registryServer.baseUrl());
    }

    @AfterAll
    public static void shutdownMockRegistry() {
        registryServer.shutdown();
    }

    @Test
    void valuePassesSchemaValidation() {
        Record record = record(RECORD_KEY, VALID_JSON);
        BytebufValidator validator = BytebufValidators.jsonSchemaValidator(apicurioConfig, GLOBAL_ID);
        var future = validator.validate(record.value(), record, false);

        assertThat(future)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .returns(true, Result::valid);
    }

    @Test
    void jsonValueFailsSchemaValidation() {
        Record record = record(RECORD_KEY, INVALID_JSON);
        BytebufValidator validator = BytebufValidators.jsonSchemaValidator(apicurioConfig, GLOBAL_ID);
        var future = validator.validate(record.value(), record, false);

        assertThat(future)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .returns(false, Result::valid);
    }

    @Test
    void nonJsonValueFailsToParse() {
        Record record = record(RECORD_KEY, "not a json value".getBytes(StandardCharsets.UTF_8));
        BytebufValidator validator = BytebufValidators.jsonSchemaValidator(apicurioConfig, GLOBAL_ID);
        var future = validator.validate(record.value(), record, false);
        assertThat(future)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .returns(false, Result::valid);
    }

    @Test
    void valueWithCorrectSchemaIdInHeaderPassesValidation() {
        Header[] headers = new Header[]{ new RecordHeader(SerdeHeaders.HEADER_VALUE_GLOBAL_ID, toByteArray(GLOBAL_ID)) };
        Record record = record(RECORD_KEY, VALID_JSON, headers);
        BytebufValidator validator = BytebufValidators.jsonSchemaValidator(apicurioConfig, GLOBAL_ID);
        var future = validator.validate(record.value(), record, false);
        assertThat(future)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .returns(true, Result::valid);
    }

    @Test
    void valueWithCorrectSchemaIdInBodyPassesValidation() {
        var value = asSchemaIdPrefixBuf(GLOBAL_ID, VALID_JSON);
        Record record = record(RECORD_KEY, value);
        BytebufValidator validator = BytebufValidators.jsonSchemaValidator(apicurioConfig, GLOBAL_ID);
        var future = validator.validate(record.value(), record, false);
        assertThat(future)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .returns(true, Result::valid);
    }

    @Test
    void valueWithWrongSchemaIdInHeaderRejected() {
        Header[] headers = new Header[]{ new RecordHeader(SerdeHeaders.HEADER_VALUE_GLOBAL_ID, toByteArray(GLOBAL_ID + 1)) };
        Record record = record(RECORD_KEY, VALID_JSON, headers);
        BytebufValidator validator = BytebufValidators.jsonSchemaValidator(apicurioConfig, GLOBAL_ID);
        var future = validator.validate(record.value(), record, false);
        assertThat(future)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .isEqualTo(new Result(false, "Unexpected schema id in record (2), expecting 1"));
    }

    @Test
    void valueWithUnexpectedSchemaIdInBodyRejected() {
        var value = asSchemaIdPrefixBuf(GLOBAL_ID + 1, VALID_JSON);
        Record record = record(RECORD_KEY, value);
        BytebufValidator validator = BytebufValidators.jsonSchemaValidator(apicurioConfig, GLOBAL_ID);
        var future = validator.validate(record.value(), record, false);
        assertThat(future)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .isEqualTo(new Result(false, "Unexpected schema id in record (2), expecting 1"));
    }

    @Test
    void keyWithCorrectSchemaIdInHeaderPassesValidation() {
        Header[] headers = new Header[]{ new RecordHeader(SerdeHeaders.HEADER_VALUE_GLOBAL_ID, toByteArray(GLOBAL_ID)) };
        Record record = record(VALID_JSON, null, headers);
        BytebufValidator validator = BytebufValidators.jsonSchemaValidator(apicurioConfig, GLOBAL_ID);
        var future = validator.validate(record.key(), record, true);
        assertThat(future)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .returns(true, Result::valid);
    }

    @Test
    void keyWithUnexpectedSchemaIdInBodyRejected() {
        var key = asSchemaIdPrefixBuf(GLOBAL_ID + 1, VALID_JSON);
        Record record = record(key, null, new Header[]{});
        BytebufValidator validator = BytebufValidators.jsonSchemaValidator(apicurioConfig, GLOBAL_ID);
        var future = validator.validate(record.key(), record, true);
        assertThat(future)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .isEqualTo(new Result(false, "Unexpected schema id in record (2), expecting 1"));
    }

    private byte[] toByteArray(long globalId) {
        var buf = ByteBuffer.allocate(Long.BYTES);
        buf.putLong(globalId);
        return buf.array();
    }

    private byte[] asSchemaIdPrefixBuf(long globalId, byte[] content) {
        ByteBuffer buf = ByteBuffer.allocate(1 /* magic */ + Long.BYTES /* global id */ + content.length);
        buf.put(AbstractKafkaSerDe.MAGIC_BYTE);
        buf.putLong(globalId);
        buf.put(content);

        return buf.array();
    }
}
