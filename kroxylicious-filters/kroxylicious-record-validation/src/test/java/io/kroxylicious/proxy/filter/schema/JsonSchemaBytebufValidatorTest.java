/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.common.record.DefaultRecord;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;

import io.kroxylicious.proxy.filter.schema.validation.Result;
import io.kroxylicious.proxy.filter.schema.validation.bytebuf.BytebufValidator;
import io.kroxylicious.proxy.filter.schema.validation.bytebuf.BytebufValidators;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class JsonSchemaBytebufValidatorTest {

    static WireMockServer registryServer;

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
                get(urlEqualTo("/apis/registry/v2/ids/globalIds/1?dereference=false"))
                        .willReturn(WireMock.aResponse()
                                .withHeader("Content-Type", "application/json")
                                .withBody(JSON_SCHEMA)));

        registryServer.stubFor(
                get(urlEqualTo("/apis/registry/v2/ids/globalIds/1/references"))
                        .willReturn(WireMock.aResponse()
                                .withHeader("Content-Type", "application/json")
                                .withBody("[]")));
    }

    @AfterAll
    public static void shutdownMockRegistry() {
        registryServer.shutdown();
    }

    @Test
    void testValueValidated() {
        BytebufValidator mockValidator = mock(BytebufValidator.class);
        Record record = createRecord("a", "{\"firstName\":\"a\",\"lastName\":\"b\"}");
        BytebufValidator validator = BytebufValidators.jsonSchemaValidator(Map.of("apicurio.registry.url", registryServer.baseUrl()), 1L);
        Result result = validate(record, validator);
        assertTrue(result.valid());
        verifyNoMoreInteractions(mockValidator);
    }

    @Test
    void testValueInvalidAgeInvalidated() {
        BytebufValidator mockValidator = mock(BytebufValidator.class);
        Record record = createRecord("a", "{\"firstName\":\"a\",\"lastName\":\"b\",\"age\":-3}");
        BytebufValidator validator = BytebufValidators.jsonSchemaValidator(Map.of("apicurio.registry.url", registryServer.baseUrl()), 1L);
        Result result = validate(record, validator);
        assertFalse(result.valid());
        verifyNoMoreInteractions(mockValidator);
    }

    @Test
    void testInvalidValueInValidated() {
        BytebufValidator mockValidator = mock(BytebufValidator.class);
        Record record = createRecord("a", "123");
        BytebufValidator validator = BytebufValidators.jsonSchemaValidator(Map.of("apicurio.registry.url", registryServer.baseUrl()), 1L);
        Result result = validate(record, validator);
        assertFalse(result.valid());
        verifyNoMoreInteractions(mockValidator);
    }

    private static Result validate(Record record, BytebufValidator validator) {
        try {
            return validator.validate(record.value(), record.valueSize(), record, false).toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
        catch (ExecutionException | InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private Record createRecord(String key, String value) {
        ByteBuffer keyBuf = toBufNullable(key);
        ByteBuffer valueBuf = toBufNullable(value);

        try (ByteBufferOutputStream bufferOutputStream = new ByteBufferOutputStream(1000); DataOutputStream dataOutputStream = new DataOutputStream(bufferOutputStream)) {
            DefaultRecord.writeTo(dataOutputStream, 0, 0, keyBuf, valueBuf, Record.EMPTY_HEADERS);
            dataOutputStream.flush();
            bufferOutputStream.flush();
            ByteBuffer buffer = bufferOutputStream.buffer();
            buffer.flip();
            return DefaultRecord.readFrom(buffer, 0, 0, 0, 0L);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static ByteBuffer toBufNullable(String key) {
        if (key == null) {
            return null;
        }
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.wrap(keyBytes);
    }
}
