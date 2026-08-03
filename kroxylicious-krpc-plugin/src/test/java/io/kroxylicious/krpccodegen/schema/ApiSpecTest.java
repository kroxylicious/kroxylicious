/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.krpccodegen.schema;

import java.io.UncheckedIOException;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ApiSpecTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final MessageSpec FETCH_REQUEST = messageSpec("""
            {
              "apiKey": 1,
              "type": "request",
              "listeners": ["broker"],
              "name": "FetchRequest",
              "validVersions": "0-16",
              "flexibleVersions": "12+",
              "fields": []
            }
            """);

    private static final MessageSpec FETCH_RESPONSE = messageSpec("""
            {
              "apiKey": 1,
              "type": "response",
              "name": "FetchResponse",
              "validVersions": "0-16",
              "flexibleVersions": "12+",
              "fields": []
            }
            """);

    private static MessageSpec messageSpec(String content) {
        try {
            return MAPPER.readValue(content, MessageSpec.class);
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    void shouldDeriveFieldsFromRequestAndResponse() {
        // When
        var spec = new ApiSpec(FETCH_REQUEST, FETCH_RESPONSE);

        // Then
        assertThat(spec.name()).isEqualTo("Fetch");
        assertThat(spec.apiKey()).isEqualTo((short) 1);
        assertThat(spec.kafkaApiKeyEnumName()).isEqualTo("FETCH");
        assertThat(spec.listeners()).isEqualTo(Set.of(RequestListenerType.BROKER));
        assertThat(spec.request()).isSameAs(FETCH_REQUEST);
        assertThat(spec.response()).isSameAs(FETCH_RESPONSE);
    }

    @Test
    void shouldRejectRequestNameNotEndingWithRequest() {
        // Given
        var badRequest = messageSpec("""
                {
                  "apiKey": 1,
                  "type": "request",
                  "listeners": ["broker"],
                  "name": "Fetch",
                  "validVersions": "0",
                  "flexibleVersions": "0+",
                  "fields": []
                }
                """);

        // When/Then
        assertThatThrownBy(() -> new ApiSpec(badRequest, FETCH_RESPONSE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not end with 'Request'");
    }

    @Test
    void shouldRejectResponseNameNotEndingWithResponse() {
        // Given
        var badResponse = messageSpec("""
                {
                  "apiKey": 1,
                  "type": "response",
                  "name": "Fetch",
                  "validVersions": "0",
                  "flexibleVersions": "0+",
                  "fields": []
                }
                """);

        // When/Then
        assertThatThrownBy(() -> new ApiSpec(FETCH_REQUEST, badResponse))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not end with 'Response'");
    }

    @Test
    void shouldRejectMismatchedApiKeys() {
        // Given
        var otherResponse = messageSpec("""
                {
                  "apiKey": 2,
                  "type": "response",
                  "name": "FetchResponse",
                  "validVersions": "0",
                  "flexibleVersions": "0+",
                  "fields": []
                }
                """);

        // When/Then
        assertThatThrownBy(() -> new ApiSpec(FETCH_REQUEST, otherResponse))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not match response apiKey");
    }
}
