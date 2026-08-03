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
    void shouldDetectEntityFieldInRequest() {
        // Given
        var request = messageSpec("""
                {
                  "apiKey": 1,
                  "type": "request",
                  "listeners": ["broker"],
                  "name": "FetchRequest",
                  "validVersions": "0",
                  "flexibleVersions": "0+",
                  "fields": [
                    { "name": "TopicName", "type": "string", "versions": "0+", "entityType": "topicName",
                      "about": "The topic." }
                  ]
                }
                """);
        var spec = new ApiSpec(request, FETCH_RESPONSE);

        // When
        var result = spec.hasAtLeastOneEntityField(Set.of(EntityType.TOPIC_NAME));

        // Then
        assertThat(result).isTrue();
    }

    @Test
    void shouldDetectEntityFieldInResponse() {
        // Given
        var response = messageSpec("""
                {
                  "apiKey": 1,
                  "type": "response",
                  "name": "FetchResponse",
                  "validVersions": "0",
                  "flexibleVersions": "0+",
                  "fields": [
                    { "name": "TopicName", "type": "string", "versions": "0+", "entityType": "topicName",
                      "about": "The topic." }
                  ]
                }
                """);
        var spec = new ApiSpec(FETCH_REQUEST, response);

        // When
        var result = spec.hasAtLeastOneEntityField(Set.of(EntityType.TOPIC_NAME));

        // Then
        assertThat(result).isTrue();
    }

    @Test
    void shouldDetectAbsenceOfEntityField() {
        // Given
        var spec = new ApiSpec(FETCH_REQUEST, FETCH_RESPONSE);

        // When
        var result = spec.hasAtLeastOneEntityField(Set.of(EntityType.TOPIC_NAME));

        // Then
        assertThat(result).isFalse();
    }

    @Test
    void shouldDetectResourceListInRequest() {
        // Given
        var request = messageSpec("""
                {
                  "apiKey": 1,
                  "type": "request",
                  "listeners": ["broker"],
                  "name": "FetchRequest",
                  "validVersions": "0",
                  "flexibleVersions": "0+",
                  "fields": [
                    { "name": "Resources", "type": "[]Resource", "versions": "0+",
                      "about": "The resources.", "fields": [
                      { "name": "ResourceType", "type": "int8", "versions": "0+",
                        "about": "The resource type." },
                      { "name": "ResourceName", "type": "string", "versions": "0+",
                        "about": "The resource name." }
                    ]}
                  ]
                }
                """);
        var spec = new ApiSpec(request, FETCH_RESPONSE);

        // When
        var result = spec.hasResourceList();

        // Then
        assertThat(result).isTrue();
    }

    @Test
    void shouldDetectResourceListInResponse() {
        // Given
        var response = messageSpec("""
                {
                  "apiKey": 1,
                  "type": "response",
                  "name": "FetchResponse",
                  "validVersions": "0",
                  "flexibleVersions": "0+",
                  "fields": [
                    { "name": "Resources", "type": "[]Resource", "versions": "0+",
                      "about": "The resources.", "fields": [
                      { "name": "ResourceType", "type": "int8", "versions": "0+",
                        "about": "The resource type." },
                      { "name": "ResourceName", "type": "string", "versions": "0+",
                        "about": "The resource name." }
                    ]}
                  ]
                }
                """);
        var spec = new ApiSpec(FETCH_REQUEST, response);

        // When
        var result = spec.hasResourceList();

        // Then
        assertThat(result).isTrue();
    }

    @Test
    void shouldDetectAbsenceOfResourceList() {
        // Given
        var spec = new ApiSpec(FETCH_REQUEST, FETCH_RESPONSE);

        // When
        var result = spec.hasResourceList();

        // Then
        assertThat(result).isFalse();
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
