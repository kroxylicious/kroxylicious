/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.auth;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class AccessTokenResponseTest {

    ObjectMapper mapper = new ObjectMapper();

    @Test
    void deserialize() throws JsonProcessingException {
        String jsonForm = """
                {"token_type": "Bearer", "expires_in": 3600, "access_token": "abc"}
                """;
        AccessTokenResponse response = mapper.readValue(jsonForm, AccessTokenResponse.class);
        assertThat(response).isNotNull();
        assertThat(response.accessToken()).isEqualTo("abc");
        assertThat(response.tokenType()).isEqualTo("Bearer");
        assertThat(response.expiresIn()).isEqualTo(3600);
    }

    public static Stream<Arguments> invalidJson() {
        String noTokenType = """
                {"expires_in": 3600, "access_token": "abc"}
                """;
        String noExpiresIn = """
                {"token_type": "Bearer", "access_token": "abc"}
                """;
        String noAccessToken = """
                {"token_type": "Bearer", "expires_in": 3600}
                """;
        String negativeExpiresIn = """
                {"token_type": "Bearer", "expires_in": -1, "access_token": "abc"}
                """;
        String zeroExpiresIn = """
                {"token_type": "Bearer", "expires_in": 0, "access_token": "abc"}
                """;
        return Stream.of(argumentSet("no token type", noTokenType, MismatchedInputException.class, "Missing required creator property 'token_type'"),
                argumentSet("no expires in", noExpiresIn, MismatchedInputException.class, "Missing required creator property 'expires_in'"),
                argumentSet("no access token", noAccessToken, MismatchedInputException.class, "Missing required creator property 'access_token'"),
                argumentSet("negative expires in", negativeExpiresIn, ValueInstantiationException.class, "expires_in must be greater than 0"),
                argumentSet("zero expires in", zeroExpiresIn, ValueInstantiationException.class, "expires_in must be greater than 0"));
    }

    @MethodSource
    @ParameterizedTest
    void invalidJson(String json, Class<? extends Exception> expectedType, String expectedMessage) {
        assertThatThrownBy(() -> mapper.readValue(json, AccessTokenResponse.class)).isInstanceOf(expectedType).hasMessageContaining(expectedMessage);
    }

    @Test
    void serialize() throws JsonProcessingException {
        String jsonForm = """
                {"token_type":"bodge","expires_in":12,"access_token":"tokentoken"}""";
        AccessTokenResponse response = new AccessTokenResponse("bodge", 12, "tokentoken");
        String serialized = mapper.writeValueAsString(response);
        assertThat(serialized).isEqualTo(jsonForm);
    }

    @Test
    void toStringTokenRedacted() {
        AccessTokenResponse response = new AccessTokenResponse("bodge", 12, "tokentoken");
        assertThat(response.toString()).doesNotContain("tokentoken");
    }
}