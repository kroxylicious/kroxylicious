/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.mds;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MdsTokenTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void readsJwtExpiryWithoutTreatingItAsSignatureValidation() {
        // Given
        byte[] response = response("{\"exp\":2000000000}");

        // When
        MdsToken token = MdsToken.parse(response, mapper);

        // Then
        assertThat(token.expiresAt()).isEqualTo(Instant.ofEpochSecond(2000000000));
        assertThat(token.toString()).doesNotContain(token.value()).contains("<redacted>");
    }

    @ParameterizedTest
    @ValueSource(strings = { "{}", "null", "{\"exp\":null}", "{\"exp\":\"2000000000\"}", "{\"exp\":-1}", "{\"exp\":1.5}",
            "{\"exp\":9223372036854775808}", "{\"exp\":9223372036854775807}" })
    void rejectsMissingOrUnusableExpiry(String claims) {
        // Given
        byte[] response = response(claims);

        // When
        // Then
        assertThatThrownBy(() -> MdsToken.parse(response, mapper)).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("MDS returned an invalid bearer token").hasNoCause();
    }

    @ParameterizedTest
    @ValueSource(strings = { "null", "{}", "{\"token_type\":\"Basic\",\"auth_token\":\"sensitive\"}",
            "{\"token_type\":\"Bearer\",\"auth_token\":\"sensitive\\u0001injection\"}", "{sensitive", "[]" })
    void rejectsInvalidResponsesWithoutLeakingThem(String response) {
        // Given
        byte[] body = response.getBytes(StandardCharsets.UTF_8);

        // When
        // Then
        assertThatThrownBy(() -> MdsToken.parse(body, mapper)).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("MDS returned an invalid bearer token").hasNoCause();
    }

    private static byte[] response(String claims) {
        String payload = Base64.getUrlEncoder().withoutPadding().encodeToString(claims.getBytes(StandardCharsets.UTF_8));
        return ("{\"token_type\":\"Bearer\",\"auth_token\":\"a." + payload + ".b\"}").getBytes(StandardCharsets.UTF_8);
    }
}
