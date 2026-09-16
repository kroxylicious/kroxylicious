/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.mds;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MdsSubjectTest extends MdsReauthenticationTestSupport {
    @TempDir
    Path directory;

    @ParameterizedTest
    @ValueSource(strings = { "", ",\"sub\":null", ",\"sub\":7", ",\"sub\":true", ",\"sub\":[]",
            ",\"sub\":{}", ",\"sub\":\"\"", ",\"sub\":\"proxy\"", ",\"sub\":\"Alice\"", ",\"sub\":\"alice \"" })
    void requiresExactTextualSubject(String field) {
        // Given
        String payload = Base64.getUrlEncoder().withoutPadding().encodeToString(
                ("{\"exp\":2000000000" + field + "}").getBytes(StandardCharsets.UTF_8));
        byte[] body = ("{\"token_type\":\"Bearer\",\"auth_token\":\"a." + payload + ".b\"}").getBytes(StandardCharsets.UTF_8);

        // When
        // Then
        assertThatThrownBy(() -> MdsToken.parse(body, new ObjectMapper(), "alice"))
                .isInstanceOf(MdsFailure.class).hasMessageContaining("TOKEN_SUBJECT").hasNoCause();
    }

    @Test
    void rejectsWrongSubjectBeforeAnyInitialSaslExchange() throws Exception {
        // Given
        try (var mds = new TestMdsServer(directory); var client = mds.client(mds.config(mds.clientTls))) {
            mds.response = TestMdsServer.tokenResponse("proxy");
            tokens.add(client.impersonate("alice"));

            // When
            var result = request().get(10, TimeUnit.SECONDS);

            // Then
            assertThat(result.closeConnection()).isTrue();
            assertThat(sent).isEmpty();
        }
    }

    @Test
    void rejectsWrongSubjectDuringRenewalWithoutUsingOldSession() throws Exception {
        // Given
        token("old", 30);
        success(30000);
        request().join();
        time(25);
        int sentBeforeRenewal = sent.size();
        try (var mds = new TestMdsServer(directory); var client = mds.client(mds.config(mds.clientTls))) {
            mds.response = TestMdsServer.tokenResponse("proxy");
            tokens.add(client.impersonate("alice"));

            // When
            var result = request().get(10, TimeUnit.SECONDS);

            // Then
            assertThat(result.closeConnection()).isTrue();
            assertThat(result.message()).isNull();
            assertThat(sent).hasSize(sentBeforeRenewal);
        }
    }
}
