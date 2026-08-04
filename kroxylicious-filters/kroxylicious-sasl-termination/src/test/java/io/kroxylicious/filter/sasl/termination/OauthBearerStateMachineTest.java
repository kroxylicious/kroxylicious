/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerSaslServerProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OauthBearerStateMachineTest {

    private OauthBearerStateMachine handler;
    private OAuthBearerValidatorCallbackHandler callbackHandler;

    @BeforeAll
    static void registerProvider() {
        OAuthBearerSaslServerProvider.initialize();
    }

    @BeforeEach
    void setUp() {
        callbackHandler = new OAuthBearerValidatorCallbackHandler();
        handler = new OauthBearerStateMachine(callbackHandler, java.time.Clock.systemUTC());
    }

    @AfterEach
    void tearDown() {
        if (handler != null) {
            handler.dispose();
        }
    }

    @Test
    void shouldReturnCorrectMechanismName() {
        assertThat(handler.mechanismName()).isEqualTo(OAUTHBEARER_MECHANISM);
    }

    @Test
    void shouldDisposeIdempotently() {
        // When/Then
        handler.dispose();
        assertThatCode(() -> handler.dispose()).doesNotThrowAnyException();
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void shouldRejectNullCallbackHandler() {
        // Given
        var clock = java.time.Clock.systemUTC();

        // When/Then
        assertThatThrownBy(() -> new OauthBearerStateMachine(null, clock))
                .isInstanceOf(NullPointerException.class);
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void shouldRejectNullClock() {
        assertThatThrownBy(() -> new OauthBearerStateMachine(callbackHandler, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldReturnMaxAuthBytes() {
        assertThat(handler.maxAuthBytes()).isEqualTo(128 * 1024);
    }

    @Test
    void shouldDisposeAfterEvaluateRound() throws Exception {
        // Given
        byte[] invalidToken = "n,,auth=Bearer invalid-token".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        handler.evaluateRound(invalidToken).toCompletableFuture().get();

        // When/Then
        handler.dispose();
        assertThatCode(() -> handler.dispose()).doesNotThrowAnyException();
    }

    @Test
    void shouldFailForInvalidToken() throws Exception {
        // Given
        // The callback handler is not configured, so token validation will fail
        byte[] invalidToken = "n,,auth=Bearer invalid-token".getBytes(java.nio.charset.StandardCharsets.UTF_8);

        // When
        RoundResult result = handler.evaluateRound(invalidToken)
                .toCompletableFuture().get();

        // Then
        assertThat(result).isInstanceOf(RoundResult.Failure.class);
        assertThat(((RoundResult.Failure) result).exception()).isNotNull();
    }
}
