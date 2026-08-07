/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;

import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallback;
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

    private static final byte[] CONTROL_A = new byte[]{ 0x01 };

    private OauthBearerStateMachine handler;
    private OAuthBearerValidatorCallbackHandler callbackHandler;

    @BeforeAll
    static void registerProvider() {
        OAuthBearerSaslServerProvider.initialize();
    }

    @BeforeEach
    void setUp() {
        callbackHandler = new OAuthBearerValidatorCallbackHandler();
        handler = new OauthBearerStateMachine(callbackHandler, OauthBearerMechanismConfig.DEFAULT_MAX_AUTH_BYTES);
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
        // When/Then
        assertThatThrownBy(() -> new OauthBearerStateMachine(null, OauthBearerMechanismConfig.DEFAULT_MAX_AUTH_BYTES))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldReturnMaxAuthBytes() {
        assertThat(handler.maxAuthBytes()).isEqualTo(128 * 1024);
    }

    @Test
    void shouldDisposeAfterEvaluateRound() throws Exception {
        // Given
        byte[] invalidToken = "n,,auth=Bearer invalid-token".getBytes(StandardCharsets.UTF_8);
        handler.evaluateRound(invalidToken).toCompletableFuture().get();

        // When/Then
        handler.dispose();
        assertThatCode(() -> handler.dispose()).doesNotThrowAnyException();
    }

    @Test
    void shouldFailForInvalidToken() throws Exception {
        // Given
        byte[] invalidToken = "n,,auth=Bearer invalid-token".getBytes(StandardCharsets.UTF_8);

        // When
        RoundResult result = handler.evaluateRound(invalidToken)
                .toCompletableFuture().get();

        // Then
        assertThat(result).isInstanceOf(RoundResult.Failure.class);
        assertThat(((RoundResult.Failure) result).exception()).isNotNull();
    }

    // RFC 7628 §3.2.2-3.2.3: multi-round error sequence

    @Test
    void shouldReturnChallengeWithJsonErrorWhenTokenIsRejected() throws Exception {
        // Given
        handler = new OauthBearerStateMachine(
                rejectingCallbackHandler("invalid_token", null, null), OauthBearerMechanismConfig.DEFAULT_MAX_AUTH_BYTES);

        // When
        RoundResult result = handler.evaluateRound(clientInitialResponse())
                .toCompletableFuture().get();

        // Then
        assertThat(result).isInstanceOf(RoundResult.Challenge.class);
        var challenge = (RoundResult.Challenge) result;
        assertThat(new String(challenge.responseBytes(), StandardCharsets.UTF_8))
                .contains("\"status\":\"invalid_token\"");
    }

    @Test
    void shouldIncludeScopeAndOpenIdConfigInErrorChallenge() throws Exception {
        // Given
        handler = new OauthBearerStateMachine(
                rejectingCallbackHandler("insufficient_scope", "email profile",
                        "https://example.com/.well-known/openid-configuration"),
                OauthBearerMechanismConfig.DEFAULT_MAX_AUTH_BYTES);

        // When
        RoundResult result = handler.evaluateRound(clientInitialResponse())
                .toCompletableFuture().get();

        // Then
        assertThat(result).isInstanceOf(RoundResult.Challenge.class);
        String errorJson = new String(((RoundResult.Challenge) result).responseBytes(), StandardCharsets.UTF_8);
        assertThat(errorJson)
                .contains("\"status\":\"insufficient_scope\"")
                .contains("\"scope\":\"email profile\"")
                .contains("\"openid-configuration\":\"https://example.com/.well-known/openid-configuration\"");
    }

    @Test
    void shouldFailAfterClientAcknowledgesErrorWithControlA() throws Exception {
        // Given
        handler = new OauthBearerStateMachine(
                rejectingCallbackHandler("invalid_token", null, null), OauthBearerMechanismConfig.DEFAULT_MAX_AUTH_BYTES);
        handler.evaluateRound(clientInitialResponse()).toCompletableFuture().get();

        // When
        RoundResult result = handler.evaluateRound(CONTROL_A)
                .toCompletableFuture().get();

        // Then
        assertThat(result).isInstanceOf(RoundResult.Failure.class);
        assertThat(((RoundResult.Failure) result).exception()).isNotNull();
    }

    private static byte[] clientInitialResponse() {
        return "n,,auth=Bearer sometoken".getBytes(StandardCharsets.UTF_8);
    }

    private static OAuthBearerValidatorCallbackHandler rejectingCallbackHandler(
                                                                                String errorStatus, String errorScope, String errorOpenIDConfiguration) {
        return new OAuthBearerValidatorCallbackHandler() {
            @Override
            public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
                for (Callback cb : callbacks) {
                    if (cb instanceof OAuthBearerValidatorCallback vcb) {
                        vcb.error(errorStatus, errorScope, errorOpenIDConfiguration);
                    }
                    else {
                        throw new UnsupportedCallbackException(cb);
                    }
                }
            }
        };
    }
}
