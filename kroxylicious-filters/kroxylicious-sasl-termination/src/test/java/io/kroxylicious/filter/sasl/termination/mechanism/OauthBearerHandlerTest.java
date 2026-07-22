/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination.mechanism;

import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerSaslServerProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
import static org.assertj.core.api.Assertions.assertThat;

class OauthBearerHandlerTest {

    private OauthBearerHandler handler;
    private OAuthBearerValidatorCallbackHandler callbackHandler;

    @BeforeAll
    static void registerProvider() {
        OAuthBearerSaslServerProvider.initialize();
    }

    @BeforeEach
    void setUp() {
        callbackHandler = new OAuthBearerValidatorCallbackHandler();
        handler = new OauthBearerHandler(callbackHandler, java.time.Clock.systemUTC());
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
        handler.dispose();
    }

    @Test
    void shouldFailForInvalidToken() throws Exception {
        // Given
        // The callback handler is not configured, so token validation will fail
        byte[] invalidToken = "n,,auth=Bearer invalid-token".getBytes(java.nio.charset.StandardCharsets.UTF_8);

        // When
        AuthenticationResult result = handler.handleAuthenticate(invalidToken)
                .toCompletableFuture().get();

        // Then
        assertThat(result.outcome()).isEqualTo(AuthenticationResult.Outcome.FAILURE);
    }
}
