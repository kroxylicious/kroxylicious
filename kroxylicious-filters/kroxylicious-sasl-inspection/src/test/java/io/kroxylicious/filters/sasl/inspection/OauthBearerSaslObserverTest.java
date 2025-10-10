/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters.sasl.inspection;

import java.util.stream.Stream;

import javax.security.sasl.SaslException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OauthBearerSaslObserverTest {

    @Test
    void initialState() {
        // Given/When
        var observer = new OauthBearerSaslObserver();

        // Then
        assertThat(observer.isFinished()).isFalse();
        assertThatThrownBy(observer::authorizationId).isInstanceOf(SaslException.class);
    }


    static Stream<Arguments> goodInitialResponses() {
        return Stream.of(
                Arguments.argumentSet("response with gs2authzid", TestData.SASL_OAUTHBEARER_CLIENT_INITIAL, "user@example.com"),
                Arguments.argumentSet("both authcid and authzid", TestData.SASL_PLAIN_CLIENT_INITIAL_WITH_AUTHZID, "Ursel"));
    }

    @ParameterizedTest
    @MethodSource("goodInitialResponses")
    void shouldYieldAuthorizationIdFollowingSuccessfulNegotiation(byte[] initialResponse, String expectedAuthorizationId) throws SaslException {
        // Given
        var observer = new OauthBearerSaslObserver();

        // When
        observer.clientResponse(initialResponse);
        observer.serverChallenge(new byte[0]);

        // Then
        assertThat(observer.isFinished()).isTrue();
        assertThat(observer.authorizationId()).isEqualTo(expectedAuthorizationId);
    }

}