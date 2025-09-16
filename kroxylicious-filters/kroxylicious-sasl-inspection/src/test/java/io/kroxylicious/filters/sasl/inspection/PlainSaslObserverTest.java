/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters.sasl.inspection;

import java.util.stream.Stream;

import org.apache.kafka.common.errors.AuthenticationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PlainSaslObserverTest {

    @Test
    void initialState() {
        // Given/When
        var observer = new PlainSaslObserver();

        // Then
        assertThat(observer.isFinished()).isFalse();
        assertThatThrownBy(observer::authorizationId).isInstanceOf(AuthenticationException.class);
    }

    static Stream<Arguments> goodInitialResponses() {
        return Stream.of(
                Arguments.argumentSet("authcid only", TestData.SASL_PLAIN_CLIENT_INITIAL, "tim"),
                Arguments.argumentSet("both authcid and authzid", TestData.SASL_PLAIN_CLIENT_INITIAL_WITH_AUTHZID, "Ursel"));
    }

    @ParameterizedTest
    @MethodSource("goodInitialResponses")
    void shouldYieldAuthorizationIdFollowingSuccessfulNegotiation(byte[] initialResponse, String expectedAuthorizationId) {
        // Given
        var observer = new PlainSaslObserver();

        // When
        observer.clientResponse(initialResponse);
        observer.serverChallenge(new byte[0]);

        // Then
        assertThat(observer.isFinished()).isTrue();
        assertThat(observer.authorizationId()).isEqualTo(expectedAuthorizationId);
    }

    static Stream<Arguments> badClientResponse() {
        return Stream.of(
                Arguments.argumentSet("too many tokens", (Object) "far\0too\0many\0tokens".getBytes(UTF_8)),
                Arguments.argumentSet("empty response", (Object) "".getBytes(UTF_8)),
                Arguments.argumentSet("empty authcid", (Object) "\0\0tanstaaftanstaaf".getBytes(UTF_8)));
    }

    @ParameterizedTest
    @MethodSource(value = "badClientResponse")
    void shouldRejectBadClientInitialResponse(byte[] initialResponse) {
        // Given
        var observer = new PlainSaslObserver();

        // When/Then
        assertThatThrownBy(() -> observer.clientResponse(initialResponse))
                .isInstanceOf(AuthenticationException.class);

        assertThat(observer.isFinished()).isFalse();
        assertThatThrownBy(observer::authorizationId)
                .isInstanceOf(AuthenticationException.class);

    }

}