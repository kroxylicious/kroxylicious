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

class ScramSaslObserverTest {

    private static final String MECHANISM_NAME = "SCRAM-SHA-256";

    @Test
    void initialState() {
        // Given/When
        var observer = new ScramSaslObserver(MECHANISM_NAME);

        // Then
        assertThat(observer.isFinished()).isFalse();
        assertThatThrownBy(observer::authorizationId).isInstanceOf(AuthenticationException.class);
    }

    @Test
    void mechanismName() {
        // Given/When
        var observer = new ScramSaslObserver(MECHANISM_NAME);

        // Then
        assertThat(observer.mechanismName()).isEqualTo(MECHANISM_NAME);
    }

    static Stream<Arguments> goodClientFirst() {
        return Stream.of(
                Arguments.argumentSet("username only", TestData.SASL_SCRAM_SHA_256_CLIENT_INITIAL, "user"),
                Arguments.argumentSet("authzid and username present", TestData.SASL_SCRAM_SHA256_CLIENT_INITIAL_WITH_AUTHZID, "Ursel"),
                Arguments.argumentSet("saslname with encoded comma", "n,,n=test=2Cuser,r=rOprNGfwEbeRWgbNEkqO".getBytes(UTF_8), "test,user"),
                Arguments.argumentSet("saslname with encoded equals", "n,,n=test=3Duser,r=rOprNGfwEbeRWgbNEkqO".getBytes(UTF_8), "test=user"),
                Arguments.argumentSet("saslname with many encoded chars", "n,,n=encoded=3Dchars=3Da=2Cgo=2Cgo,r=rOprNGfwEbeRWgbNEkqO".getBytes(UTF_8),
                        "encoded=chars=a,go,go"));
    }

    @ParameterizedTest
    @MethodSource("goodClientFirst")
    void shouldYieldAuthorizationIdFollowingSuccessfulNegotiation(byte[] clientFirst, String expectedAuthorizationId) {
        // Given
        var observer = new ScramSaslObserver(MECHANISM_NAME);

        // When
        observer.clientResponse(clientFirst);
        observer.serverChallenge(TestData.SASL_SCRAM_SHA_256_SERVER_FIRST);

        observer.clientResponse(TestData.SASL_SCRAM_SHA_256_CLIENT_FINAL);
        observer.serverChallenge(TestData.SASL_SCRAM_SHA_256_SERVER_FINAL);

        // Then
        assertThat(observer.isFinished()).isTrue();
        assertThat(observer.authorizationId()).isEqualTo(expectedAuthorizationId);
    }

    static Stream<Arguments> badClientFirstMessage() {
        return Stream.of(
                Arguments.argumentSet("too many attributes (includes disallowed reserved-mext)",
                        (Object) "n,a=ursel,m=unexpected,n=testuser,r=fyko+d2lbbFgONRv9qkxdawL".getBytes(
                                UTF_8)),
                Arguments.argumentSet("missing username", (Object) "n,,r=fyko+d2lbbFgONRv9qkxdawL".getBytes(UTF_8)),
                Arguments.argumentSet("empty username", (Object) "n,,n=,r=fyko+d2lbbFgONRv9qkxdawL".getBytes(UTF_8)),
                Arguments.argumentSet("unrecognized encoded char", (Object) "n,,n=hello=2D,r=fyko+d2lbbFgONRv9qkxdawL".getBytes(UTF_8)));
    }

    @ParameterizedTest
    @MethodSource(value = "badClientFirstMessage")
    void shouldRejectBadClientFirstMessage(byte[] initialResponse) {
        // Given
        var observer = new ScramSaslObserver(MECHANISM_NAME);

        // When/Then
        assertThatThrownBy(() -> observer.clientResponse(initialResponse))
                .isInstanceOf(AuthenticationException.class);

        assertThat(observer.isFinished()).isFalse();
        assertThatThrownBy(observer::authorizationId)
                .isInstanceOf(AuthenticationException.class);

    }

    @Test
    void shouldHandleNegotiationEndingWithServerError() {
        // Given
        var observer = new ScramSaslObserver(MECHANISM_NAME);

        // When
        observer.clientResponse(TestData.SASL_SCRAM_SHA_256_CLIENT_INITIAL);
        observer.serverChallenge(TestData.SASL_SCRAM_SHA_256_SERVER_FIRST);

        observer.clientResponse(TestData.SASL_SCRAM_SHA_256_CLIENT_FINAL);
        observer.serverChallenge("e=unknown-user".getBytes(UTF_8));

        // Then
        assertThat(observer.isFinished()).isTrue();
        assertThat(observer.authorizationId()).isEqualTo("user");
    }
}