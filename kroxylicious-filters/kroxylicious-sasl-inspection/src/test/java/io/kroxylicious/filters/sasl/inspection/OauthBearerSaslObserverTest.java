/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters.sasl.inspection;

import java.util.stream.Stream;

import javax.security.sasl.SaslException;

import org.jose4j.lang.InvalidKeyException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OauthBearerSaslObserverTest {

    private static final byte[] JSON_ERROR_RESPONSE = """
                {"status":"invalid_token"}
            """.getBytes(UTF_8);
    private static final byte[] CONTROL_A = "\u0001".getBytes(UTF_8);

    @Test
    void initialState() {
        // Given/When
        var observer = new OauthBearerSaslObserver();

        // Then
        assertThat(observer.isFinished()).isFalse();
        assertThatThrownBy(observer::authorizationId).isInstanceOf(SaslException.class);
    }

    @Test
    void mechanismName() {
        // Given/When
        var observer = new OauthBearerSaslObserver();

        // Then
        assertThat(observer.mechanismName()).isEqualTo("OAUTHBEARER");
    }

    static Stream<Arguments> goodInitialResponses() {
        return Stream.of(
                // https://datatracker.ietf.org/doc/html/rfc7628#section-3.1
                // The GS2 header MAY include the username associated with the resource
                // being accessed, the "authzid". It is worth noting that application
                // protocols are allowed to require an authzid, as are specific server
                // implementations.
                Arguments.argumentSet("non-JWT response with gs2authzid", TestData.SASL_OAUTHBEARER_CLIENT_INITIAL, "user@example.com"),

                // https://datatracker.ietf.org/doc/html/rfc5801#section-4
                // The "gs2-authzid" holds the SASL authorization identity.
                // The server MUST replace any "," (comma) in the string with "=2C"
                // The server MUST replace any "=" (equals) in the string with "=3D"
                Arguments.argumentSet("gs2authzid with encoded comma", TestData.SASL_OAUTHBEARER_CLIENT_INITIAL_ENCODED_COMMA, "billy,bob"),
                Arguments.argumentSet("gs2authzid with encoded equals", TestData.SASL_OAUTHBEARER_CLIENT_INITIAL_ENCODED_EQUALS, "billy=bob"),

                Arguments.argumentSet("signed JWT response with no gs2authzid", TestData.SASL_OAUTHBEARER_SIGNED_JWT, "johndoe"),
                Arguments.argumentSet("unsigned JWT response with no gs2authzid", TestData.SASL_OAUTHBEARER_UNSECURED_JWT, "johndoe"));
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

    static Stream<Arguments> badInitialResponses() {
        return Stream.of(
                Arguments.argumentSet("empty auth key value", (Object) TestData.SASL_OAUTHBEARER_CLIENT_INITIAL_EMPTY_AUTH),
                Arguments.argumentSet("bad gs2-header",
                        (Object) "z,,\u0001host=server.example.com\u0001port=143\u0001auth=Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJqb2huZG9lIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNzYwMzY4NTg2LCJleHAiOjE3NjAzNzIxODZ9.TuSlWIpx1qQuUA3QhpnTx4LV2o_RTpy15azKvu7lO4M\u0001\u0001"
                                .getBytes(UTF_8)),
                Arguments.argumentSet("unrecognized encoded char within gs2authzid",
                        (Object) "n,a=billy=2Dbob,\u0001host=server.example.com\u0001port=143\u0001auth=Bearer vF9dft4qmTc2Nvb3RlckBhbHRhdmlzdGEuY29tCg==\u0001\u0001"
                                .getBytes(UTF_8)),
                Arguments.argumentSet("no gs2authzid and JWT with missing subject claim",
                        (Object) "n,,\u0001host=server.example.com\u0001port=143\u0001auth=Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJuYW1lIjoiSm9obiBEb2UiLCJhZG1pbiI6dHJ1ZSwiaWF0IjoxNzYwMzcxOTQ2LCJleHAiOjE3NjAzNzU1NDZ9.pfBuYRs3Tlx5t64YGFKr7ab2439vZGR-XI9HZU0HwLs\u0001\u0001"
                                .getBytes(UTF_8)));
    }

    @ParameterizedTest
    @MethodSource(value = "badInitialResponses")
    void shouldRejectBadClientFirstMessage(byte[] initialResponse) {
        // Given
        var observer = new OauthBearerSaslObserver();

        // When/Then
        assertThatThrownBy(() -> observer.clientResponse(initialResponse))
                .isInstanceOf(SaslException.class);

        assertThat(observer.isFinished()).isFalse();
        assertThatThrownBy(observer::authorizationId)
                .isInstanceOf(SaslException.class);
    }

    /**
     * We don't support the case where encrypted JWT tokens are used.  This
     * test makes sure we report sufficient details to allow the case to be identified.
     */
    @Test
    void shouldRejectJwtEncryptedClientFirstMessage() {
        // Given
        var observer = new OauthBearerSaslObserver();

        // When/Then
        assertThatThrownBy(() -> observer.clientResponse(TestData.SASL_OAUTHBEARER_ENCRYPTED_JWT))
                .isInstanceOf(SaslException.class)
                .hasRootCauseInstanceOf(InvalidKeyException.class)
                .hasRootCauseMessage("The key must not be null.");

        assertThat(observer.isFinished()).isFalse();
        assertThatThrownBy(observer::authorizationId)
                .isInstanceOf(SaslException.class);
    }

    /**
     * <a href="https://datatracker.ietf.org/doc/html/rfc7628#section-3.2.2">rfc7628 3.2.2 says:</a>
     * For a failed authentication, the server returns an error result in
     * JSON [RFC7159] format and fails the authentication.
     *
     * @throws SaslException sasl exception
     */
    @Test
    void shouldHandleServerErrorResponse() throws SaslException {
        // Given
        var observer = new OauthBearerSaslObserver();
        observer.clientResponse(TestData.SASL_OAUTHBEARER_SIGNED_JWT);

        // When
        observer.serverChallenge(JSON_ERROR_RESPONSE);

        // Then
        assertThat(observer.isFinished()).isFalse();
    }

    /**
     * <a href="https://datatracker.ietf.org/doc/html/rfc7628#section-3.2.3">rfc7628 3.2.2 says:</a>
     * The client MUST then send either an additional client response consisting of a single %x01
     * (control A) character to the server in order to allow the server to
     * finish the exchange
     *
     * @throws SaslException sasl exception
     */
    @Test
    void shouldPermitCompletionErrorMessageSequence() throws SaslException {
        // Given
        var observer = new OauthBearerSaslObserver();
        observer.clientResponse(TestData.SASL_OAUTHBEARER_SIGNED_JWT);

        observer.serverChallenge(JSON_ERROR_RESPONSE);
        assertThat(observer.isFinished()).isFalse();
        observer.clientResponse(CONTROL_A);

        // When
        observer.serverChallenge(new byte[0]);

        // Then
        assertThat(observer.isFinished()).isTrue();
    }

    @Test
    void shouldDetectUnexpectedClientResponse() throws SaslException {
        // Given
        var observer = new OauthBearerSaslObserver();
        observer.clientResponse(TestData.SASL_OAUTHBEARER_SIGNED_JWT);
        observer.serverChallenge(JSON_ERROR_RESPONSE);
        byte[] unexpectedClientResponse = "boo".getBytes(UTF_8);

        // When/Then
        assertThatThrownBy(() -> observer.clientResponse(unexpectedClientResponse))
                .isInstanceOf(SaslException.class);
    }

    @Test
    void shouldDetectUnexpectedServerChallenge() throws SaslException {
        // Given
        var observer = new OauthBearerSaslObserver();
        observer.clientResponse(TestData.SASL_OAUTHBEARER_SIGNED_JWT);
        observer.serverChallenge(JSON_ERROR_RESPONSE);
        observer.clientResponse(CONTROL_A);
        byte[] unexpectedChallenge = "boo".getBytes(UTF_8);

        // When/Then
        assertThatThrownBy(() -> observer.serverChallenge(unexpectedChallenge))
                .isInstanceOf(SaslException.class);
    }

}