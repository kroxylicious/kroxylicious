/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.jws;

import java.nio.charset.StandardCharsets;

import org.jose4j.jwk.JsonWebKeySet;
import org.junit.jupiter.api.Test;

import static io.kroxylicious.test.jws.JwsTestUtils.ECDSA_SIGN_JWKS;
import static io.kroxylicious.test.jws.JwsTestUtils.VALID_JWS_USING_ECDSA_JWK_AND_CONTENT_DETACHED_PAYLOAD;
import static io.kroxylicious.test.jws.JwsTestUtils.VALID_JWS_USING_ECDSA_JWK_AND_UNENCODED_CONTENT_DETACHED_PAYLOAD;
import static io.kroxylicious.test.jws.JwsTestUtils.VALID_JWS_USING_ECDSA_JWK_PAYLOAD;
import static io.kroxylicious.test.jws.JwsTestUtils.generateJws;
import static io.kroxylicious.test.jws.JwsTestUtils.invalidJws;
import static io.kroxylicious.test.jws.JwsTestUtils.validJwsUsingEcdsaJwk;
import static io.kroxylicious.test.jws.JwsTestUtils.validJwsUsingEcdsaJwkAndContentDetached;
import static io.kroxylicious.test.jws.JwsTestUtils.validJwsUsingEcdsaJwkAndUnencodedContentDetached;
import static io.kroxylicious.test.jws.JwsTestUtils.validJwsUsingMissingEcdsaJwk;
import static io.kroxylicious.test.jws.JwsTestUtils.validJwsUsingRsaJwk;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JwsTestUtilsTest {
    static final String BASE64_ENCODED_LEFT_CURLY_BRACKET = "ey";
    static final JsonWebKeySet EMPTY_JWKS = new JsonWebKeySet();

    @Test
    void generateJwsGeneratesValidEcdsaJws() {
        String jws = generateJws(ECDSA_SIGN_JWKS, VALID_JWS_USING_ECDSA_JWK_PAYLOAD, false, false);

        assertThat(jws).isNotEmpty();
        assertThat(isJsonObject(jws)).isTrue();
    }

    @Test
    void generateJwsGeneratesValidEcdsaJwsWithDetachedContent() {
        String jws = generateJws(ECDSA_SIGN_JWKS, VALID_JWS_USING_ECDSA_JWK_AND_CONTENT_DETACHED_PAYLOAD,
                true,
                false);

        assertThat(jws).isNotEmpty();
        assertThat(isJsonObject(jws)).isTrue();
        assertThat(isJwsWithDetachedContent(jws)).isTrue();
    }

    @Test
    void generateJwsGeneratesValidEcdsaJwsWithDetachedContentAndUnencodedPayload() {
        String jws = generateJws(ECDSA_SIGN_JWKS,
                VALID_JWS_USING_ECDSA_JWK_AND_UNENCODED_CONTENT_DETACHED_PAYLOAD, true, true);

        assertThat(jws).isNotEmpty();
        assertThat(isJsonObject(jws)).isTrue();
        assertThat(isJwsWithDetachedContent(jws)).isTrue();
    }

    @Test
    void generateJwsFailsWhenJwksIsEmpty() {
        assertThatThrownBy(() -> generateJws(EMPTY_JWKS, "random string", false, false)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void generateJwsFailsWhenPayloadIsInvalid() {
        assertThatThrownBy(() -> generateJws(ECDSA_SIGN_JWKS, "unencoded.payload.with.dot", false, true)).isInstanceOf(IllegalArgumentException.class);
    }

    private boolean isJsonObject(String string) {
        return string.startsWith(BASE64_ENCODED_LEFT_CURLY_BRACKET);
    }

    @Test
    void validJwsUsingEcdsaJwkReturnsDefensiveCopy() {
        byte[] first = validJwsUsingEcdsaJwk();
        assertThat(first).isNotNull();
        assertThat(new String(first, StandardCharsets.UTF_8)).isNotEmpty();

        byte[] originalCopy = first.clone();
        first[0] = (byte) 0xFF;

        byte[] second = validJwsUsingEcdsaJwk();
        assertThat(second).isEqualTo(originalCopy);
        assertThat(second).isNotSameAs(first);
    }

    @Test
    void validJwsUsingMissingEcdsaJwkReturnsDefensiveCopy() {
        byte[] first = validJwsUsingMissingEcdsaJwk();
        assertThat(first).isNotNull();
        assertThat(new String(first, StandardCharsets.UTF_8)).isNotEmpty();

        byte[] originalCopy = first.clone();
        first[0] = (byte) 0xFF;

        byte[] second = validJwsUsingMissingEcdsaJwk();
        assertThat(second).isEqualTo(originalCopy);
        assertThat(second).isNotSameAs(first);
    }

    @Test
    void validJwsUsingRsaJwkReturnsDefensiveCopy() {
        byte[] first = validJwsUsingRsaJwk();
        assertThat(first).isNotNull();
        assertThat(new String(first, StandardCharsets.UTF_8)).isNotEmpty();

        byte[] originalCopy = first.clone();
        first[0] = (byte) 0xFF;

        byte[] second = validJwsUsingRsaJwk();
        assertThat(second).isEqualTo(originalCopy);
        assertThat(second).isNotSameAs(first);
    }

    @Test
    void validJwsUsingEcdsaJwkAndContentDetachedReturnsDefensiveCopy() {
        byte[] first = validJwsUsingEcdsaJwkAndContentDetached();
        assertThat(first).isNotNull();

        String jwsString = new String(first, StandardCharsets.UTF_8);
        assertThat(isJwsWithDetachedContent(jwsString)).isTrue();
        assertThat(startsWithBase64JsonObject(jwsString)).isTrue();

        byte[] originalCopy = first.clone();
        first[0] = (byte) 0xFF;

        byte[] second = validJwsUsingEcdsaJwkAndContentDetached();
        assertThat(second).isEqualTo(originalCopy);
        assertThat(second).isNotSameAs(first);
    }

    @Test
    void validJwsUsingEcdsaJwkAndUnencodedContentDetachedReturnsDefensiveCopy() {
        byte[] first = validJwsUsingEcdsaJwkAndUnencodedContentDetached();
        assertThat(first).isNotNull();

        String jwsString = new String(first, StandardCharsets.UTF_8);
        assertThat(isJwsWithDetachedContent(jwsString)).isTrue();
        assertThat(startsWithBase64JsonObject(jwsString)).isTrue();

        byte[] originalCopy = first.clone();
        first[0] = (byte) 0xFF;

        byte[] second = validJwsUsingEcdsaJwkAndUnencodedContentDetached();
        assertThat(second).isEqualTo(originalCopy);
        assertThat(second).isNotSameAs(first);
    }

    @Test
    void invalidJwsReturnsDefensiveCopy() {
        byte[] first = invalidJws();
        assertThat(first).isNotNull();

        byte[] expectedValue = "This is a non JWS value".getBytes(StandardCharsets.UTF_8);
        assertThat(first).isEqualTo(expectedValue);

        byte[] originalCopy = first.clone();
        first[0] = (byte) 0xFF;

        byte[] second = invalidJws();
        assertThat(second).isEqualTo(originalCopy);
        assertThat(second).isNotSameAs(first);
    }

    private boolean isJwsWithDetachedContent(String jws) {
        return jws.contains("..");
    }

    private boolean startsWithBase64JsonObject(String value) {
        return value.startsWith(BASE64_ENCODED_LEFT_CURLY_BRACKET);
    }
}