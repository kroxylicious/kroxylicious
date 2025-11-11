/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.jws;

import org.jose4j.jwk.JsonWebKeySet;
import org.junit.jupiter.api.Test;

import static io.kroxylicious.test.jws.JwsTestUtils.ECDSA_SIGN_JWKS;
import static io.kroxylicious.test.jws.JwsTestUtils.VALID_JWS_USING_ECDSA_JWK_AND_CONTENT_DETACHED_PAYLOAD;
import static io.kroxylicious.test.jws.JwsTestUtils.VALID_JWS_USING_ECDSA_JWK_AND_UNENCODED_CONTENT_DETACHED_PAYLOAD;
import static io.kroxylicious.test.jws.JwsTestUtils.VALID_JWS_USING_ECDSA_JWK_PAYLOAD;
import static io.kroxylicious.test.jws.JwsTestUtils.generateJws;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JwsTestUtilsTest {
    static final String BASE64_ENCODED_LEFT_CURLY_BRACKET = "ey";
    static final JsonWebKeySet EMPTY_JWKS = new JsonWebKeySet();

    @Test
    void generateJwsGeneratesValidEcdsaJws() {
        String jws = generateJws(ECDSA_SIGN_JWKS, VALID_JWS_USING_ECDSA_JWK_PAYLOAD, false, false);

        assertFalse(jws.isEmpty());
        assertTrue(isJsonObject(jws));
    }

    @Test
    void generateJwsGeneratesValidEcdsaJwsWithDetachedContent() {
        String jws = generateJws(ECDSA_SIGN_JWKS, VALID_JWS_USING_ECDSA_JWK_AND_CONTENT_DETACHED_PAYLOAD,
                true,
                false);

        assertFalse(jws.isEmpty());
        assertTrue(isJsonObject(jws));
        assertTrue(isJwsWithDetachedContent(jws));
    }

    @Test
    void generateJwsGeneratesValidEcdsaJwsWithDetachedContentAndUnencodedPayload() {
        String jws = generateJws(ECDSA_SIGN_JWKS,
                VALID_JWS_USING_ECDSA_JWK_AND_UNENCODED_CONTENT_DETACHED_PAYLOAD, true, true);

        assertFalse(jws.isEmpty());
        assertTrue(isJsonObject(jws));
        assertTrue(isJwsWithDetachedContent(jws));
    }

    @Test
    void generateJwsFailsWhenJwksIsEmpty() {
        assertThrows(IndexOutOfBoundsException.class, () -> generateJws(EMPTY_JWKS, "random string", false, false));
    }

    private boolean isJsonObject(String string) {
        return string.startsWith(BASE64_ENCODED_LEFT_CURLY_BRACKET);
    }

    private boolean isJwsWithDetachedContent(String string) {
        return string.contains("..");
    }
}