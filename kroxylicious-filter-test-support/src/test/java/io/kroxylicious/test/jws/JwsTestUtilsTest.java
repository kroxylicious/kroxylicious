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

    private boolean isJwsWithDetachedContent(String string) {
        return string.contains("..");
    }
}