/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.keyvault;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WrapOrUnwrapResponseTest {

    public static final byte[] CARRIED_BYTES = { 1, 2, 3 };

    @Test
    void deserializeResponse() throws JsonProcessingException {
        var response = """
                {
                  "kid": "https://myvault.vault.azure.net/keys/sdktestkey/0698c2156c1a4e1da5b6bab6f6422fd6",
                  "value": "AQID"
                }
                """;
        WrapOrUnwrapResponse wrapOrUnwrapResponse = new ObjectMapper().readValue(response, WrapOrUnwrapResponse.class);
        assertThat(wrapOrUnwrapResponse).isNotNull();
        assertThat(wrapOrUnwrapResponse.value()).isEqualTo("AQID");
        assertThat(wrapOrUnwrapResponse.decodedValue()).containsExactly(CARRIED_BYTES);
        assertThat(wrapOrUnwrapResponse.keyId()).isEqualTo("https://myvault.vault.azure.net/keys/sdktestkey/0698c2156c1a4e1da5b6bab6f6422fd6");
    }

    @Test
    void toleratesUnknownFields() throws JsonProcessingException {
        var response = """
                {
                  "kid": "https://myvault.vault.azure.net/keys/sdktestkey/0698c2156c1a4e1da5b6bab6f6422fd6",
                  "value": "AQID",
                  "future": "proof"
                }
                """;
        WrapOrUnwrapResponse wrapOrUnwrapResponse = new ObjectMapper().readValue(response, WrapOrUnwrapResponse.class);
        assertThat(wrapOrUnwrapResponse).isNotNull();
        assertThat(wrapOrUnwrapResponse.value()).isEqualTo("AQID");
        assertThat(wrapOrUnwrapResponse.decodedValue()).containsExactly(CARRIED_BYTES);
        assertThat(wrapOrUnwrapResponse.keyId()).isEqualTo("https://myvault.vault.azure.net/keys/sdktestkey/0698c2156c1a4e1da5b6bab6f6422fd6");
    }

    @Test
    void valueCannotBeDecoded() {
        WrapOrUnwrapResponse wrapOrUnwrapResponse = new WrapOrUnwrapResponse("abc", "!!!");
        assertThatThrownBy(wrapOrUnwrapResponse::decodedValue).isInstanceOf(IllegalArgumentException.class);
    }

}
