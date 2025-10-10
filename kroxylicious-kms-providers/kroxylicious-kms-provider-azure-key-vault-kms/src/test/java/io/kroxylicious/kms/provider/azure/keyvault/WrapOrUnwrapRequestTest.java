/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.keyvault;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WrapOrUnwrapRequestTest {

    @Test
    void testSerialize() throws JsonProcessingException {
        WrapOrUnwrapRequest wrapOrUnwrapRequest = new WrapOrUnwrapRequest("algo", "keybytes");
        String outo = new ObjectMapper().writeValueAsString(wrapOrUnwrapRequest);
        String expected = """
                {"alg":"algo","value":"keybytes"}""";
        assertThat(outo).isEqualTo(expected);
    }

    @CsvSource(nullValues = "null", value = { "null, keybytes, alg cannot be null", "algo, null, value cannot be null" })
    @ParameterizedTest
    void constructorArgsMustBeNonNull(String algorithm, String keybytes, String error) {
        assertThatThrownBy(() -> new WrapOrUnwrapRequest(algorithm, keybytes))
                .isInstanceOf(NullPointerException.class)
                .hasMessage(error);
    }

    @Test
    void fromBytes() {
        WrapOrUnwrapRequest request = WrapOrUnwrapRequest.from("algo", new byte[]{ 1, 2, 3 });
        assertThat(request).isNotNull();
        assertThat(request.algorithm()).isEqualTo("algo");
        assertThat(request.value()).isEqualTo("AQID");
    }

}