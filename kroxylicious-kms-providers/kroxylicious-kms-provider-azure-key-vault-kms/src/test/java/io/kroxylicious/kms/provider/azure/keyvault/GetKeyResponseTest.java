/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.keyvault;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GetKeyResponseTest {

    /**
     * Covers unknown properties in the key and attribute objects, we only deserialize what we use
     */
    @Test
    void deserializeComprehensiveResponse() throws JsonProcessingException {
        var response = """
                {
                  "key": {
                    "kid": "https://myvault.vault.azure.net/keys/CreateSoftKeyTest/78deebed173b48e48f55abf87ed4cf71",
                    "kty": "RSA",
                    "key_ops": [
                      "encrypt",
                      "decrypt",
                      "sign",
                      "verify",
                      "wrapKey",
                      "unwrapKey"
                    ],
                    "n": "2HJAE5fU3Cw2Rt9hEuq-F6XjINKGa-zskfISVqopqUy60GOs2eyhxbWbJBeUXNor_gf-tXtNeuqeBgitLeVa640UDvnEjYTKWjCniTxZRaU7ewY8BfTSk-7KxoDdLsPSpX_MX4rwlAx-_1UGk5t4sQgTbm9T6Fm2oqFd37dsz5-Gj27UP2GTAShfJPFD7MqU_zIgOI0pfqsbNL5xTQVM29K6rX4jSPtylZV3uWJtkoQIQnrIHhk1d0SC0KwlBV3V7R_LVYjiXLyIXsFzSNYgQ68ZjAwt8iL7I8Osa-ehQLM13DVvLASaf7Jnu3sC3CWl3Gyirgded6cfMmswJzY87w",
                    "e": "AQAB"
                  },
                  "attributes": {
                    "enabled": true,
                    "created": 1493942451,
                    "updated": 1493942451,
                    "recoveryLevel": "Recoverable+Purgeable"
                  },
                  "tags": {
                    "purpose": "unit test",
                    "test name ": "CreateGetDeleteKeyTest"
                  }
                }
                """;
        GetKeyResponse getKeyResponse = new ObjectMapper().readValue(response, GetKeyResponse.class);
        assertThat(getKeyResponse).isNotNull();
        KeyAttributes attributes = getKeyResponse.attributes();
        assertThat(attributes).isNotNull();
        assertThat(attributes.enabled()).isTrue();
        JsonWebKey key = getKeyResponse.key();
        assertThat(key).isNotNull();
        assertThat(key.keyId()).isEqualTo("https://myvault.vault.azure.net/keys/CreateSoftKeyTest/78deebed173b48e48f55abf87ed4cf71");
        assertThat(key.keyType()).isEqualTo("RSA");
        assertThat(key.keyOperations()).isEqualTo(List.of("encrypt", "decrypt", "sign", "verify", "wrapKey", "unwrapKey"));
    }

    @Test
    void keyRequired() {
        var response = """
                {
                  "attributes": {
                    "enabled": true
                  }
                }
                """;
        ObjectMapper objectMapper = new ObjectMapper();
        assertThatThrownBy(() -> objectMapper.readValue(response, GetKeyResponse.class))
                .isInstanceOf(MismatchedInputException.class).hasMessageContaining("Missing required creator property 'key'");
    }

    @Test
    void attributesRequired() {
        var response = """
                {
                  "key": {
                    "kid": "https://myvault.vault.azure.net/keys/CreateSoftKeyTest/78deebed173b48e48f55abf87ed4cf71",
                    "kty": "RSA",
                    "key_ops": [
                      "verify",
                      "unwrapKey"
                    ]
                  }
                }
                """;
        ObjectMapper objectMapper = new ObjectMapper();
        assertThatThrownBy(() -> objectMapper.readValue(response, GetKeyResponse.class))
                .isInstanceOf(MismatchedInputException.class).hasMessageContaining("Missing required creator property 'attributes'");
    }

}
