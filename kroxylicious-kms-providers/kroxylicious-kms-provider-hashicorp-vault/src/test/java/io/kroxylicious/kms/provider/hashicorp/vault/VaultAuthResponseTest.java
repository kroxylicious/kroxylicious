/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VaultAuthResponseTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testDeserialization() throws Exception {
        String json = """
                {
                  "auth": {
                    "client_token": "hvs.some-token",
                    "lease_duration": 3600,
                    "metadata": {
                      "role": "my-role"
                    }
                  }
                }
                """;
        VaultAuthResponse response = objectMapper.readValue(json, VaultAuthResponse.class);

        assertThat(response.auth()).isNotNull();
        assertThat(response.auth().clientToken()).isEqualTo("hvs.some-token");
        assertThat(response.auth().leaseDuration()).isEqualTo(3600);
    }

    @Test
    void testMissingClientToken() {
        assertThatThrownBy(() -> new VaultAuthResponse.Auth(null, 3600))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testMissingAuth() {
        assertThatThrownBy(() -> new VaultAuthResponse(null))
                .isInstanceOf(NullPointerException.class);
    }
}
