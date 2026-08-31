/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;

import io.kroxylicious.kms.provider.hashicorp.vault.VaultResponse.ReadKeyData;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VaultResponseTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<VaultResponse<ReadKeyData>> READ_KEY_TYPE = new TypeReference<>() {
    };

    @Test
    void readKeyDataDeserialisesLatestVersion() throws Exception {
        // Given
        String json = """
                {
                  "data": {
                    "name": "mykey",
                    "latest_version": 3
                  }
                }
                """;

        // When
        var result = MAPPER.readValue(json, READ_KEY_TYPE).data();

        // Then
        assertThat(result.name()).isEqualTo("mykey");
        assertThat(result.latestVersion()).isEqualTo(3);
    }

    @Test
    void readKeyDataRejectsMissingLatestVersion() {
        // Given
        String json = """
                {
                  "data": {
                    "name": "mykey"
                  }
                }
                """;

        // When / Then
        assertThatThrownBy(() -> MAPPER.readValue(json, READ_KEY_TYPE))
                .isInstanceOf(MismatchedInputException.class);
    }
}
