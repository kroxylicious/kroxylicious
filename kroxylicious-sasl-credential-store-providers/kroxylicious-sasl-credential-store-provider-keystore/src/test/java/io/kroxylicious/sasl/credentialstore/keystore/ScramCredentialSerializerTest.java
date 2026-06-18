/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sasl.credentialstore.keystore;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.sasl.credentialstore.ScramCredential;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for JSON serialization and deserialization of ScramCredential.
 */
class ScramCredentialSerializerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ScramCredentialSerializer serializer = new ScramCredentialSerializer();

    @Test
    void shouldRoundTripCredentialViaJson() throws Exception {
        // Given
        ScramCredential original = new ScramCredential(
                "alice",
                new byte[]{ 1, 2, 3, 4, 5 },
                4096,
                new byte[]{ 10, 20, 30, 40, 50 },
                new byte[]{ 11, 21, 31, 41, 51 },
                "SHA-256");

        // When - serialize to JSON
        String json = objectMapper.writeValueAsString(original);

        // Then - JSON should contain base64-encoded bytes
        assertThat(json).contains("\"username\":\"alice\"");
        assertThat(json).contains("\"iterations\":4096");
        assertThat(json).contains("\"hashAlgorithm\":\"SHA-256\"");

        // When - deserialize from JSON
        ScramCredential deserialized = objectMapper.readValue(json, ScramCredential.class);

        // Then - should match original
        assertThat(deserialized.username()).isEqualTo(original.username());
        assertThat(deserialized.salt()).isEqualTo(original.salt());
        assertThat(deserialized.iterations()).isEqualTo(original.iterations());
        assertThat(deserialized.serverKey()).isEqualTo(original.serverKey());
        assertThat(deserialized.storedKey()).isEqualTo(original.storedKey());
        assertThat(deserialized.hashAlgorithm()).isEqualTo(original.hashAlgorithm());
    }

    @Test
    void shouldRoundTripCredentialViaSerializer() throws Exception {
        // Given
        ScramCredential original = new ScramCredential(
                "bob",
                new byte[]{ (byte) 0xFF, (byte) 0xEE, (byte) 0xDD },
                8192,
                new byte[]{ (byte) 0xAA, (byte) 0xBB, (byte) 0xCC },
                new byte[]{ (byte) 0x11, (byte) 0x22, (byte) 0x33 },
                "SHA-512");

        // When - serialize
        byte[] serialized = serializer.serialize(original);

        // Then - deserialize
        ScramCredential deserialized = serializer.deserialize(serialized, "bob");

        // Then - should match original
        assertThat(deserialized.username()).isEqualTo(original.username());
        assertThat(deserialized.salt()).isEqualTo(original.salt());
        assertThat(deserialized.iterations()).isEqualTo(original.iterations());
        assertThat(deserialized.serverKey()).isEqualTo(original.serverKey());
        assertThat(deserialized.storedKey()).isEqualTo(original.storedKey());
        assertThat(deserialized.hashAlgorithm()).isEqualTo(original.hashAlgorithm());
    }

    @Test
    void shouldHandleEmptyByteArraysInJson() throws Exception {
        // Given - credential with minimum length byte arrays (length 1, as empty is not allowed)
        ScramCredential original = new ScramCredential(
                "test",
                new byte[]{ 1 },
                4096,
                new byte[]{ 2 },
                new byte[]{ 3 },
                "SHA-256");

        // When - round-trip via JSON
        String json = objectMapper.writeValueAsString(original);
        ScramCredential deserialized = objectMapper.readValue(json, ScramCredential.class);

        // Then - should preserve array contents
        assertThat(deserialized.salt()).isEqualTo(original.salt());
        assertThat(deserialized.serverKey()).isEqualTo(original.serverKey());
        assertThat(deserialized.storedKey()).isEqualTo(original.storedKey());
    }

    @Test
    void shouldDefensiveCopyArraysOnConstruction() {
        // Given - mutable arrays
        byte[] salt = { 1, 2, 3 };
        byte[] serverKey = { 4, 5, 6 };
        byte[] storedKey = { 7, 8, 9 };

        // When - create credential
        ScramCredential credential = new ScramCredential(
                "user",
                salt,
                4096,
                serverKey,
                storedKey,
                "SHA-256");

        // Then - modify original arrays
        salt[0] = 99;
        serverKey[0] = 99;
        storedKey[0] = 99;

        // And - credential should be unaffected
        assertThat(credential.salt()).isEqualTo(new byte[]{ 1, 2, 3 });
        assertThat(credential.serverKey()).isEqualTo(new byte[]{ 4, 5, 6 });
        assertThat(credential.storedKey()).isEqualTo(new byte[]{ 7, 8, 9 });
    }

    @Test
    void shouldDefensiveCopyArraysOnAccess() {
        // Given
        ScramCredential credential = new ScramCredential(
                "user",
                new byte[]{ 1, 2, 3 },
                4096,
                new byte[]{ 4, 5, 6 },
                new byte[]{ 7, 8, 9 },
                "SHA-256");

        // When - get arrays and modify them
        byte[] salt = credential.salt();
        byte[] serverKey = credential.serverKey();
        byte[] storedKey = credential.storedKey();

        salt[0] = 99;
        serverKey[0] = 99;
        storedKey[0] = 99;

        // Then - subsequent access should return original values
        assertThat(credential.salt()).isEqualTo(new byte[]{ 1, 2, 3 });
        assertThat(credential.serverKey()).isEqualTo(new byte[]{ 4, 5, 6 });
        assertThat(credential.storedKey()).isEqualTo(new byte[]{ 7, 8, 9 });
    }

    @Test
    void shouldImplementEqualsCorrectlyWithArrays() {
        // Given - two credentials with same content
        ScramCredential credential1 = new ScramCredential(
                "user",
                new byte[]{ 1, 2, 3 },
                4096,
                new byte[]{ 4, 5, 6 },
                new byte[]{ 7, 8, 9 },
                "SHA-256");

        ScramCredential credential2 = new ScramCredential(
                "user",
                new byte[]{ 1, 2, 3 },
                4096,
                new byte[]{ 4, 5, 6 },
                new byte[]{ 7, 8, 9 },
                "SHA-256");

        // Then - should be equal
        assertThat(credential1).isEqualTo(credential2);
        assertThat(credential1.hashCode()).isEqualTo(credential2.hashCode());
    }

    @Test
    void shouldImplementEqualsCorrectlyWithDifferentArrays() {
        // Given - two credentials with different salt
        ScramCredential credential1 = new ScramCredential(
                "user",
                new byte[]{ 1, 2, 3 },
                4096,
                new byte[]{ 4, 5, 6 },
                new byte[]{ 7, 8, 9 },
                "SHA-256");

        ScramCredential credential2 = new ScramCredential(
                "user",
                new byte[]{ 1, 2, 4 }, // Different
                4096,
                new byte[]{ 4, 5, 6 },
                new byte[]{ 7, 8, 9 },
                "SHA-256");

        // Then - should not be equal
        assertThat(credential1).isNotEqualTo(credential2);
    }
}
