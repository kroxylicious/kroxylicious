/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sasl.credentialstore.keystore;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.sasl.credentialstore.ScramCredential;

/**
 * Serializes and deserializes {@link ScramCredential} objects to/from JSON for storage in KeyStore.
 * <p>
 * The JSON payload includes a {@code version} field to allow the format to be evolved
 * while maintaining backwards compatibility.
 * </p>
 */
public class ScramCredentialSerializer {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    static final int CURRENT_VERSION = 1;

    /**
     * Serialize a SCRAM credential to JSON bytes.
     *
     * @param credential the credential to serialize
     * @return JSON bytes suitable for storing in a KeyStore SecretKey
     * @throws IllegalArgumentException if serialization fails
     */
    public byte[] serialize(ScramCredential credential) {
        try {
            var versioned = new VersionedCredential(
                    CURRENT_VERSION,
                    credential.username(),
                    credential.salt(),
                    credential.iterations(),
                    credential.serverKey(),
                    credential.storedKey(),
                    credential.hashAlgorithm());
            return OBJECT_MAPPER.writeValueAsBytes(versioned);
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Failed to serialize credential for user: " + credential.username(), e);
        }
    }

    /**
     * Deserialize a SCRAM credential from JSON bytes.
     *
     * @param bytes JSON bytes from a KeyStore SecretKey
     * @param alias the KeyStore alias (used for error messages)
     * @return the deserialized credential
     * @throws IllegalArgumentException if deserialization or validation fails
     */
    public ScramCredential deserialize(byte[] bytes, String alias) {
        VersionedCredential versioned;
        try {
            versioned = OBJECT_MAPPER.readValue(bytes, VersionedCredential.class);
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Failed to deserialize credential for alias: " + alias, e);
        }
        if (versioned.version() != CURRENT_VERSION) {
            throw new IllegalArgumentException(
                    "Unsupported credential version " + versioned.version() + " for alias: " + alias
                            + " (expected " + CURRENT_VERSION + ")");
        }
        try {
            return new ScramCredential(
                    versioned.username(),
                    versioned.salt(),
                    versioned.iterations(),
                    versioned.serverKey(),
                    versioned.storedKey(),
                    versioned.hashAlgorithm());
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid credential for alias: " + alias, e);
        }
    }

    record VersionedCredential(
                               @JsonProperty(required = true) int version,
                               @JsonProperty(required = true) String username,
                               @JsonProperty(required = true) byte[] salt,
                               @JsonProperty(required = true) int iterations,
                               @JsonProperty(required = true) byte[] serverKey,
                               @JsonProperty(required = true) byte[] storedKey,
                               @JsonProperty(required = true) String hashAlgorithm) {}
}
