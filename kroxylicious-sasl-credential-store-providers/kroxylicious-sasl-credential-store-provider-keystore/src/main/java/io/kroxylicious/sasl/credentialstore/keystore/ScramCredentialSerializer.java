/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sasl.credentialstore.keystore;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.sasl.credentialstore.ScramCredential;

/**
 * Serializes and deserializes {@link ScramCredential} objects to/from JSON for storage in KeyStore.
 * <p>
 * Credentials are serialized as compact JSON and stored as the bytes of a SecretKey entry.
 * </p>
 */
public class ScramCredentialSerializer {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * Serialize a SCRAM credential to JSON bytes.
     *
     * @param credential the credential to serialize
     * @return JSON bytes suitable for storing in a KeyStore SecretKey
     * @throws IllegalArgumentException if serialization fails
     */
    public byte[] serialize(ScramCredential credential) {
        try {
            return OBJECT_MAPPER.writeValueAsBytes(credential);
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
        try {
            ScramCredential credential = OBJECT_MAPPER.readValue(bytes, ScramCredential.class);
            // Verify the credential is valid (canonical constructor will validate)
            return credential;
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Failed to deserialize credential for alias: " + alias
                    + ". Content: " + new String(bytes, StandardCharsets.UTF_8), e);
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid credential for alias: " + alias, e);
        }
    }
}
