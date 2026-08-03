/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.scram.credentialstore.keystore;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.scram.credentialstore.ScramCredential;
import io.kroxylicious.scram.credentialstore.ScramHashAlgorithm;

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
                    credential.hashAlgorithm().algorithmName());
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
                    ScramHashAlgorithm.fromAlgorithmName(versioned.hashAlgorithm()));
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid credential for alias: " + alias, e);
        }
    }

    // Sensitive byte[] fields (salt, serverKey, storedKey) live in the JVM heap for the
    // lifetime of this object. This is an accepted risk: the same data already resides in
    // the ScramCredential record that this type feeds into, and there is no practical way
    // to guarantee memory clearing in a JVM (GC copies, JIT dead-store elimination).
    // See the "Accepted risk: credential material in JVM heap" section of proposal 124.
    @SuppressWarnings("ArrayRecordComponent") // defensive copies, equals and hashCode are overridden
    record VersionedCredential(
                               @JsonProperty(required = true) int version,
                               @JsonProperty(required = true) String username,
                               @JsonProperty(required = true) byte[] salt,
                               @JsonProperty(required = true) int iterations,
                               @JsonProperty(required = true) byte[] serverKey,
                               @JsonProperty(required = true) byte[] storedKey,
                               @JsonProperty(required = true) String hashAlgorithm) {

        VersionedCredential {
            salt = salt != null ? salt.clone() : null;
            serverKey = serverKey != null ? serverKey.clone() : null;
            storedKey = storedKey != null ? storedKey.clone() : null;
        }

        @Override
        public byte[] salt() {
            return salt != null ? salt.clone() : null;
        }

        @Override
        public byte[] serverKey() {
            return serverKey != null ? serverKey.clone() : null;
        }

        @Override
        public byte[] storedKey() {
            return storedKey != null ? storedKey.clone() : null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof VersionedCredential that)) {
                return false;
            }
            return version == that.version
                    && iterations == that.iterations
                    && Objects.equals(username, that.username)
                    && Arrays.equals(salt, that.salt)
                    && Arrays.equals(serverKey, that.serverKey)
                    && Arrays.equals(storedKey, that.storedKey)
                    && Objects.equals(hashAlgorithm, that.hashAlgorithm);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(version, username, iterations, hashAlgorithm);
            result = 31 * result + Arrays.hashCode(salt);
            result = 31 * result + Arrays.hashCode(serverKey);
            result = 31 * result + Arrays.hashCode(storedKey);
            return result;
        }

        @Override
        public String toString() {
            return "VersionedCredential{version=" + version
                    + ", username='" + username + "'"
                    + ", iterations=" + iterations
                    + ", hashAlgorithm='" + hashAlgorithm + "'"
                    + "}";
        }
    }
}
