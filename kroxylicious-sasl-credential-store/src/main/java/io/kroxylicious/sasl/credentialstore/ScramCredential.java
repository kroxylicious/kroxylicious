/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sasl.credentialstore;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * SCRAM credential for SASL authentication.
 * <p>
 * Stores the salted, hashed password data required for SCRAM authentication.
 * Never stores plaintext passwords.
 * </p>
 * <p>
 * SCRAM (Salted Challenge Response Authentication Mechanism) as defined in RFC 5802
 * uses PBKDF2 to derive two keys from the password:
 * </p>
 * <ul>
 *     <li><strong>Stored key</strong> - Used to verify the client knows the password</li>
 *     <li><strong>Server key</strong> - Used by the server to prove it knows the password</li>
 * </ul>
 *
 * <h2>Security Considerations</h2>
 * <p>
 * Credentials must be generated with sufficient iterations to resist brute-force attacks.
 * The minimum is 4096 iterations, but higher values (10000+) are recommended for production use.
 * </p>
 *
 * @param username the username associated with this credential
 * @param salt salt bytes used in PBKDF2 derivation
 * @param iterations number of PBKDF2 iterations (must be >= 4096)
 * @param serverKey SCRAM server key bytes
 * @param storedKey SCRAM stored key bytes
 * @param hashAlgorithm the hash algorithm ("SHA-256" or "SHA-512")
 */
public record ScramCredential(
                              @JsonProperty(required = true) String username,
                              @JsonProperty(required = true) byte[] salt,
                              @JsonProperty(required = true) int iterations,
                              @JsonProperty(required = true) byte[] serverKey,
                              @JsonProperty(required = true) byte[] storedKey,
                              @JsonProperty(required = true) String hashAlgorithm) {

    /**
     * Minimum number of PBKDF2 iterations required.
     */
    public static final int MINIMUM_ITERATIONS = 4096;

    /**
     * Supported hash algorithms.
     */
    private static final Set<String> SUPPORTED_ALGORITHMS = Set.of("SHA-256", "SHA-512");

    /**
     * Canonical constructor with validation.
     *
     * @throws NullPointerException if any parameter is null
     * @throws IllegalArgumentException if iterations {@literal <} 4096 or hash algorithm is not supported
     */
    public ScramCredential {
        Objects.requireNonNull(username, "username must not be null");
        Objects.requireNonNull(salt, "salt must not be null");
        Objects.requireNonNull(serverKey, "serverKey must not be null");
        Objects.requireNonNull(storedKey, "storedKey must not be null");
        Objects.requireNonNull(hashAlgorithm, "hashAlgorithm must not be null");

        if (username.isEmpty()) {
            throw new IllegalArgumentException("username must not be empty");
        }
        if (salt.length == 0) {
            throw new IllegalArgumentException("salt must not be empty");
        }
        if (serverKey.length == 0) {
            throw new IllegalArgumentException("serverKey must not be empty");
        }
        if (storedKey.length == 0) {
            throw new IllegalArgumentException("storedKey must not be empty");
        }
        if (iterations < MINIMUM_ITERATIONS) {
            throw new IllegalArgumentException(
                    "iterations must be at least " + MINIMUM_ITERATIONS + ", got: " + iterations);
        }
        if (!SUPPORTED_ALGORITHMS.contains(hashAlgorithm)) {
            throw new IllegalArgumentException(
                    "hashAlgorithm must be one of " + SUPPORTED_ALGORITHMS + ", got: " + hashAlgorithm);
        }

        // Defensive copies for mutable arrays
        salt = salt.clone();
        serverKey = serverKey.clone();
        storedKey = storedKey.clone();
    }

    /**
     * Accessor that returns a defensive copy of the salt.
     *
     * @return a copy of the salt bytes
     */
    @Override
    public byte[] salt() {
        return salt.clone();
    }

    /**
     * Accessor that returns a defensive copy of the server key.
     *
     * @return a copy of the server key bytes
     */
    @Override
    public byte[] serverKey() {
        return serverKey.clone();
    }

    /**
     * Accessor that returns a defensive copy of the stored key.
     *
     * @return a copy of the stored key bytes
     */
    @Override
    public byte[] storedKey() {
        return storedKey.clone();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ScramCredential that = (ScramCredential) obj;
        return iterations == that.iterations &&
                Objects.equals(username, that.username) &&
                Arrays.equals(salt, that.salt) &&
                Arrays.equals(serverKey, that.serverKey) &&
                Arrays.equals(storedKey, that.storedKey) &&
                Objects.equals(hashAlgorithm, that.hashAlgorithm);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(username, iterations, hashAlgorithm);
        result = 31 * result + Arrays.hashCode(salt);
        result = 31 * result + Arrays.hashCode(serverKey);
        result = 31 * result + Arrays.hashCode(storedKey);
        return result;
    }

    @Override
    public String toString() {
        return "ScramCredential{" +
                "username='" + username + '\'' +
                ", hashAlgorithm='" + hashAlgorithm + '\'' +
                ", iterations=" + iterations +
                ", salt=***" +
                ", serverKey=***" +
                ", storedKey=***" +
                '}';
    }
}
