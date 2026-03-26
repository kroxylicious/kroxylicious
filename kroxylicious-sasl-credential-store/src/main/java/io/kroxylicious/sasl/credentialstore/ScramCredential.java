/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sasl.credentialstore;

import java.util.Objects;
import java.util.Set;

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
 * @param salt Base64-encoded salt used in PBKDF2 derivation
 * @param iterations number of PBKDF2 iterations (must be >= 4096)
 * @param serverKey Base64-encoded SCRAM server key
 * @param storedKey Base64-encoded SCRAM stored key
 * @param hashAlgorithm the hash algorithm ("SHA-256" or "SHA-512")
 */
public record ScramCredential(
                              String username,
                              String salt,
                              int iterations,
                              String serverKey,
                              String storedKey,
                              String hashAlgorithm) {

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
        if (salt.isEmpty()) {
            throw new IllegalArgumentException("salt must not be empty");
        }
        if (serverKey.isEmpty()) {
            throw new IllegalArgumentException("serverKey must not be empty");
        }
        if (storedKey.isEmpty()) {
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
    }
}
