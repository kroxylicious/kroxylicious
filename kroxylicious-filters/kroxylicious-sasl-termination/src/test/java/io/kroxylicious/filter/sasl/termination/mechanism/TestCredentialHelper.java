/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination.mechanism;

import java.security.SecureRandom;
import java.util.Base64;

import org.apache.kafka.common.security.scram.internals.ScramFormatter;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;

import io.kroxylicious.sasl.credentialstore.ScramCredential;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Test helper for generating SCRAM credentials.
 */
class TestCredentialHelper {

    private static final int DEFAULT_ITERATIONS = 4096;
    private static final int SALT_LENGTH = 20;

    /**
     * Generate a SCRAM credential for testing.
     *
     * @param username the username
     * @param password the plaintext password
     * @param mechanism the SCRAM mechanism
     * @return the generated credential
     */
    @NonNull
    static ScramCredential generateCredential(
                                              @NonNull String username,
                                              @NonNull String password,
                                              @NonNull ScramMechanism mechanism) {

        try {
            byte[] salt = generateSalt();
            String saltBase64 = Base64.getEncoder().encodeToString(salt);

            ScramFormatter formatter = new ScramFormatter(mechanism);

            // Generate the salted password
            byte[] saltedPassword = formatter.saltedPassword(password, salt, DEFAULT_ITERATIONS);

            // Generate server key and stored key from the salted password
            byte[] serverKey = formatter.serverKey(saltedPassword);
            byte[] clientKey = formatter.clientKey(saltedPassword);
            byte[] storedKey = formatter.storedKey(clientKey);

            String serverKeyBase64 = Base64.getEncoder().encodeToString(serverKey);
            String storedKeyBase64 = Base64.getEncoder().encodeToString(storedKey);

            String hashAlgorithm = mechanism == ScramMechanism.SCRAM_SHA_256 ? "SHA-256" : "SHA-512";

            return new ScramCredential(
                    username,
                    saltBase64,
                    DEFAULT_ITERATIONS,
                    serverKeyBase64,
                    storedKeyBase64,
                    hashAlgorithm);
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Failed to generate SCRAM credential", e);
        }
    }

    /**
     * Generate a cryptographically random salt.
     *
     * @return the salt bytes
     */
    @NonNull
    private static byte[] generateSalt() {
        byte[] salt = new byte[SALT_LENGTH];
        new SecureRandom().nextBytes(salt);
        return salt;
    }
}
