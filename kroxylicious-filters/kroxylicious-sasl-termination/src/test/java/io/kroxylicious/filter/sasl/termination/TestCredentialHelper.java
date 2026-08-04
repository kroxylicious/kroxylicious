/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import java.security.SecureRandom;

import org.apache.kafka.common.security.scram.internals.ScramFormatter;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;

import io.kroxylicious.scram.credentialstore.ScramCredential;
import io.kroxylicious.scram.credentialstore.ScramHashAlgorithm;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Test helper for generating SCRAM credentials.
 */
public class TestCredentialHelper {

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
    public static ScramCredential generateCredential(
                                                     @NonNull String username,
                                                     @NonNull String password,
                                                     @NonNull ScramMechanism mechanism) {

        try {
            byte[] salt = generateSalt();

            ScramFormatter formatter = new ScramFormatter(mechanism);

            // Generate the salted password
            byte[] saltedPassword = formatter.saltedPassword(password, salt, DEFAULT_ITERATIONS);

            // Generate server key and stored key from the salted password
            byte[] serverKey = formatter.serverKey(saltedPassword);
            byte[] clientKey = formatter.clientKey(saltedPassword);
            byte[] storedKey = formatter.storedKey(clientKey);

            ScramHashAlgorithm hashAlgorithm = mechanism == ScramMechanism.SCRAM_SHA_256 ? ScramHashAlgorithm.SHA_256 : ScramHashAlgorithm.SHA_512;

            return new ScramCredential(
                    username,
                    salt,
                    DEFAULT_ITERATIONS,
                    serverKey,
                    storedKey,
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
