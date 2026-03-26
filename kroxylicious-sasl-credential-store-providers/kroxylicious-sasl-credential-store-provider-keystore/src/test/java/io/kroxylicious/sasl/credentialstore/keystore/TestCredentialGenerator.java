/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sasl.credentialstore.keystore;

import java.io.FileOutputStream;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.Base64;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.apache.kafka.common.security.scram.internals.ScramFormatter;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;

import io.kroxylicious.sasl.credentialstore.ScramCredential;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Utility for generating test SCRAM credentials and KeyStore files.
 * <p>
 * Uses Kafka's {@link ScramFormatter} to generate properly salted and hashed credentials
 * following the SCRAM specification.
 * </p>
 */
public class TestCredentialGenerator {

    private static final int DEFAULT_ITERATIONS = 4096;
    private static final int SALT_LENGTH = 20;

    /**
     * Generate a KeyStore containing SCRAM credentials for testing.
     *
     * @param outputPath path where the KeyStore will be written
     * @param storePassword password for the KeyStore
     * @param users array of username/password pairs (alternating username, password)
     * @throws Exception if generation fails
     */
    public static void generateKeyStore(
                                        @NonNull Path outputPath,
                                        @NonNull String storePassword,
                                        @NonNull String... users)
            throws Exception {

        if (users.length % 2 != 0) {
            throw new IllegalArgumentException("users must contain alternating username/password pairs");
        }

        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        keyStore.load(null, storePassword.toCharArray());

        for (int i = 0; i < users.length; i += 2) {
            String username = users[i];
            String password = users[i + 1];

            ScramCredential credential = generateScramCredential(username, password, ScramMechanism.SCRAM_SHA_256);

            ScramCredentialSerializer serializer = new ScramCredentialSerializer();
            byte[] credentialBytes = serializer.serialize(credential);

            // Use AES as the algorithm for the SecretKey (PKCS12 requires a proper algorithm, not RAW)
            SecretKey secretKey = new SecretKeySpec(credentialBytes, "AES");
            KeyStore.SecretKeyEntry entry = new KeyStore.SecretKeyEntry(secretKey);
            KeyStore.PasswordProtection protection = new KeyStore.PasswordProtection(storePassword.toCharArray());

            keyStore.setEntry(username, entry, protection);
        }

        try (FileOutputStream fos = new FileOutputStream(outputPath.toFile())) {
            keyStore.store(fos, storePassword.toCharArray());
        }
    }

    /**
     * Generate a SCRAM credential for a user.
     *
     * @param username the username
     * @param password the plaintext password
     * @param mechanism the SCRAM mechanism
     * @return the generated credential
     */
    @NonNull
    public static ScramCredential generateScramCredential(
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
