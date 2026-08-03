/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.scram.credentialstore.keystore;

import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.HexFormat;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.apache.kafka.common.security.scram.internals.ScramFormatter;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;

import io.kroxylicious.scram.credentialstore.ScramCredential;
import io.kroxylicious.scram.credentialstore.ScramHashAlgorithm;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Utility for generating test keystores with SCRAM credentials.
 */
public class TestCredentialGenerator {

    private static final int DEFAULT_ITERATIONS = 10000;
    private static final int SALT_LENGTH = 20;
    private final SecureRandom secureRandom;

    public TestCredentialGenerator() {
        this(new SecureRandom());
    }

    public TestCredentialGenerator(SecureRandom secureRandom) {
        this.secureRandom = secureRandom;
    }

    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File path comes from trusted test configuration")
    public void generateKeyStore(
                                 Path outputPath,
                                 String storePassword,
                                 String... users)
            throws Exception {
        generateKeyStore(outputPath, storePassword, ScramMechanism.SCRAM_SHA_256, users);
    }

    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File path comes from trusted test configuration")
    public void generateKeyStore(
                                 Path outputPath,
                                 String storePassword,
                                 ScramMechanism mechanism,
                                 String... users)
            throws Exception {
        if (users.length % 2 != 0) {
            throw new IllegalArgumentException("users must contain alternating username/password pairs");
        }

        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        keyStore.load(null, storePassword.toCharArray());

        for (int i = 0; i < users.length; i += 2) {
            String username = users[i];
            String password = users[i + 1];

            ScramCredential credential = generateScramCredential(username, password, mechanism);

            ScramCredentialSerializer serializer = new ScramCredentialSerializer();
            byte[] credentialBytes = serializer.serialize(credential);

            SecretKey secretKey = new SecretKeySpec(credentialBytes, "AES");
            KeyStore.SecretKeyEntry entry = new KeyStore.SecretKeyEntry(secretKey);
            KeyStore.PasswordProtection protection = new KeyStore.PasswordProtection(storePassword.toCharArray());

            keyStore.setEntry(hashUsername(username), entry, protection);
        }

        try (FileOutputStream fos = new FileOutputStream(outputPath.toFile())) {
            keyStore.store(fos, storePassword.toCharArray());
        }
        KeystoreFilePermissions.setOwnerOnly(outputPath);
    }

    public ScramCredential generateScramCredential(
                                                   String username,
                                                   String password,
                                                   ScramMechanism mechanism) {
        try {
            byte[] salt = new byte[SALT_LENGTH];
            secureRandom.nextBytes(salt);

            ScramFormatter formatter = new ScramFormatter(mechanism);

            byte[] saltedPassword = formatter.saltedPassword(password, salt, DEFAULT_ITERATIONS);
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

    static String hashUsername(String username) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(username.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        }
        catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
