/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.scram.credentialstore.file;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.apache.kafka.common.security.scram.internals.ScramMechanism;

import io.kroxylicious.scram.credentialstore.ScramCredential;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Test utility for generating KeyStore files containing SCRAM credentials.
 * <p>
 * Convenience for creating a KeyStore with multiple users in one operation,
 * without the safety checks of {@link ScramCredentialFileManager#createKeyStore}.
 * </p>
 */
public class TestCredentialGenerator {

    private final ScramCredentialFileManager credentialManager;

    public TestCredentialGenerator() {
        this.credentialManager = new ScramCredentialFileManager();
    }

    public TestCredentialGenerator(ScramCredentialFileManager credentialManager) {
        this.credentialManager = credentialManager;
    }

    /**
     * Generate a KeyStore containing SCRAM credentials with specified mechanism.
     *
     * @param outputPath path where the KeyStore will be written
     * @param storePassword password for the KeyStore and each individual key entry
     * @param mechanism the SCRAM mechanism to use
     * @param users array of username/password pairs (alternating username, password)
     * @throws KeyStoreException if the keystore type is not available
     * @throws NoSuchAlgorithmException if the SCRAM algorithm is not available
     * @throws CertificateException if a certificate in the keystore could not be loaded
     * @throws IOException if the keystore file cannot be written
     */
    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File path comes from test configuration")
    public void generateKeyStore(
                                 Path outputPath,
                                 String storePassword,
                                 ScramMechanism mechanism,
                                 String... users)
            throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException {
        if (users.length % 2 != 0) {
            throw new IllegalArgumentException("users must contain alternating username/password pairs");
        }

        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        keyStore.load(null, storePassword.toCharArray());

        byte[] phantomKeyBytes = new byte[32];
        new SecureRandom().nextBytes(phantomKeyBytes);
        SecretKey phantomKey = new SecretKeySpec(phantomKeyBytes, "HmacSHA256");
        keyStore.setEntry(ScramCredentialFileManager.PHANTOM_SALT_KEY_ALIAS,
                new KeyStore.SecretKeyEntry(phantomKey),
                new KeyStore.PasswordProtection(storePassword.toCharArray()));

        for (int i = 0; i < users.length; i += 2) {
            String username = users[i];
            String password = users[i + 1];

            ScramCredential credential = credentialManager.generateScramCredential(username, password, mechanism);

            ScramCredentialSerializer serializer = new ScramCredentialSerializer();
            byte[] credentialBytes = serializer.serialize(credential);

            SecretKey secretKey = new SecretKeySpec(credentialBytes, "AES");
            KeyStore.SecretKeyEntry entry = new KeyStore.SecretKeyEntry(secretKey);
            KeyStore.PasswordProtection protection = new KeyStore.PasswordProtection(storePassword.toCharArray());

            keyStore.setEntry(ScramCredentialFileManager.hashUsername(username), entry, protection);
        }

        CredentialFilePermissions.ensureOwnerOnlyBeforeWrite(outputPath);
        try (FileOutputStream fos = new FileOutputStream(outputPath.toFile())) {
            keyStore.store(fos, storePassword.toCharArray());
        }
    }
}
