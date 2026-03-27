/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sasl.credentialstore.keystore;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.apache.kafka.common.security.scram.internals.ScramFormatter;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;

import io.kroxylicious.sasl.credentialstore.ScramCredential;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Utility for managing SCRAM credentials in Java KeyStore files.
 * <p>
 * Provides CRUD operations for managing users and credentials:
 * </p>
 * <ul>
 *     <li>Create new KeyStore files</li>
 *     <li>Add users with credentials</li>
 *     <li>Remove users</li>
 *     <li>Update user passwords</li>
 *     <li>List users</li>
 * </ul>
 * <p>
 * Uses Kafka's {@link ScramFormatter} to generate properly salted and hashed credentials
 * following the SCRAM specification (RFC 5802).
 * </p>
 * <p>
 * <strong>Thread Safety:</strong> Not thread-safe. Create new instances per operation.
 * </p>
 * <p>
 * <strong>Concurrent Access:</strong> This utility assumes exclusive access to the KeyStore file.
 * It should not operate on a KeyStore file that is actively being used by a running proxy.
 * </p>
 */
public class KeystoreCredentialManager {

    private static final int DEFAULT_ITERATIONS = 4096;
    private static final int SALT_LENGTH = 20;
    private final SecureRandom secureRandom;

    /**
     * Create a credential manager with a new {@link SecureRandom} instance.
     */
    public KeystoreCredentialManager() {
        this(new SecureRandom());
    }

    /**
     * Create a credential manager with the specified {@link SecureRandom}.
     *
     * @param secureRandom the random source to use for salt generation
     */
    public KeystoreCredentialManager(SecureRandom secureRandom) {
        this.secureRandom = secureRandom;
    }

    /**
     * Create a new KeyStore file.
     * <p>
     * If the file already exists, it will be overwritten.
     * </p>
     *
     * @param keystorePath path where the KeyStore will be created
     * @param storePassword password for the KeyStore
     * @param storeType KeyStore type (e.g., "PKCS12", "JKS")
     * @throws KeyStoreException if KeyStore creation fails
     */
    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File path comes from trusted configuration")
    public void createKeyStore(
                               Path keystorePath,
                               String storePassword,
                               String storeType)
            throws KeyStoreException {
        try {
            KeyStore keyStore = KeyStore.getInstance(storeType);
            keyStore.load(null, storePassword.toCharArray());

            try (FileOutputStream fos = new FileOutputStream(keystorePath.toFile())) {
                keyStore.store(fos, storePassword.toCharArray());
            }
        }
        catch (IOException | NoSuchAlgorithmException | CertificateException e) {
            throw new KeyStoreException("Failed to create KeyStore at " + keystorePath, e);
        }
    }

    /**
     * Add a user to an existing KeyStore.
     * <p>
     * If the user already exists, their credential will be replaced.
     * </p>
     *
     * @param keystorePath path to the KeyStore file
     * @param storePassword KeyStore password
     * @param username username to add
     * @param password plaintext password for the user
     * @param mechanism SCRAM mechanism (SCRAM-SHA-256 or SCRAM-SHA-512)
     * @throws KeyStoreException if the operation fails
     */
    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File path comes from trusted configuration")
    public void addUser(
                        Path keystorePath,
                        String storePassword,
                        String username,
                        String password,
                        ScramMechanism mechanism)
            throws KeyStoreException {

        try {
            KeyStore keyStore = loadKeyStore(keystorePath, storePassword);

            ScramCredential credential = generateScramCredential(username, password, mechanism);

            ScramCredentialSerializer serializer = new ScramCredentialSerializer();
            byte[] credentialBytes = serializer.serialize(credential);

            SecretKey secretKey = new SecretKeySpec(credentialBytes, "AES");
            KeyStore.SecretKeyEntry entry = new KeyStore.SecretKeyEntry(secretKey);
            KeyStore.PasswordProtection protection = new KeyStore.PasswordProtection(storePassword.toCharArray());

            keyStore.setEntry(username, entry, protection);

            saveKeyStore(keyStore, keystorePath, storePassword);
        }
        catch (Exception e) {
            throw new KeyStoreException("Failed to add user '" + username + "' to KeyStore", e);
        }
    }

    /**
     * Remove a user from the KeyStore.
     *
     * @param keystorePath path to the KeyStore file
     * @param storePassword KeyStore password
     * @param username username to remove
     * @throws KeyStoreException if the operation fails
     */
    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File path comes from trusted configuration")
    public void removeUser(
                           Path keystorePath,
                           String storePassword,
                           String username)
            throws KeyStoreException {

        try {
            KeyStore keyStore = loadKeyStore(keystorePath, storePassword);

            if (!keyStore.containsAlias(username)) {
                throw new KeyStoreException("User '" + username + "' not found in KeyStore");
            }

            keyStore.deleteEntry(username);

            saveKeyStore(keyStore, keystorePath, storePassword);
        }
        catch (IOException | NoSuchAlgorithmException | CertificateException e) {
            throw new KeyStoreException("Failed to remove user '" + username + "' from KeyStore", e);
        }
    }

    /**
     * Update a user's password.
     * <p>
     * This is implemented as a remove followed by an add operation.
     * </p>
     *
     * @param keystorePath path to the KeyStore file
     * @param storePassword KeyStore password
     * @param username username to update
     * @param newPassword new plaintext password
     * @param mechanism SCRAM mechanism
     * @throws KeyStoreException if the operation fails
     */
    public void updatePassword(
                               Path keystorePath,
                               String storePassword,
                               String username,
                               String newPassword,
                               ScramMechanism mechanism)
            throws KeyStoreException {

        // Verify user exists before attempting update
        try {
            KeyStore keyStore = loadKeyStore(keystorePath, storePassword);
            if (!keyStore.containsAlias(username)) {
                throw new KeyStoreException("User '" + username + "' not found in KeyStore");
            }
        }
        catch (IOException | NoSuchAlgorithmException | CertificateException e) {
            throw new KeyStoreException("Failed to update password for user '" + username + "'", e);
        }

        // Update is implemented as remove + add
        addUser(keystorePath, storePassword, username, newPassword, mechanism);
    }

    /**
     * List all usernames in the KeyStore.
     *
     * @param keystorePath path to the KeyStore file
     * @param storePassword KeyStore password
     * @return list of usernames (aliases) in the KeyStore
     * @throws KeyStoreException if the operation fails
     */
    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File path comes from trusted configuration")
    public List<String> listUsers(
                                  Path keystorePath,
                                  String storePassword)
            throws KeyStoreException {

        try {
            KeyStore keyStore = loadKeyStore(keystorePath, storePassword);

            List<String> users = new ArrayList<>();
            Enumeration<String> aliases = keyStore.aliases();
            while (aliases.hasMoreElements()) {
                String alias = aliases.nextElement();
                // Only include key entries (not certificate entries)
                if (keyStore.isKeyEntry(alias)) {
                    users.add(alias);
                }
            }

            Collections.sort(users);
            return users;
        }
        catch (IOException | NoSuchAlgorithmException | CertificateException e) {
            throw new KeyStoreException("Failed to list users from KeyStore", e);
        }
    }

    /**
     * Generate a KeyStore containing SCRAM credentials.
     * <p>
     * Convenience method for creating a KeyStore with multiple users in one operation.
     * Primarily useful for testing.
     * </p>
     *
     * @param outputPath path where the KeyStore will be written
     * @param storePassword password for the KeyStore
     * @param users array of username/password pairs (alternating username, password)
     * @throws Exception if generation fails
     */
    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File path comes from trusted configuration")
    public void generateKeyStore(
                                 Path outputPath,
                                 String storePassword,
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

            ScramCredential credential = generateScramCredential(username, password, ScramMechanism.SCRAM_SHA_256);

            ScramCredentialSerializer serializer = new ScramCredentialSerializer();
            byte[] credentialBytes = serializer.serialize(credential);

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
    public ScramCredential generateScramCredential(
                                                   String username,
                                                   String password,
                                                   ScramMechanism mechanism) {

        try {
            byte[] salt = generateSalt();

            ScramFormatter formatter = new ScramFormatter(mechanism);

            // Generate the salted password
            byte[] saltedPassword = formatter.saltedPassword(password, salt, DEFAULT_ITERATIONS);

            // Generate server key and stored key from the salted password
            byte[] serverKey = formatter.serverKey(saltedPassword);
            byte[] clientKey = formatter.clientKey(saltedPassword);
            byte[] storedKey = formatter.storedKey(clientKey);

            String hashAlgorithm = mechanism == ScramMechanism.SCRAM_SHA_256 ? "SHA-256" : "SHA-512";

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
     * Load a KeyStore from disk.
     */
    private KeyStore loadKeyStore(
                                  Path keystorePath,
                                  String storePassword)
            throws IOException, KeyStoreException, NoSuchAlgorithmException, CertificateException {

        if (!Files.exists(keystorePath)) {
            throw new KeyStoreException("KeyStore file not found: " + keystorePath);
        }

        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try (FileInputStream fis = new FileInputStream(keystorePath.toFile())) {
            keyStore.load(fis, storePassword.toCharArray());
        }
        return keyStore;
    }

    /**
     * Save a KeyStore to disk.
     */
    private void saveKeyStore(
                              KeyStore keyStore,
                              Path keystorePath,
                              String storePassword)
            throws IOException, KeyStoreException, NoSuchAlgorithmException, CertificateException {

        try (FileOutputStream fos = new FileOutputStream(keystorePath.toFile())) {
            keyStore.store(fos, storePassword.toCharArray());
        }
    }

    /**
     * Generate a cryptographically random salt.
     *
     * @return the salt bytes
     */
    private byte[] generateSalt() {
        byte[] salt = new byte[SALT_LENGTH];
        secureRandom.nextBytes(salt);
        return salt;
    }
}
