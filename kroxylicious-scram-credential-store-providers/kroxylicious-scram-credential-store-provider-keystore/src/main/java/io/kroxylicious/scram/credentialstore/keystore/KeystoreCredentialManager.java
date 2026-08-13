/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.scram.credentialstore.keystore;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HexFormat;
import java.util.List;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.apache.kafka.common.security.scram.internals.ScramFormatter;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;

import io.kroxylicious.scram.credentialstore.CredentialServiceUnavailableException;
import io.kroxylicious.scram.credentialstore.ScramCredential;
import io.kroxylicious.scram.credentialstore.ScramHashAlgorithm;

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

    private static final int DEFAULT_ITERATIONS = 10000;
    private static final int SALT_LENGTH = 20;
    private static final int MIN_PASSWORD_LENGTH = 12;
    private static final int MAX_USERNAME_LENGTH = 255;
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
     * Validate password meets minimum length requirements.
     * <p>
     * NIST SP 800-63B Rev 4 recommends minimum 12 characters for service credentials,
     * without composition rules (uppercase, digits, special characters).
     * </p>
     *
     * @param password password to validate
     * @param parameterName parameter name for error message
     * @throws CredentialValidationException if password is too short
     */
    private void validatePasswordLength(String password, String parameterName) {
        if (password.length() < MIN_PASSWORD_LENGTH) {
            throw new CredentialValidationException(
                    parameterName + " must be at least " + MIN_PASSWORD_LENGTH + " characters long. " +
                            "NIST recommends 12-15 characters minimum for service credentials. " +
                            "Consider using a passphrase (e.g., \"coffee-sunrise-laptop-2026\") " +
                            "or a password manager to generate a strong password.");
        }
    }

    private static void validateUsername(String username) {
        if (username == null || username.isEmpty()) {
            throw new CredentialValidationException("Username must not be null or empty");
        }
        if (username.length() > MAX_USERNAME_LENGTH) {
            throw new CredentialValidationException("Username must not exceed " + MAX_USERNAME_LENGTH + " characters");
        }
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
        validatePasswordLength(storePassword, "KeyStore password");
        try {
            KeyStore keyStore = KeyStore.getInstance(storeType);
            keyStore.load(null, storePassword.toCharArray());

            try (FileOutputStream fos = new FileOutputStream(keystorePath.toFile())) {
                keyStore.store(fos, storePassword.toCharArray());
            }
            KeystoreFilePermissions.setOwnerOnly(keystorePath);
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
        addUser(keystorePath, storePassword, username, password, mechanism, DEFAULT_ITERATIONS);
    }

    /**
     * Add a user to an existing KeyStore with an explicit iteration count.
     * <p>
     * If the user already exists, their credential will be replaced.
     * </p>
     *
     * @param keystorePath path to the KeyStore file
     * @param storePassword KeyStore password
     * @param username username to add
     * @param password plaintext password for the user
     * @param mechanism SCRAM mechanism (SCRAM-SHA-256 or SCRAM-SHA-512)
     * @param iterations SCRAM iteration count
     * @throws KeyStoreException if the operation fails
     */
    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File path comes from trusted configuration")
    public void addUser(
                        Path keystorePath,
                        String storePassword,
                        String username,
                        String password,
                        ScramMechanism mechanism,
                        int iterations)
            throws KeyStoreException {
        validateUsername(username);
        validatePasswordLength(password, "User password");

        try {
            KeyStore keyStore = loadKeyStore(keystorePath, storePassword);

            ScramCredential credential = generateScramCredential(username, password, mechanism, iterations);

            ScramCredentialSerializer serializer = new ScramCredentialSerializer();
            byte[] credentialBytes = serializer.serialize(credential);

            SecretKey secretKey = new SecretKeySpec(credentialBytes, "AES");
            KeyStore.SecretKeyEntry entry = new KeyStore.SecretKeyEntry(secretKey);
            KeyStore.PasswordProtection protection = new KeyStore.PasswordProtection(storePassword.toCharArray());

            keyStore.setEntry(hashUsername(username), entry, protection);

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

            String alias = hashUsername(username);
            if (!keyStore.containsAlias(alias)) {
                throw new KeyStoreException("User '" + username + "' not found in KeyStore");
            }

            keyStore.deleteEntry(alias);

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
        updatePassword(keystorePath, storePassword, username, newPassword, mechanism, DEFAULT_ITERATIONS);
    }

    /**
     * Update a user's password with an explicit iteration count.
     * <p>
     * This is implemented as a remove followed by an add operation.
     * </p>
     *
     * @param keystorePath path to the KeyStore file
     * @param storePassword KeyStore password
     * @param username username to update
     * @param newPassword new plaintext password
     * @param mechanism SCRAM mechanism
     * @param iterations SCRAM iteration count
     * @throws KeyStoreException if the operation fails
     */
    public void updatePassword(
                               Path keystorePath,
                               String storePassword,
                               String username,
                               String newPassword,
                               ScramMechanism mechanism,
                               int iterations)
            throws KeyStoreException {
        validateUsername(username);
        validatePasswordLength(newPassword, "New password");

        // Verify user exists before attempting update
        try {
            KeyStore keyStore = loadKeyStore(keystorePath, storePassword);
            if (!keyStore.containsAlias(hashUsername(username))) {
                throw new KeyStoreException("User '" + username + "' not found in KeyStore");
            }
        }
        catch (IOException | NoSuchAlgorithmException | CertificateException e) {
            throw new KeyStoreException("Failed to update password for user '" + username + "'", e);
        }

        addUser(keystorePath, storePassword, username, newPassword, mechanism, iterations);
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
            ScramCredentialSerializer serializer = new ScramCredentialSerializer();
            KeyStore.PasswordProtection protection = new KeyStore.PasswordProtection(storePassword.toCharArray());

            List<String> users = new ArrayList<>();
            Enumeration<String> aliases = keyStore.aliases();
            while (aliases.hasMoreElements()) {
                String alias = aliases.nextElement();
                if (keyStore.isKeyEntry(alias)) {
                    KeyStore.SecretKeyEntry entry = (KeyStore.SecretKeyEntry) keyStore.getEntry(alias, protection);
                    ScramCredential credential = serializer.deserialize(entry.getSecretKey().getEncoded(), alias);
                    users.add(credential.username());
                }
            }

            Collections.sort(users);
            return users;
        }
        catch (IOException | NoSuchAlgorithmException | CertificateException | java.security.UnrecoverableEntryException e) {
            throw new KeyStoreException("Failed to list users from KeyStore", e);
        }
    }

    /**
     * Credential metadata for a user stored in the KeyStore.
     *
     * @param username the username
     * @param mechanism the SCRAM mechanism (e.g. SCRAM-SHA-256)
     * @param iterations the SCRAM iteration count
     */
    public record UserCredentialInfo(String username, String mechanism, int iterations) implements Comparable<UserCredentialInfo> {
        @Override
        public int compareTo(UserCredentialInfo other) {
            return this.username.compareTo(other.username);
        }
    }

    /**
     * List all credentials stored in the KeyStore.
     *
     * @param keystorePath path to the KeyStore file
     * @param storePassword KeyStore password
     * @return list of credential metadata, sorted by username
     * @throws KeyStoreException if the operation fails
     */
    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File path comes from trusted configuration")
    public List<UserCredentialInfo> listCredentials(
                                                    Path keystorePath,
                                                    String storePassword)
            throws KeyStoreException {
        try {
            KeyStore keyStore = loadKeyStore(keystorePath, storePassword);
            ScramCredentialSerializer serializer = new ScramCredentialSerializer();
            KeyStore.PasswordProtection protection = new KeyStore.PasswordProtection(storePassword.toCharArray());

            List<UserCredentialInfo> credentials = new ArrayList<>();
            Enumeration<String> aliases = keyStore.aliases();
            while (aliases.hasMoreElements()) {
                String alias = aliases.nextElement();
                if (keyStore.isKeyEntry(alias)) {
                    KeyStore.SecretKeyEntry entry = (KeyStore.SecretKeyEntry) keyStore.getEntry(alias, protection);
                    ScramCredential credential = serializer.deserialize(entry.getSecretKey().getEncoded(), alias);
                    String mechanism = credential.hashAlgorithm() == ScramHashAlgorithm.SHA_256 ? "SCRAM-SHA-256" : "SCRAM-SHA-512";
                    credentials.add(new UserCredentialInfo(credential.username(), mechanism, credential.iterations()));
                }
            }

            Collections.sort(credentials);
            return credentials;
        }
        catch (IOException | NoSuchAlgorithmException | CertificateException | java.security.UnrecoverableEntryException e) {
            throw new KeyStoreException("Failed to list credentials from KeyStore", e);
        }
    }

    /**
     * Generate a KeyStore containing SCRAM credentials with specified mechanism.
     * <p>
     * Convenience method for creating a KeyStore with multiple users in one operation.
     * Primarily useful for testing.
     * </p>
     *
     * @param outputPath path where the KeyStore will be written
     * @param storePassword password for the KeyStore
     * @param mechanism the SCRAM mechanism to use
     * @param users array of username/password pairs (alternating username, password)
     * @throws Exception if generation fails
     */
    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File path comes from trusted configuration")
    public void generateKeyStore(
                                 Path outputPath,
                                 String storePassword,
                                 ScramMechanism mechanism,
                                 String... users)
            throws Exception {
        validatePasswordLength(storePassword, "KeyStore password");

        if (users.length % 2 != 0) {
            throw new CredentialValidationException("users must contain alternating username/password pairs");
        }

        // Validate all user passwords upfront
        for (int i = 1; i < users.length; i += 2) {
            validatePasswordLength(users[i], "User password for '" + users[i - 1] + "'");
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

    /**
     * Generate a KeyStore containing SCRAM credentials with separate key password.
     *
     * @param outputPath path where the KeyStore will be written
     * @param storePassword password for the KeyStore
     * @param keyPassword password for each individual key entry
     * @param mechanism the SCRAM mechanism to use
     * @param users array of username/password pairs (alternating username, password)
     * @throws KeyStoreException if the keystore type is not available
     * @throws NoSuchAlgorithmException if the SCRAM algorithm is not available
     * @throws CertificateException if a certificate in the keystore could not be loaded
     * @throws IOException if the keystore file cannot be written
     */
    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File path comes from trusted configuration")
    public void generateKeyStore(
                                 Path outputPath,
                                 String storePassword,
                                 String keyPassword,
                                 ScramMechanism mechanism,
                                 String... users)
            throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException {
        if (users.length % 2 != 0) {
            throw new CredentialValidationException("users must contain alternating username/password pairs");
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
            KeyStore.PasswordProtection protection = new KeyStore.PasswordProtection(keyPassword.toCharArray());

            keyStore.setEntry(hashUsername(username), entry, protection);
        }

        try (FileOutputStream fos = new FileOutputStream(outputPath.toFile())) {
            keyStore.store(fos, storePassword.toCharArray());
        }
        KeystoreFilePermissions.setOwnerOnly(outputPath);
    }

    /**
     * Generate a KeyStore containing SCRAM-SHA-256 credentials.
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
        generateKeyStore(outputPath, storePassword, ScramMechanism.SCRAM_SHA_256, users);
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
        return generateScramCredential(username, password, mechanism, DEFAULT_ITERATIONS);
    }

    /**
     * Generate a SCRAM credential with an explicit iteration count.
     *
     * @param username the username
     * @param password plaintext password
     * @param mechanism SCRAM mechanism
     * @param iterations SCRAM iteration count
     * @return the generated credential
     */
    public ScramCredential generateScramCredential(
                                                   String username,
                                                   String password,
                                                   ScramMechanism mechanism,
                                                   int iterations) {
        if (iterations < ScramCredential.MINIMUM_ITERATIONS) {
            throw new CredentialValidationException(
                    "Iteration count must be at least " + ScramCredential.MINIMUM_ITERATIONS);
        }

        try {
            byte[] salt = generateSalt();

            ScramFormatter formatter = new ScramFormatter(mechanism);

            byte[] saltedPassword = formatter.saltedPassword(password, salt, iterations);

            byte[] serverKey = formatter.serverKey(saltedPassword);
            byte[] clientKey = formatter.clientKey(saltedPassword);
            byte[] storedKey = formatter.storedKey(clientKey);

            ScramHashAlgorithm hashAlgorithm = mechanism == ScramMechanism.SCRAM_SHA_256 ? ScramHashAlgorithm.SHA_256 : ScramHashAlgorithm.SHA_512;

            return new ScramCredential(
                    username,
                    salt,
                    iterations,
                    serverKey,
                    storedKey,
                    hashAlgorithm);
        }
        catch (Exception e) {
            throw new CredentialValidationException("Failed to generate SCRAM credential", e);
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

        try {
            KeystoreFilePermissions.checkForCredentialStore(keystorePath);
        }
        catch (CredentialServiceUnavailableException e) {
            throw new KeyStoreException(e.getMessage(), e);
        }

        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try (FileInputStream fis = new FileInputStream(keystorePath.toFile())) {
            keyStore.load(fis, storePassword.toCharArray());
        }
        catch (IOException e) {
            if (e.getCause() instanceof java.security.UnrecoverableKeyException) {
                throw new KeyStoreException("Failed to open KeyStore at " + keystorePath
                        + ": the most likely cause is an incorrect KeyStore password", e);
            }
            throw e;
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

    /**
     * Compute a lowercase hex-encoded SHA-256 hash of a username for use as a KeyStore alias.
     *
     * @param username the username to hash
     * @return the hex-encoded SHA-256 hash
     */
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
