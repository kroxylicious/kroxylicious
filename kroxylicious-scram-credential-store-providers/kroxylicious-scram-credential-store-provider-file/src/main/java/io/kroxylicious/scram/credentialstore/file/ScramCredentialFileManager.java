/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.scram.credentialstore.file;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.util.ArrayList;
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
 * Utility for managing SCRAM credentials in a proxy SCRAM credential file (backed by a Java KeyStore).
 * <p>
 * Provides CRUD operations for managing users and credentials:
 * </p>
 * <ul>
 *     <li>Create new SCRAM credential files</li>
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
 * <strong>Concurrent Access:</strong> This utility assumes exclusive access to the SCRAM credential
 * file. It should not operate on a SCRAM credential file that is actively being used by a running
 * proxy.
 * </p>
 */
public class ScramCredentialFileManager {

    private static final int DEFAULT_ITERATIONS = 10000;
    private static final int SALT_LENGTH = 20;
    private static final int PHANTOM_SALT_KEY_LENGTH = 32;
    private static final int MIN_PASSWORD_LENGTH = 12;
    private static final int MAX_USERNAME_LENGTH = 255;
    static final String PHANTOM_SALT_KEY_ALIAS = "__kroxylicious_phantom_salt_key__";
    private final SecureRandom secureRandom;

    /**
     * Create a credential manager with a new {@link SecureRandom} instance.
     */
    public ScramCredentialFileManager() {
        this(new SecureRandom());
    }

    /**
     * Create a credential manager with the specified {@link SecureRandom}.
     *
     * @param secureRandom the random source to use for salt generation
     */
    public ScramCredentialFileManager(SecureRandom secureRandom) {
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
     * Create a new SCRAM credential file.
     * <p>
     * The file must not already exist.
     * </p>
     *
     * @param keystorePath path where the SCRAM credential file will be created
     * @param storePassword password for the file
     * @throws ScramCredentialFileException if creation fails or the file already exists
     */
    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File path comes from trusted configuration")
    public void createKeyStore(
                               Path keystorePath,
                               String storePassword) {
        validatePasswordLength(storePassword, "File password");
        try {
            KeyStore keyStore = KeyStore.getInstance("PKCS12");
            keyStore.load(null, storePassword.toCharArray());

            storePhantomSaltKey(keyStore, storePassword);

            CredentialFilePermissions.createExclusively(keystorePath);
            try (OutputStream os = Files.newOutputStream(keystorePath)) {
                keyStore.store(os, storePassword.toCharArray());
            }
        }
        catch (FileAlreadyExistsException e) {
            throw new ScramCredentialFileException("SCRAM credential file already exists: " + keystorePath, e);
        }
        catch (IOException | NoSuchAlgorithmException | CertificateException | KeyStoreException e) {
            throw new ScramCredentialFileException("Failed to create SCRAM credential file at " + keystorePath, e);
        }
    }

    /**
     * Add a user to an existing SCRAM credential file.
     * <p>
     * If the user already exists, their credential will be replaced.
     * </p>
     *
     * @param keystorePath path to the SCRAM credential file
     * @param storePassword password for the SCRAM credential file
     * @param username username to add
     * @param password plaintext password for the user
     * @param mechanism SCRAM mechanism (SCRAM-SHA-256 or SCRAM-SHA-512)
     * @throws ScramCredentialFileException if the operation fails
     */
    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File path comes from trusted configuration")
    public void addUser(
                        Path keystorePath,
                        String storePassword,
                        String username,
                        String password,
                        ScramMechanism mechanism) {
        addUser(keystorePath, storePassword, username, password, mechanism, DEFAULT_ITERATIONS);
    }

    /**
     * Add a user to an existing SCRAM credential file with an explicit iteration count.
     * <p>
     * If the user already exists, their credential will be replaced.
     * </p>
     *
     * @param keystorePath path to the SCRAM credential file
     * @param storePassword password for the SCRAM credential file
     * @param username username to add
     * @param password plaintext password for the user
     * @param mechanism SCRAM mechanism (SCRAM-SHA-256 or SCRAM-SHA-512)
     * @param iterations SCRAM iteration count
     * @throws ScramCredentialFileException if the operation fails
     */
    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File path comes from trusted configuration")
    public void addUser(
                        Path keystorePath,
                        String storePassword,
                        String username,
                        String password,
                        ScramMechanism mechanism,
                        int iterations) {
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
        catch (IOException | NoSuchAlgorithmException | CertificateException | KeyStoreException e) {
            throw new ScramCredentialFileException("Failed to add user '" + username + "' to SCRAM credential file", e);
        }
    }

    /**
     * Remove a user from the SCRAM credential file.
     *
     * @param keystorePath path to the SCRAM credential file
     * @param storePassword password for the SCRAM credential file
     * @param username username to remove
     * @throws ScramCredentialFileException if the operation fails
     */
    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File path comes from trusted configuration")
    public void removeUser(
                           Path keystorePath,
                           String storePassword,
                           String username) {
        try {
            KeyStore keyStore = loadKeyStore(keystorePath, storePassword);

            String alias = hashUsername(username);
            if (!keyStore.containsAlias(alias)) {
                throw new ScramCredentialFileException("User '" + username + "' not found in SCRAM credential file");
            }

            keyStore.deleteEntry(alias);

            saveKeyStore(keyStore, keystorePath, storePassword);
        }
        catch (IOException | NoSuchAlgorithmException | CertificateException | KeyStoreException e) {
            throw new ScramCredentialFileException("Failed to remove user '" + username + "' from SCRAM credential file", e);
        }
    }

    /**
     * Update a user's password.
     * <p>
     * This is implemented as a remove followed by an add operation.
     * </p>
     *
     * @param keystorePath path to the SCRAM credential file
     * @param storePassword password for the SCRAM credential file
     * @param username username to update
     * @param newPassword new plaintext password
     * @param mechanism SCRAM mechanism
     * @throws ScramCredentialFileException if the operation fails
     */
    public void updatePassword(
                               Path keystorePath,
                               String storePassword,
                               String username,
                               String newPassword,
                               ScramMechanism mechanism) {
        updatePassword(keystorePath, storePassword, username, newPassword, mechanism, DEFAULT_ITERATIONS);
    }

    /**
     * Update a user's password with an explicit iteration count.
     * <p>
     * This is implemented as a remove followed by an add operation.
     * </p>
     *
     * @param keystorePath path to the SCRAM credential file
     * @param storePassword password for the SCRAM credential file
     * @param username username to update
     * @param newPassword new plaintext password
     * @param mechanism SCRAM mechanism
     * @param iterations SCRAM iteration count
     * @throws ScramCredentialFileException if the operation fails
     */
    public void updatePassword(
                               Path keystorePath,
                               String storePassword,
                               String username,
                               String newPassword,
                               ScramMechanism mechanism,
                               int iterations) {
        validateUsername(username);
        validatePasswordLength(newPassword, "New password");

        // Verify user exists before attempting update
        try {
            KeyStore keyStore = loadKeyStore(keystorePath, storePassword);
            if (!keyStore.containsAlias(hashUsername(username))) {
                throw new ScramCredentialFileException("User '" + username + "' not found in SCRAM credential file");
            }
        }
        catch (IOException | NoSuchAlgorithmException | CertificateException | KeyStoreException e) {
            throw new ScramCredentialFileException("Failed to update password for user '" + username + "' in SCRAM credential file", e);
        }

        addUser(keystorePath, storePassword, username, newPassword, mechanism, iterations);
    }

    /**
     * List all usernames in the SCRAM credential file.
     *
     * @param keystorePath path to the SCRAM credential file
     * @param storePassword password for the SCRAM credential file
     * @return list of usernames (aliases) in the SCRAM credential file, sorted alphabetically
     * @throws ScramCredentialFileException if the operation fails
     */
    public List<String> listUsers(
                                  Path keystorePath,
                                  String storePassword) {
        return loadAllCredentials(keystorePath, storePassword).stream()
                .map(ScramCredential::username)
                .sorted()
                .toList();
    }

    /**
     * Credential metadata for a user stored in the SCRAM credential file.
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
     * List all credentials stored in the SCRAM credential file.
     *
     * @param keystorePath path to the SCRAM credential file
     * @param storePassword password for the SCRAM credential file
     * @return list of credential metadata, sorted by username
     * @throws ScramCredentialFileException if the operation fails
     */
    public List<UserCredentialInfo> listCredentials(
                                                    Path keystorePath,
                                                    String storePassword) {
        return loadAllCredentials(keystorePath, storePassword).stream()
                .map(c -> new UserCredentialInfo(
                        c.username(),
                        c.hashAlgorithm() == ScramHashAlgorithm.SHA_256 ? "SCRAM-SHA-256" : "SCRAM-SHA-512",
                        c.iterations()))
                .sorted()
                .toList();
    }

    /**
     * Load and deserialize all SCRAM credentials from the SCRAM credential file.
     */
    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File path comes from trusted configuration")
    private List<ScramCredential> loadAllCredentials(
                                                     Path keystorePath,
                                                     String storePassword) {
        try {
            KeyStore keyStore = loadKeyStore(keystorePath, storePassword);
            ScramCredentialSerializer serializer = new ScramCredentialSerializer();
            KeyStore.PasswordProtection protection = new KeyStore.PasswordProtection(storePassword.toCharArray());

            List<ScramCredential> credentials = new ArrayList<>();
            Enumeration<String> aliases = keyStore.aliases();
            while (aliases.hasMoreElements()) {
                String alias = aliases.nextElement();
                if (PHANTOM_SALT_KEY_ALIAS.equals(alias)) {
                    continue;
                }
                if (keyStore.isKeyEntry(alias)) {
                    KeyStore.SecretKeyEntry entry = (KeyStore.SecretKeyEntry) keyStore.getEntry(alias, protection);
                    credentials.add(serializer.deserialize(entry.getSecretKey().getEncoded(), alias));
                }
            }
            return credentials;
        }
        catch (IOException | NoSuchAlgorithmException | CertificateException | java.security.UnrecoverableEntryException | KeyStoreException e) {
            throw new ScramCredentialFileException("Failed to load credentials from SCRAM credential file", e);
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
     * Load the SCRAM credential file from disk, auto-detecting the underlying KeyStore type.
     */
    private KeyStore loadKeyStore(
                                  Path keystorePath,
                                  String storePassword)
            throws IOException, NoSuchAlgorithmException, CertificateException {

        if (!Files.exists(keystorePath)) {
            throw new ScramCredentialFileException("SCRAM credential file not found: " + keystorePath);
        }

        try {
            CredentialFilePermissions.checkForCredentialStore(keystorePath);
        }
        catch (CredentialServiceUnavailableException e) {
            throw new ScramCredentialFileException(e.getMessage(), e);
        }

        try {
            return KeyStore.getInstance(keystorePath.toFile(), storePassword.toCharArray());
        }
        catch (IOException e) {
            if (e.getCause() instanceof java.security.UnrecoverableKeyException) {
                throw new ScramCredentialFileException("Failed to open SCRAM credential file at " + keystorePath
                        + ": the most likely cause is an incorrect KeyStore password", e);
            }
            throw e;
        }
        catch (KeyStoreException e) {
            throw new ScramCredentialFileException("Failed to open SCRAM credential file at " + keystorePath, e);
        }
    }

    /**
     * Save the SCRAM credential file to disk.
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

    private void storePhantomSaltKey(KeyStore keyStore, String storePassword) throws KeyStoreException {
        byte[] keyBytes = new byte[PHANTOM_SALT_KEY_LENGTH];
        secureRandom.nextBytes(keyBytes);
        SecretKey phantomKey = new SecretKeySpec(keyBytes, "HmacSHA256");
        KeyStore.SecretKeyEntry entry = new KeyStore.SecretKeyEntry(phantomKey);
        KeyStore.PasswordProtection protection = new KeyStore.PasswordProtection(storePassword.toCharArray());
        keyStore.setEntry(PHANTOM_SALT_KEY_ALIAS, entry, protection);
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
