/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.scram.credentialstore.keystore;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableEntryException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.scram.credentialstore.CredentialServiceUnavailableException;
import io.kroxylicious.scram.credentialstore.ScramCredential;
import io.kroxylicious.scram.credentialstore.ScramCredentialStore;

import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * KeyStore-based implementation of {@link ScramCredentialStore}.
 * <p>
 * Loads all SCRAM credentials from a Java KeyStore file into memory at construction time.
 * Credentials are stored as {@link javax.crypto.SecretKey} entries with a hex-encoded SHA-256 hash of the
 * username as the alias. The original username is stored within the JSON payload.
 * </p>
 */
public class KeystoreScramCredentialStore implements ScramCredentialStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(KeystoreScramCredentialStore.class);
    private static final String ALIAS_LOG_KEY = "alias";

    private final Map<String, ScramCredential> credentialCache;
    private final ScramCredentialSerializer serializer;
    private final byte[] phantomSaltKey;

    /**
     * Create a new KeyStore-based credential store.
     *
     * @param config the configuration
     * @throws CredentialServiceUnavailableException if the KeyStore cannot be loaded
     */
    public KeystoreScramCredentialStore(KeystoreScramCredentialStoreConfig config) throws CredentialServiceUnavailableException {
        this.serializer = new ScramCredentialSerializer();
        var loaded = loadKeyStore(config);
        this.credentialCache = loaded.credentials;
        if (loaded.phantomSaltKey == null) {
            throw new CredentialServiceUnavailableException(
                    "KeyStore at " + config.file() + " does not contain a phantom salt key. Recreate the KeyStore using the CLI tool.");
        }
        this.phantomSaltKey = loaded.phantomSaltKey;
        LOGGER.atInfo()
                .addKeyValue("count", credentialCache.size())
                .addKeyValue("file", config.file())
                .log("Loaded SCRAM credentials from KeyStore");
    }

    @Override
    public CompletionStage<ScramCredential> lookupCredential(String username) {
        Objects.requireNonNull(username, "username must not be null");

        ScramCredential credential = credentialCache.get(username);
        return CompletableFuture.completedFuture(credential);
    }

    @Override
    public byte[] phantomSaltKey() {
        return phantomSaltKey.clone();
    }

    @SuppressWarnings("ArrayRecordComponent")
    private record LoadResult(Map<String, ScramCredential> credentials, @Nullable byte[] phantomSaltKey) {
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof LoadResult(Map<String, ScramCredential> thatCredentials, byte[] thatPhantomSaltKey))) {
                return false;
            }
            return Objects.equals(credentials, thatCredentials) && Arrays.equals(phantomSaltKey, thatPhantomSaltKey);
        }

        @Override
        public int hashCode() {
            return 31 * Objects.hashCode(credentials) + Arrays.hashCode(phantomSaltKey);
        }

        @Override
        public String toString() {
            return "LoadResult[credentials=" + credentials + ", phantomSaltKey=***]";
        }
    }

    /**
     * Load the KeyStore and extract all SCRAM credentials.
     *
     * @param config the configuration
     * @return loaded credentials and phantom salt key
     * @throws CredentialServiceUnavailableException if loading fails
     */
    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File path comes from trusted configuration")
    private LoadResult loadKeyStore(KeystoreScramCredentialStoreConfig config) throws CredentialServiceUnavailableException {
        KeystoreFilePermissions.checkForCredentialStore(Path.of(config.file()));
        try {
            KeyStore keyStore = KeyStore.getInstance("PKCS12");

            char[] storePassword = config.storePassword().getProvidedPassword().toCharArray();
            try {
                try (FileInputStream fis = new FileInputStream(config.file())) {
                    keyStore.load(fis, storePassword);
                }
                return extractCredentialsAndKey(keyStore, storePassword);
            }
            finally {
                Arrays.fill(storePassword, '\0');
            }
        }
        catch (KeyStoreException | IOException | NoSuchAlgorithmException | CertificateException e) {
            throw new CredentialServiceUnavailableException(
                    "Failed to load KeyStore from: " + config.file(), e);
        }
    }

    /**
     * Extract all SCRAM credentials and the phantom salt key from the KeyStore.
     */
    private LoadResult extractCredentialsAndKey(KeyStore keyStore, char[] storePassword) throws CredentialServiceUnavailableException {
        Map<String, ScramCredential> credentials = new HashMap<>();
        byte[] phantomKey = null;
        try {
            Enumeration<String> aliases = keyStore.aliases();
            while (aliases.hasMoreElements()) {
                String alias = aliases.nextElement();
                if (KeystoreCredentialManager.PHANTOM_SALT_KEY_ALIAS.equals(alias)) {
                    phantomKey = extractPhantomSaltKey(keyStore, alias, storePassword);
                    continue;
                }
                ScramCredential credential = extractCredential(keyStore, alias, storePassword);
                if (credential != null) {
                    credentials.put(credential.username(), credential);
                    LOGGER.atDebug().addKeyValue("username", credential.username()).log("Loaded credential");
                }
            }
            return new LoadResult(Collections.unmodifiableMap(credentials), phantomKey);
        }
        catch (KeyStoreException | NoSuchAlgorithmException e) {
            throw new CredentialServiceUnavailableException("Failed to extract credentials from KeyStore", e);
        }
    }

    @Nullable
    private byte[] extractPhantomSaltKey(KeyStore keyStore, String alias, char[] storePassword)
            throws KeyStoreException, NoSuchAlgorithmException, CredentialServiceUnavailableException {
        KeyStore.Entry entry = getKeystoreEntry(keyStore, alias, storePassword);
        if (entry instanceof KeyStore.SecretKeyEntry secretKeyEntry) {
            return secretKeyEntry.getSecretKey().getEncoded();
        }
        LOGGER.atWarn().addKeyValue(ALIAS_LOG_KEY, alias).log("Phantom salt key entry is not a SecretKeyEntry");
        return null;
    }

    @Nullable
    private ScramCredential extractCredential(KeyStore keyStore, String alias, char[] storePassword)
            throws KeyStoreException, NoSuchAlgorithmException, CredentialServiceUnavailableException {
        if (!keyStore.isKeyEntry(alias)) {
            LOGGER.atDebug().addKeyValue(ALIAS_LOG_KEY, alias).log("Skipping non-key entry");
            return null;
        }
        KeyStore.Entry entry = getKeystoreEntry(keyStore, alias, storePassword);
        if (!(entry instanceof KeyStore.SecretKeyEntry secretKeyEntry)) {
            LOGGER.atDebug().addKeyValue(ALIAS_LOG_KEY, alias).log("Skipping non-SecretKey entry");
            return null;
        }
        return deserializeCredential(secretKeyEntry, alias);
    }

    private KeyStore.Entry getKeystoreEntry(KeyStore keyStore, String alias, char[] storePassword)
            throws KeyStoreException, NoSuchAlgorithmException, CredentialServiceUnavailableException {
        try {
            return keyStore.getEntry(alias, new KeyStore.PasswordProtection(storePassword));
        }
        catch (UnrecoverableEntryException e) {
            throw new CredentialServiceUnavailableException(
                    "Failed to recover KeyStore entry for alias '" + alias + "' - incorrect store password?", e);
        }
    }

    private ScramCredential deserializeCredential(KeyStore.SecretKeyEntry secretKeyEntry, String alias)
            throws CredentialServiceUnavailableException {
        byte[] credentialBytes = secretKeyEntry.getSecretKey().getEncoded();
        try {
            return serializer.deserialize(credentialBytes, alias);
        }
        catch (IllegalArgumentException e) {
            throw new CredentialServiceUnavailableException(
                    "Malformed credential in KeyStore entry for alias '" + alias + "'", e);
        }
    }
}
