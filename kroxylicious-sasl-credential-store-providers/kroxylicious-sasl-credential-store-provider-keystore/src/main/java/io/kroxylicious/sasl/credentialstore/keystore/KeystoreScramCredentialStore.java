/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sasl.credentialstore.keystore;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableEntryException;
import java.security.cert.CertificateException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.crypto.SecretKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.sasl.credentialstore.CredentialServiceUnavailableException;
import io.kroxylicious.sasl.credentialstore.ScramCredential;
import io.kroxylicious.sasl.credentialstore.ScramCredentialStore;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * KeyStore-based implementation of {@link ScramCredentialStore}.
 * <p>
 * Loads all SCRAM credentials from a Java KeyStore file into memory at construction time.
 * Credentials are stored as {@link SecretKey} entries with the username as the alias.
 * </p>
 * <p>
 * The entire KeyStore is loaded into memory for fast lookups. File watching for dynamic
 * updates is not supported in this version.
 * </p>
 */
public class KeystoreScramCredentialStore implements ScramCredentialStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(KeystoreScramCredentialStore.class);

    private final Map<String, ScramCredential> credentialCache;
    private final ScramCredentialSerializer serializer;

    /**
     * Create a new KeyStore-based credential store.
     *
     * @param config the configuration
     * @throws CredentialServiceUnavailableException if the KeyStore cannot be loaded
     */
    public KeystoreScramCredentialStore(KeystoreScramCredentialStoreConfig config) throws CredentialServiceUnavailableException {
        this.serializer = new ScramCredentialSerializer();
        this.credentialCache = loadKeyStore(config);
        LOGGER.info("Loaded {} SCRAM credentials from KeyStore: {}", credentialCache.size(), config.file());
    }

    @Override
    public CompletionStage<ScramCredential> lookupCredential(String username) {
        if (username == null) {
            throw new NullPointerException("username must not be null");
        }

        ScramCredential credential = credentialCache.get(username);
        return CompletableFuture.completedFuture(credential);
    }

    /**
     * Load the KeyStore and extract all SCRAM credentials.
     *
     * @param config the configuration
     * @return map of username to credential
     * @throws CredentialServiceUnavailableException if loading fails
     */
    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File path comes from trusted configuration")
    private Map<String, ScramCredential> loadKeyStore(KeystoreScramCredentialStoreConfig config) throws CredentialServiceUnavailableException {
        try {
            KeyStore keyStore = KeyStore.getInstance(config.effectiveStoreType());

            char[] storePassword = config.storePassword().getProvidedPassword().toCharArray();
            char[] keyPassword = config.effectiveKeyPassword().getProvidedPassword().toCharArray();

            try (FileInputStream fis = new FileInputStream(config.file())) {
                keyStore.load(fis, storePassword);
            }

            return extractCredentials(keyStore, keyPassword);
        }
        catch (KeyStoreException | IOException | NoSuchAlgorithmException | CertificateException e) {
            throw new CredentialServiceUnavailableException(
                    "Failed to load KeyStore from: " + config.file(), e);
        }
    }

    /**
     * Extract all SCRAM credentials from the KeyStore.
     *
     * @param keyStore the loaded KeyStore
     * @param keyPassword the password for individual keys
     * @return map of username to credential
     * @throws CredentialServiceUnavailableException if extraction fails
     */
    private Map<String, ScramCredential> extractCredentials(KeyStore keyStore, char[] keyPassword) throws CredentialServiceUnavailableException {
        Map<String, ScramCredential> credentials = new HashMap<>();
        int skippedEntries = 0;

        try {
            Enumeration<String> aliases = keyStore.aliases();
            while (aliases.hasMoreElements()) {
                String alias = aliases.nextElement();

                if (!keyStore.isKeyEntry(alias)) {
                    LOGGER.debug("Skipping non-key entry: {}", alias);
                    skippedEntries++;
                    continue;
                }

                try {
                    KeyStore.Entry entry = keyStore.getEntry(alias, new KeyStore.PasswordProtection(keyPassword));

                    if (!(entry instanceof KeyStore.SecretKeyEntry secretKeyEntry)) {
                        LOGGER.debug("Skipping non-SecretKey entry: {}", alias);
                        skippedEntries++;
                        continue;
                    }

                    SecretKey secretKey = secretKeyEntry.getSecretKey();
                    byte[] credentialBytes = secretKey.getEncoded();

                    ScramCredential credential = serializer.deserialize(credentialBytes, alias);
                    credentials.put(credential.username(), credential);

                    LOGGER.debug("Loaded credential for user: {}", credential.username());
                }
                catch (UnrecoverableEntryException e) {
                    LOGGER.warn("Failed to recover entry for alias: {} - incorrect password?", alias, e);
                    skippedEntries++;
                }
                catch (IllegalArgumentException e) {
                    LOGGER.warn("Failed to deserialize credential for alias: {}", alias, e);
                    skippedEntries++;
                }
            }

            if (skippedEntries > 0) {
                LOGGER.info("Skipped {} KeyStore entries (non-SecretKey or invalid credentials)", skippedEntries);
            }

            return Collections.unmodifiableMap(credentials);
        }
        catch (KeyStoreException | NoSuchAlgorithmException e) {
            throw new CredentialServiceUnavailableException("Failed to extract credentials from KeyStore", e);
        }
    }
}
