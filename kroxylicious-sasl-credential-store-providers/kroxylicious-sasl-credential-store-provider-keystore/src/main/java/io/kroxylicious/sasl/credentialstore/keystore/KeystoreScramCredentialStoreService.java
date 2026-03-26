/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sasl.credentialstore.keystore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.sasl.credentialstore.ScramCredentialStore;
import io.kroxylicious.sasl.credentialstore.ScramCredentialStoreService;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Service for creating KeyStore-based SCRAM credential stores.
 * <p>
 * This service loads credentials from a Java KeyStore file. The KeyStore should contain
 * {@link javax.crypto.SecretKey} entries where:
 * </p>
 * <ul>
 *     <li>The alias is the username</li>
 *     <li>The key bytes contain JSON-serialized {@link io.kroxylicious.sasl.credentialstore.ScramCredential} data</li>
 * </ul>
 *
 * <h2>Configuration</h2>
 * <pre>{@code
 * credentialStore: KeystoreScramCredentialStore
 * credentialStoreConfig:
 *   file: /path/to/credentials.jks
 *   storePassword:
 *     password: "keystore-password"
 *   storeType: PKCS12
 * }</pre>
 *
 * <h2>Lifecycle</h2>
 * <ol>
 *     <li>{@link #initialize(KeystoreScramCredentialStoreConfig)} - Load and validate configuration</li>
 *     <li>{@link #buildCredentialStore()} - Create credential store instances (may be called multiple times)</li>
 *     <li>{@link #close()} - Clean up resources (idempotent)</li>
 * </ol>
 */
@Plugin(configType = KeystoreScramCredentialStoreConfig.class)
public class KeystoreScramCredentialStoreService implements ScramCredentialStoreService<KeystoreScramCredentialStoreConfig> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KeystoreScramCredentialStoreService.class);

    @Nullable
    private KeystoreScramCredentialStoreConfig config;
    private boolean initialized = false;
    private boolean closed = false;

    @Override
    public void initialize(KeystoreScramCredentialStoreConfig config) {
        if (initialized) {
            throw new IllegalStateException("Service has already been initialized");
        }
        if (closed) {
            throw new IllegalStateException("Service has been closed");
        }

        this.config = config;
        this.initialized = true;

        LOGGER.info("Initialized KeyStore credential store service for file: {}", config.file());
    }

    @Override
    public ScramCredentialStore buildCredentialStore() {
        if (!initialized) {
            throw new IllegalStateException("Service has not been initialized");
        }
        if (closed) {
            throw new IllegalStateException("Service has been closed");
        }

        try {
            return new KeystoreScramCredentialStore(config);
        }
        catch (io.kroxylicious.sasl.credentialstore.CredentialServiceUnavailableException e) {
            throw new IllegalStateException("Failed to build credential store", e);
        }
    }

    @Override
    public void close() {
        if (closed) {
            return; // Idempotent
        }

        closed = true;
        LOGGER.info("Closed KeyStore credential store service");
    }
}
