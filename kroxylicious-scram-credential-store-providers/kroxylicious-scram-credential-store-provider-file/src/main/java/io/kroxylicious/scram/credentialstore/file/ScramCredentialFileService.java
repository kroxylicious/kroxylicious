/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.scram.credentialstore.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.scram.credentialstore.ScramCredentialStore;
import io.kroxylicious.scram.credentialstore.ScramCredentialStoreService;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Service for creating file-based SCRAM credential stores, backed by a Java KeyStore.
 * <p>
 * This service loads credentials from a proxy SCRAM credential file. The file should contain
 * {@link javax.crypto.SecretKey} entries where:
 * </p>
 * <ul>
 *     <li>The alias is the lowercase hex SHA-256 hash of the username (UTF-8 encoded)</li>
 *     <li>The key bytes contain JSON-serialized {@link io.kroxylicious.scram.credentialstore.ScramCredential} data</li>
 * </ul>
 *
 * <h2>Configuration</h2>
 * <pre>{@code
 * credentialStore: ScramCredentialFileService
 * credentialStoreConfig:
 *   file: /path/to/credentials.p12
 *   filePassword:
 *     passwordFile: /etc/kroxylicious/file-password.txt
 * }</pre>
 *
 * <h2>Lifecycle</h2>
 * <ol>
 *     <li>{@link #initialize(ScramCredentialFileConfig)} - Load and validate configuration</li>
 *     <li>{@link #buildCredentialStore()} - Create credential store instances (may be called multiple times)</li>
 *     <li>{@link #close()} - Clean up resources (idempotent)</li>
 * </ol>
 */
@Plugin(configType = ScramCredentialFileConfig.class)
public class ScramCredentialFileService implements ScramCredentialStoreService<ScramCredentialFileConfig> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScramCredentialFileService.class);

    @Nullable
    private ScramCredentialFileConfig config;
    private boolean initialized = false;
    private boolean closed = false;

    /**
     * Creates a new {@code ScramCredentialFileService}.
     */
    public ScramCredentialFileService() {
        // Default no-arg constructor required by the plugin framework
    }

    @Override
    public void initialize(ScramCredentialFileConfig config) {
        if (initialized) {
            throw new IllegalStateException("Service has already been initialized");
        }
        if (closed) {
            throw new IllegalStateException("Service has been closed");
        }

        this.config = config;
        this.initialized = true;

        LOGGER.atInfo().addKeyValue("file", config.file()).log("Initialized SCRAM credential file service");
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
            return new ScramCredentialFile(config);
        }
        catch (io.kroxylicious.scram.credentialstore.CredentialServiceUnavailableException e) {
            throw new IllegalStateException("Failed to build credential store", e);
        }
    }

    @Override
    public void close() {
        if (closed) {
            return; // Idempotent
        }

        closed = true;
        LOGGER.info("Closed SCRAM credential file service");
    }
}
