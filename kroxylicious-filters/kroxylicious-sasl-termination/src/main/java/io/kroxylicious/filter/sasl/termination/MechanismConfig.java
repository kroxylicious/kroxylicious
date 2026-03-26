/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.plugin.PluginImplConfig;
import io.kroxylicious.proxy.plugin.PluginImplName;
import io.kroxylicious.sasl.credentialstore.ScramCredentialStoreService;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Configuration for a single SASL mechanism.
 * <p>
 * Specifies the credential store to use for this mechanism and its configuration.
 * </p>
 *
 * <h2>Example Configuration</h2>
 * <pre>{@code
 * credentialStore: KeystoreScramCredentialStore
 * credentialStoreConfig:
 *   file: /path/to/credentials.jks
 *   storePassword:
 *     password: "keystore-password"
 *   storeType: PKCS12
 * }</pre>
 *
 * @param credentialStore the plugin name of the credential store service
 * @param credentialStoreConfig the configuration for the credential store
 */
public record MechanismConfig(
                              @JsonProperty(required = true) @PluginImplName(ScramCredentialStoreService.class) @NonNull String credentialStore,
                              @JsonProperty(required = true) @PluginImplConfig(implNameProperty = "credentialStore") @NonNull Object credentialStoreConfig) {

    /**
     * Canonical constructor with validation.
     */
    public MechanismConfig {
        if (credentialStore == null || credentialStore.isEmpty()) {
            throw new IllegalArgumentException("credentialStore must not be null or empty");
        }
        if (credentialStoreConfig == null) {
            throw new IllegalArgumentException("credentialStoreConfig must not be null");
        }
    }
}
