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

/**
 * Configuration for SCRAM-SHA-256 and SCRAM-SHA-512 mechanisms.
 * <p>
 * Specifies the credential store plugin to use for looking up SCRAM credentials.
 * </p>
 *
 * <h2>Example Configuration</h2>
 * <pre>{@code
 * SCRAM-SHA-256:
 *   credentialStore: KeystoreScramCredentialStore
 *   credentialStoreConfig:
 *     file: /path/to/credentials.p12
 *     storePassword:
 *       password: "keystore-password"
 *     storeType: PKCS12
 * }</pre>
 *
 * @param credentialStore the plugin name of the credential store service
 * @param credentialStoreConfig the configuration for the credential store
 */
public record ScramMechanismConfig(
                                   @JsonProperty(required = true) @PluginImplName(ScramCredentialStoreService.class) String credentialStore,
                                   @JsonProperty(required = true) @PluginImplConfig(implNameProperty = "credentialStore") Object credentialStoreConfig)
        implements MechanismConfig {

    public ScramMechanismConfig {
        if (credentialStore == null || credentialStore.isEmpty()) {
            throw new IllegalArgumentException("credentialStore must not be null or empty");
        }
        if (credentialStoreConfig == null) {
            throw new IllegalArgumentException("credentialStoreConfig must not be null");
        }
    }
}
