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
 * Configuration for the SCRAM-SHA-256 mechanism.
 *
 * @param credentialStore the plugin name of the credential store service
 * @param credentialStoreConfig the configuration for the credential store
 */
public record ScramSha256MechanismConfig(
                                         @JsonProperty(required = true) @PluginImplName(ScramCredentialStoreService.class) String credentialStore,
                                         @JsonProperty(required = true) @PluginImplConfig(implNameProperty = "credentialStore") Object credentialStoreConfig)
        implements ScramMechanismConfig {

    private static final String MECHANISM_NAME = "SCRAM-SHA-256";

    public ScramSha256MechanismConfig {
        if (credentialStore == null || credentialStore.isEmpty()) {
            throw new IllegalArgumentException("credentialStore must not be null or empty");
        }
        if (credentialStoreConfig == null) {
            throw new IllegalArgumentException("credentialStoreConfig must not be null");
        }
    }

    @Override
    public String mechanismName() {
        return MECHANISM_NAME;
    }
}
