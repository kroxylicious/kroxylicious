/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.plugin.PluginImplConfig;
import io.kroxylicious.proxy.plugin.PluginImplName;
import io.kroxylicious.scram.credentialstore.ScramCredentialStoreService;

/**
 * Shared configuration interface for SCRAM mechanism variants.
 *
 * @param mechanismName the SASL mechanism name (e.g. {@code SCRAM-SHA-256})
 * @param credentialStore plugin name for the credential store service
 * @param credentialStoreConfig configuration for the credential store plugin
 */
public record ScramMechanismConfig(
                                   @JsonProperty(value = "mechanism", required = true) String mechanismName,
                                   @JsonProperty(required = true) @PluginImplName(ScramCredentialStoreService.class) String credentialStore,
                                   @JsonProperty(required = true) @PluginImplConfig(implNameProperty = "credentialStore") Object credentialStoreConfig)
        implements MechanismConfig {

    public static final String MECHANISM_NAME_SCRAM_SHA_256 = "SCRAM-SHA-256";
    public static final String MECHANISM_NAME_SCRAM_SHA_512 = "SCRAM-SHA-512";

    /** Validates that credential store settings are present. */
    public ScramMechanismConfig {
        if (credentialStore == null || credentialStore.isEmpty()) {
            throw new IllegalArgumentException("credentialStore must not be null or empty");
        }
        if (credentialStoreConfig == null) {
            throw new IllegalArgumentException("credentialStoreConfig must not be null");
        }
    }

}
