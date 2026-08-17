/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.plugin.PluginImplConfig;
import io.kroxylicious.proxy.plugin.PluginImplName;
import io.kroxylicious.scram.credentialstore.ScramCredential;
import io.kroxylicious.scram.credentialstore.ScramCredentialStoreService;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Shared configuration interface for SCRAM mechanism variants.
 *
 * @param mechanismName the SASL mechanism name (e.g. {@code SCRAM-SHA-256})
 * @param credentialStore plugin name for the credential store service
 * @param credentialStoreConfig configuration for the credential store plugin
 * @param phantomIterations PBKDF2 iteration count for phantom user challenges, should match real credential iterations to prevent enumeration
 */
public record ScramMechanismConfig(
                                   @JsonProperty(value = "mechanism", required = true) String mechanismName,
                                   @JsonProperty(required = true) @PluginImplName(ScramCredentialStoreService.class) String credentialStore,
                                   @JsonProperty(required = true) @PluginImplConfig(implNameProperty = "credentialStore") Object credentialStoreConfig,
                                   @JsonProperty @Nullable Integer phantomIterations)
        implements MechanismConfig {

    public static final String MECHANISM_NAME_SCRAM_SHA_256 = "SCRAM-SHA-256";
    public static final String MECHANISM_NAME_SCRAM_SHA_512 = "SCRAM-SHA-512";
    static final int DEFAULT_PHANTOM_ITERATIONS = 10000;

    /** Validates that credential store settings are present. */
    public ScramMechanismConfig {
        if (credentialStore == null || credentialStore.isEmpty()) {
            throw new IllegalArgumentException("credentialStore must not be null or empty");
        }
        if (credentialStoreConfig == null) {
            throw new IllegalArgumentException("credentialStoreConfig must not be null");
        }
        if (phantomIterations != null && phantomIterations < ScramCredential.MINIMUM_ITERATIONS) {
            throw new IllegalArgumentException(
                    "phantomIterations must be at least " + ScramCredential.MINIMUM_ITERATIONS + ", got: " + phantomIterations);
        }
    }

    /**
     * Returns the configured phantom iterations, or the default (10000) if not set.
     */
    int effectivePhantomIterations() {
        return phantomIterations != null ? phantomIterations : DEFAULT_PHANTOM_ITERATIONS;
    }

}
