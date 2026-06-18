/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration for the SASL termination filter.
 * <p>
 * Defines the SASL mechanisms supported by the filter and their associated
 * credential stores.
 * </p>
 *
 * <h2>Example Configuration</h2>
 * <pre>{@code
 * type: SaslTermination
 * config:
 *   mechanisms:
 *     SCRAM-SHA-256:
 *       credentialStore: KeystoreScramCredentialStore
 *       credentialStoreConfig:
 *         file: /path/to/credentials.jks
 *         storePassword:
 *           password: "keystore-password"
 *         storeType: PKCS12
 * }</pre>
 *
 * @param mechanisms map of mechanism name to mechanism configuration
 */
public record SaslTerminationConfig(
                                    @JsonProperty(required = true) Map<String, MechanismConfig> mechanisms) {

    /**
     * Canonical constructor with validation.
     */
    public SaslTerminationConfig {
        if (mechanisms == null || mechanisms.isEmpty()) {
            throw new IllegalArgumentException("At least one mechanism must be configured");
        }

        // Validate mechanism names (IANA registered names are uppercase)
        mechanisms.keySet().forEach(name -> {
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("Mechanism name must not be null or empty");
            }
        });
    }
}
