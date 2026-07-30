/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

/**
 * Shared configuration interface for SCRAM mechanism variants.
 *
 * @see ScramSha256MechanismConfig
 * @see ScramSha512MechanismConfig
 */
public sealed interface ScramMechanismConfig extends MechanismConfig
        permits ScramSha256MechanismConfig, ScramSha512MechanismConfig {

    String credentialStore();

    Object credentialStoreConfig();
}
