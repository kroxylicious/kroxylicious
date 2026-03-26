/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination.mechanism;

import org.apache.kafka.common.security.scram.internals.ScramMechanism;

/**
 * Factory for creating SCRAM-SHA-256 mechanism handlers.
 * <p>
 * Discovered via {@link java.util.ServiceLoader}. Registered in:
 * </p>
 * <pre>
 * META-INF/services/io.kroxylicious.filter.sasl.termination.mechanism.MechanismHandlerFactory
 * </pre>
 */
public class ScramSha256HandlerFactory implements MechanismHandlerFactory {

    private static final String MECHANISM_NAME = ScramMechanism.SCRAM_SHA_256.mechanismName();

    @Override
    public String mechanismName() {
        return MECHANISM_NAME;
    }

    @Override
    public MechanismHandler createHandler() {
        return new ScramSha256Handler();
    }
}
