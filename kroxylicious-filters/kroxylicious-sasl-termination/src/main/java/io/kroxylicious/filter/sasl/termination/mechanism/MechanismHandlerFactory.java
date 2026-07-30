/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination.mechanism;

import java.time.Clock;
import java.time.Duration;

import io.kroxylicious.filter.sasl.termination.MechanismConfig;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

/**
 * Internal factory for creating per-connection mechanism handlers.
 * <p>
 * Each factory manages the lifecycle of mechanism-specific resources
 * (credential stores, callback handlers, etc.) and creates per-connection
 * handler instances.
 * </p>
 */
public interface MechanismHandlerFactory extends AutoCloseable {

    /**
     * Get the IANA-registered mechanism name.
     * <p>
     * Must match exactly what clients send in SASL handshake requests.
     * </p>
     *
     * @return the mechanism name (e.g., "SCRAM-SHA-256", "OAUTHBEARER")
     */
    String mechanismName();

    /**
     * Initialize this factory with mechanism-specific configuration.
     * <p>
     * Called once during filter factory initialization. The factory should
     * create and store any shared resources needed by handlers (e.g., credential
     * stores, callback handlers).
     * </p>
     *
     * @param config the mechanism-specific configuration
     * @param context the filter factory context for plugin resolution
     * @param clock clock for time-dependent operations (e.g. token lifetime computation)
     * @param fixedAuthDelay fixed delay applied to all authentication rounds for timing side-channel mitigation
     * @throws PluginConfigurationException if the configuration is invalid
     */
    void initialize(MechanismConfig config, FilterFactoryContext context, Clock clock,
                    Duration fixedAuthDelay)
            throws PluginConfigurationException;

    /**
     * Create a new mechanism handler instance.
     * <p>
     * Called once per authentication session. The handler is used for a single
     * connection and then disposed.
     * </p>
     *
     * @return a new handler instance
     */
    MechanismHandler createHandler();

    /**
     * Release resources held by this factory.
     * <p>
     * Called on filter factory shutdown. Must be idempotent.
     * </p>
     */
    @Override
    void close();
}
