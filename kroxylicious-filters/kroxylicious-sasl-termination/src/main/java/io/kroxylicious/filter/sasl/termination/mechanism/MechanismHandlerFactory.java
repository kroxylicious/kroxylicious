/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination.mechanism;

import java.time.Clock;

import io.kroxylicious.filter.sasl.termination.MechanismConfig;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

/**
 * Factory for creating mechanism handlers.
 * <p>
 * Discovered via {@link java.util.ServiceLoader}. Each factory provides handlers
 * for a single SASL mechanism and manages the lifecycle of mechanism-specific
 * resources (credential stores, callback handlers, etc.).
 * </p>
 * <p>
 * <strong>Note:</strong> These are NOT user-facing plugins (no {@code @Plugin} annotation).
 * They provide internal extensibility for adding new mechanism support.
 * </p>
 *
 * <h2>Lifecycle</h2>
 * <ol>
 *     <li>Discovered via ServiceLoader</li>
 *     <li>{@link #initialize} called with mechanism-specific configuration</li>
 *     <li>{@link #createHandler} called once per connection</li>
 *     <li>{@link #close} called on shutdown</li>
 * </ol>
 *
 * <h2>Discovery</h2>
 * <p>
 * Register implementations in:
 * </p>
 * <pre>
 * META-INF/services/io.kroxylicious.filter.sasl.termination.mechanism.MechanismHandlerFactory
 * </pre>
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
     * @throws PluginConfigurationException if the configuration is invalid
     */
    void initialize(MechanismConfig config, FilterFactoryContext context, Clock clock) throws PluginConfigurationException;

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
