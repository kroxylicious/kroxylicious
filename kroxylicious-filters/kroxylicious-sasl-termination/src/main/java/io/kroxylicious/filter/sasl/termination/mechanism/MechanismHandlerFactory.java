/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination.mechanism;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Factory for creating mechanism handlers.
 * <p>
 * Discovered via {@link java.util.ServiceLoader}. Each factory provides handlers
 * for a single SASL mechanism.
 * </p>
 * <p>
 * <strong>Note:</strong> These are NOT user-facing plugins (no {@code @Plugin} annotation).
 * They provide internal extensibility for adding new mechanism support.
 * </p>
 *
 * <h2>Discovery</h2>
 * <p>
 * Register implementations in:
 * </p>
 * <pre>
 * META-INF/services/io.kroxylicious.filter.sasl.termination.mechanism.MechanismHandlerFactory
 * </pre>
 *
 * <h2>Example Implementation</h2>
 * <pre>{@code
 * public class ScramSha256HandlerFactory implements MechanismHandlerFactory {
 *     @Override
 *     public String mechanismName() {
 *         return "SCRAM-SHA-256";
 *     }
 *
 *     @Override
 *     public MechanismHandler createHandler() {
 *         return new ScramSha256Handler();
 *     }
 * }
 * }</pre>
 */
public interface MechanismHandlerFactory {

    /**
     * Get the IANA-registered mechanism name.
     * <p>
     * Must match exactly what clients send in SASL handshake requests.
     * </p>
     *
     * @return the mechanism name (e.g., "SCRAM-SHA-256", "PLAIN", "OAUTHBEARER")
     */
    @NonNull
    String mechanismName();

    /**
     * Create a new mechanism handler instance.
     * <p>
     * Called once per authentication session. The handler is used for a single
     * connection and then disposed.
     * </p>
     *
     * @return a new handler instance
     */
    @NonNull
    MechanismHandler createHandler();
}
