/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import io.kroxylicious.filter.sasl.termination.mechanism.MechanismHandlerFactory;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * FilterFactory for SASL termination.
 * <p>
 * This filter terminates SASL authentication at the proxy, authenticating clients
 * against pluggable credential stores or token validators without forwarding
 * authentication to the broker.
 * </p>
 *
 * <h2>Configuration</h2>
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
 *     OAUTHBEARER:
 *       jwksEndpointUrl: https://idp.example.com/.well-known/jwks.json
 *       expectedAudience: kafka
 *       expectedIssuer: https://idp.example.com
 * }</pre>
 *
 * <h2>Supported Mechanisms</h2>
 * <ul>
 *     <li>SCRAM-SHA-256 - Salted Challenge Response Authentication Mechanism with SHA-256</li>
 *     <li>SCRAM-SHA-512 - Salted Challenge Response Authentication Mechanism with SHA-512</li>
 *     <li>OAUTHBEARER - OAuth 2.0 Bearer Token authentication</li>
 * </ul>
 */
@Plugin(configType = SaslTerminationConfig.class)
public class SaslTermination implements FilterFactory<SaslTerminationConfig, SaslTermination.SaslTerminationContext> {

    private final Clock clock;

    @SuppressWarnings("unused")
    public SaslTermination() {
        this(Clock.systemUTC());
    }

    @VisibleForTesting
    SaslTermination(Clock clock) {
        this.clock = clock;
    }

    /**
     * Context for the SASL termination filter.
     *
     * @param handlerFactories map of mechanism name to initialized handler factory
     * @param maxTimeBeforeReauth maximum session lifetime, null if disabled
     * @param clock clock for session expiry computation
     */
    public record SaslTerminationContext(
                                         Map<String, MechanismHandlerFactory> handlerFactories,
                                         @Nullable Duration maxTimeBeforeReauth,
                                         Clock clock) {}

    @Override
    @SuppressFBWarnings(value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE", justification = "Framework guarantees non-null parameters")
    public SaslTerminationContext initialize(
                                             @NonNull FilterFactoryContext context,
                                             @NonNull SaslTerminationConfig config)
            throws PluginConfigurationException {

        Map<String, MechanismHandlerFactory> availableFactories = loadMechanismHandlerFactories();
        Map<String, MechanismHandlerFactory> initializedFactories = new HashMap<>();

        for (Map.Entry<String, MechanismConfig> entry : config.mechanisms().entrySet()) {
            String mechanismName = entry.getKey();
            MechanismConfig mechanismConfig = entry.getValue();

            MechanismHandlerFactory factory = availableFactories.get(mechanismName);
            if (factory == null) {
                throw new PluginConfigurationException(
                        "No handler available for mechanism: " + mechanismName +
                                ". Available mechanisms: " + availableFactories.keySet());
            }

            factory.initialize(mechanismConfig, context, clock);
            initializedFactories.put(mechanismName, factory);
        }

        return new SaslTerminationContext(initializedFactories, config.maxTimeBeforeReauth(), clock);
    }

    @Override
    @SuppressFBWarnings(value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE", justification = "Framework guarantees non-null parameters")
    public SaslTerminationFilter createFilter(
                                              @NonNull FilterFactoryContext context,
                                              @NonNull SaslTerminationContext filterContext) {
        return new SaslTerminationFilter(filterContext);
    }

    private Map<String, MechanismHandlerFactory> loadMechanismHandlerFactories() {
        Map<String, MechanismHandlerFactory> factories = new HashMap<>();

        ServiceLoader<MechanismHandlerFactory> loader = ServiceLoader.load(MechanismHandlerFactory.class);
        for (MechanismHandlerFactory factory : loader) {
            String mechanismName = factory.mechanismName();
            if (factories.containsKey(mechanismName)) {
                throw new IllegalStateException(
                        "Duplicate mechanism handler factory for: " + mechanismName);
            }
            factories.put(mechanismName, factory);
        }

        return factories;
    }

    @Override
    @SuppressFBWarnings(value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE", justification = "Framework guarantees non-null parameters")
    public void close(@NonNull SaslTerminationContext initializationData) {
        RuntimeException firstException = null;
        for (var factory : initializationData.handlerFactories().values()) {
            try {
                factory.close();
            }
            catch (RuntimeException e) {
                if (firstException == null) {
                    firstException = e;
                }
                else {
                    firstException.addSuppressed(e);
                }
            }
        }
        if (firstException != null) {
            throw firstException;
        }
    }
}
