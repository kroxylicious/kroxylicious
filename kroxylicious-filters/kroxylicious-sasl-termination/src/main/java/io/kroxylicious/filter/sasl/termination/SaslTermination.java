/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import io.kroxylicious.filter.sasl.termination.mechanism.MechanismHandlerFactory;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.sasl.credentialstore.ScramCredentialStore;
import io.kroxylicious.sasl.credentialstore.ScramCredentialStoreService;

/**
 * FilterFactory for SASL termination.
 * <p>
 * This filter terminates SASL authentication at the proxy, authenticating clients
 * against pluggable credential stores without forwarding authentication to the broker.
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
 * }</pre>
 *
 * <h2>Supported Mechanisms</h2>
 * <ul>
 *     <li>SCRAM-SHA-256 - Salted Challenge Response Authentication Mechanism with SHA-256</li>
 * </ul>
 *
 * <h2>Security Barrier</h2>
 * <p>
 * The filter enforces a security barrier: only {@code API_VERSIONS}, {@code SASL_HANDSHAKE},
 * and {@code SASL_AUTHENTICATE} requests are allowed before successful authentication.
 * All other requests are rejected with {@code SASL_AUTHENTICATION_FAILED} and the connection
 * is closed.
 * </p>
 */
@Plugin(configType = SaslTerminationConfig.class)
public class SaslTermination implements FilterFactory<SaslTerminationConfig, SaslTermination.SaslTerminationContext> {

    /**
     * Context for the SASL termination filter.
     * <p>
     * Contains the configured credential stores and mechanism handler factories.
     * </p>
     *
     * @param credentialStores map of mechanism name to credential store
     * @param handlerFactories map of mechanism name to handler factory
     */
    public record SaslTerminationContext(
                                         Map<String, ScramCredentialStore> credentialStores,
                                         Map<String, MechanismHandlerFactory> handlerFactories) {}

    @Override
    public SaslTerminationContext initialize(
                                             FilterFactoryContext context,
                                             SaslTerminationConfig config)
            throws PluginConfigurationException {

        // Load mechanism handler factories via ServiceLoader
        Map<String, MechanismHandlerFactory> handlerFactories = loadMechanismHandlerFactories();

        // Initialize credential stores for each configured mechanism
        Map<String, ScramCredentialStore> credentialStores = new HashMap<>();

        for (Map.Entry<String, MechanismConfig> entry : config.mechanisms().entrySet()) {
            String mechanismName = entry.getKey();
            MechanismConfig mechanismConfig = entry.getValue();

            // Check if we have a handler for this mechanism
            if (!handlerFactories.containsKey(mechanismName)) {
                throw new PluginConfigurationException(
                        "No handler available for mechanism: " + mechanismName +
                                ". Available mechanisms: " + handlerFactories.keySet());
            }

            // Initialize the credential store service
            ScramCredentialStoreService<Object> service = context.pluginInstance(
                    ScramCredentialStoreService.class,
                    mechanismConfig.credentialStore());

            service.initialize(mechanismConfig.credentialStoreConfig());

            // Build and store the credential store
            ScramCredentialStore credentialStore = service.buildCredentialStore();
            credentialStores.put(mechanismName, credentialStore);
        }

        return new SaslTerminationContext(credentialStores, handlerFactories);
    }

    @Override
    public SaslTerminationFilter createFilter(
                                              FilterFactoryContext context,
                                              SaslTerminationContext filterContext) {
        return new SaslTerminationFilter(filterContext);
    }

    /**
     * Load mechanism handler factories via ServiceLoader.
     *
     * @return map of mechanism name to factory
     */
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
}
