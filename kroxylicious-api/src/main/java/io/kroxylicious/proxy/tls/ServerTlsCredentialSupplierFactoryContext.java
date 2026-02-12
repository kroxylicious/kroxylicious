/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.tls;

import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.util.Set;

import io.kroxylicious.proxy.filter.FilterDispatchExecutor;
import io.kroxylicious.proxy.plugin.UnknownPluginInstanceException;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * <p>Construction context for {@link ServerTlsCredentialSupplier} instances.</p>
 *
 * <p>This context is provided to {@link ServerTlsCredentialSupplierFactory} methods
 * during initialization and supplier creation. It provides access to the plugin
 * infrastructure, allowing factories to discover and instantiate nested plugins.</p>
 *
 * <h2>Plugin Composition</h2>
 * <p>The context supports plugin composition through {@link #pluginInstance(Class, String)},
 * enabling credential supplier factories to depend on other plugins such as key
 * management services, certificate authorities, or secret stores.</p>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * @Plugin(configType = MySupplierConfig.class)
 * public class MySupplierFactory implements ServerTlsCredentialSupplierFactory<MySupplierConfig, Context> {
 *
 *     @Override
 *     public Context initialize(ServerTlsCredentialSupplierFactoryContext context, MySupplierConfig config) {
 *         // Get a nested plugin instance for key management
 *         KeyManagementService kms = context.pluginInstance(KeyManagementService.class, config.kmsName());
 *         return new Context(config, kms);
 *     }
 *
 *     @Override
 *     public ServerTlsCredentialSupplier create(ServerTlsCredentialSupplierFactoryContext context, Context initData) {
 *         return new MyCredentialSupplier(initData);
 *     }
 * }
 * }</pre>
 *
 * @see ServerTlsCredentialSupplierFactory
 */
public interface ServerTlsCredentialSupplierFactoryContext {

    /**
     * <p>Gets a plugin instance for the given plugin type and implementation name.</p>
     *
     * @param <P> The plugin interface type
     * @param pluginClass The plugin interface class
     * @param implementationName The specific plugin implementation name to instantiate
     * @return The plugin instance
     * @throws UnknownPluginInstanceException if the plugin with the given implementation name is unknown
     */
    <P> P pluginInstance(Class<P> pluginClass, String implementationName);

    /**
     * <p>Returns the implementation names of all registered instances of a plugin type.</p>
     *
     * @param <P> The plugin interface type
     * @param pluginClass The plugin interface class
     * @return Set of known implementation names registered for the given plugin type
     */
    <P> Set<String> pluginImplementationNames(Class<P> pluginClass);

    /**
     * <p>Returns the filter dispatch executor for asynchronous operations.</p>
     *
     * @return The filter dispatch executor for this context
     */
    @NonNull
    FilterDispatchExecutor filterDispatchExecutor();

    /**
     * <p>Creates a {@link TlsCredentials} instance from the given private key and certificate chain.</p>
     *
     * <p>This factory method validates the provided credentials before creating the
     * {@link TlsCredentials} instance. The validation ensures that:</p>
     * <ul>
     *   <li>The certificate chain is structurally valid</li>
     *   <li>The private key matches the leaf certificate's public key</li>
     * </ul>
     *
     * <p>The equivalent method on {@link ServerTlsCredentialSupplierContext} can be used
     * when the credentials are determined at per-connection time.</p>
     *
     * @param key The private key corresponding to the leaf certificate.
     * @param certificateChain The certificate chain, starting with the leaf certificate
     *        and including any intermediate certificates up to (but not including) the root CA.
     * @return Validated TlsCredentials instance
     * @throws IllegalArgumentException if the key does not match the certificate or the chain is invalid
     */
    @NonNull
    TlsCredentials tlsCredentials(@NonNull PrivateKey key, @NonNull Certificate[] certificateChain);
}
