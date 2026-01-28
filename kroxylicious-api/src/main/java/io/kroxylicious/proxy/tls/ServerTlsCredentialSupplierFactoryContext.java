/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.tls;

import java.io.InputStream;
import java.util.Set;
import java.util.concurrent.CompletionStage;

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
     * <p>This method allows credential supplier factories to depend on other plugins,
     * enabling composition and reuse of existing functionality. For example, a
     * credential supplier might use a key management service plugin to retrieve
     * private keys or a certificate authority plugin to issue certificates.</p>
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
     * <p>This method is useful for discovering available plugins at runtime, for example
     * when validating configuration or providing informative error messages.</p>
     *
     * @param <P> The plugin interface type
     * @param pluginClass The plugin interface class
     * @return Set of known implementation names registered for the given plugin type
     */
    <P> Set<String> pluginImplementationNames(Class<P> pluginClass);

    /**
     * <p>Returns the filter dispatch executor for asynchronous operations.</p>
     *
     * <p>This executor allows credential supplier factories to perform asynchronous work
     * while maintaining thread safety guarantees. All work scheduled on this executor
     * runs on the filter dispatch thread associated with the connection.</p>
     *
     * @return The filter dispatch executor for this context
     */
    @NonNull
    FilterDispatchExecutor filterDispatchExecutor();

    /**
     * <p>Creates a TlsCredentials instance from PEM-encoded certificate and private key data.</p>
     *
     * <p>This factory method performs certificate validation before creating the TlsCredentials
     * instance. The validation ensures that:</p>
     * <ul>
     *   <li>The certificate chain is structurally valid</li>
     *   <li>The private key matches the certificate's public key</li>
     *   <li>The certificate dates are valid (not expired)</li>
     * </ul>
     *
     * <p>Additional validation will be performed by the Netty TLS layer when the credentials
     * are used to establish TLS connections, including:</p>
     * <ul>
     *   <li>Chain of trust verification</li>
     *   <li>Hostname verification (if configured)</li>
     *   <li>Protocol and cipher suite negotiation</li>
     * </ul>
     *
     * <p>The returned CompletionStage completes exceptionally if:</p>
     * <ul>
     *   <li>The certificate or key data is malformed or cannot be parsed</li>
     *   <li>The private key does not match the certificate</li>
     *   <li>Certificate validation fails</li>
     * </ul>
     *
     * @param certificateChainPem PEM-encoded certificate chain (certificate and any intermediate certificates)
     * @param privateKeyPem PEM-encoded private key (PKCS#8 or traditional format)
     * @return CompletionStage that completes with validated TlsCredentials or fails with validation errors
     */
    @NonNull
    CompletionStage<TlsCredentials> tlsCredentials(@NonNull InputStream certificateChainPem, @NonNull InputStream privateKeyPem);
}
