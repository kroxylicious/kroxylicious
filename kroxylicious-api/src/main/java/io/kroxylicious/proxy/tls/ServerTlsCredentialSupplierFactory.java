/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.tls;

import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import edu.umd.cs.findbugs.annotations.UnknownNullness;

/**
 * <p>A pluggable factory for creating {@link ServerTlsCredentialSupplier} instances.</p>
 *
 * <p>ServerTlsCredentialSupplierFactory implementations are:</p>
 * <ul>
 * <li>{@linkplain java.util.ServiceLoader service} implementations provided by plugin authors</li>
 * <li>called by the proxy runtime to {@linkplain #create(ServerTlsCredentialSupplierFactoryContext, Object) create}
 *     credential supplier instances</li>
 * <li>used to configure how the proxy obtains TLS credentials for server-side connections</li>
 * </ul>
 *
 * <p>The proxy runtime guarantees that:</p>
 * <ol>
 *     <li>instances will be {@linkplain #initialize(ServerTlsCredentialSupplierFactoryContext, Object) initialized}
 *         before any attempt to {@linkplain #create(ServerTlsCredentialSupplierFactoryContext, Object) create}
 *         credential supplier instances,</li>
 *     <li>instances will eventually be {@linkplain #close(Object) closed} if and only if they were
 *         successfully initialized,</li>
 *     <li>no attempts to create credential supplier instances will be made once a factory instance is closed,</li>
 *     <li>instances will be initialized and closed on the same thread.</li>
 * </ol>
 *
 * <p>Credential supplier creation can happen on a different thread than initialization or cleanup.
 * It is suggested to pass state using the return value from {@link #initialize(ServerTlsCredentialSupplierFactoryContext, Object)}
 * rather than relying on synchronization within a factory implementation.</p>
 *
 * <h2>Lifecycle</h2>
 * <pre>
 * 1. {@link #initialize(ServerTlsCredentialSupplierFactoryContext, Object)} - validate config, create shared resources
 * 2. {@link #create(ServerTlsCredentialSupplierFactoryContext, Object)} - create supplier instances (may be called multiple times)
 * 3. {@link #close(Object)} - release resources
 * </pre>
 *
 * <h2>Usage Example: Simple File-Based Supplier</h2>
 * <pre>{@code
 * @Plugin(configType = FileBasedSupplierConfig.class)
 * public class FileBasedSupplierFactory
 *         implements ServerTlsCredentialSupplierFactory<FileBasedSupplierConfig, FileBasedSupplierConfig> {
 *
 *     @Override
 *     public FileBasedSupplierConfig initialize(ServerTlsCredentialSupplierFactoryContext context,
 *                                               FileBasedSupplierConfig config) {
 *         // Validate configuration
 *         FileBasedSupplierConfig validated = Plugins.requireConfig(this, config);
 *
 *         // Verify files exist
 *         if (!Files.exists(validated.keyPath())) {
 *             throw new PluginConfigurationException("Private key file not found: " + validated.keyPath());
 *         }
 *         if (!Files.exists(validated.certPath())) {
 *             throw new PluginConfigurationException("Certificate file not found: " + validated.certPath());
 *         }
 *
 *         return validated;
 *     }
 *
 *     @Override
 *     public ServerTlsCredentialSupplier create(ServerTlsCredentialSupplierFactoryContext context,
 *                                               FileBasedSupplierConfig config) {
 *         return new FileBasedCredentialSupplier(config.keyPath(), config.certPath());
 *     }
 * }
 *
 * public record FileBasedSupplierConfig(
 *     @JsonProperty(required = true) Path keyPath,
 *     @JsonProperty(required = true) Path certPath
 * ) {}
 * }</pre>
 *
 * <h2>Usage Example: Supplier with Shared Resources</h2>
 * <pre>{@code
 * @Plugin(configType = KmsSupplierConfig.class)
 * public class KmsSupplierFactory
 *         implements ServerTlsCredentialSupplierFactory<KmsSupplierConfig, KmsSupplierFactory.SharedContext> {
 *
 *     record SharedContext(KmsSupplierConfig config, KeyManagementService kms) {}
 *
 *     @Override
 *     public SharedContext initialize(ServerTlsCredentialSupplierFactoryContext context, KmsSupplierConfig config) {
 *         KmsSupplierConfig validated = Plugins.requireConfig(this, config);
 *
 *         // Get KMS plugin instance
 *         KeyManagementService kms = context.pluginInstance(
 *             KeyManagementService.class,
 *             validated.kmsImplementation()
 *         );
 *
 *         return new SharedContext(validated, kms);
 *     }
 *
 *     @Override
 *     public ServerTlsCredentialSupplier create(ServerTlsCredentialSupplierFactoryContext context,
 *                                               SharedContext sharedContext) {
 *         return new KmsCredentialSupplier(sharedContext.kms(), sharedContext.config());
 *     }
 *
 *     @Override
 *     public void close(SharedContext sharedContext) {
 *         // Clean up KMS resources if needed
 *         if (sharedContext.kms() instanceof AutoCloseable closeable) {
 *             closeable.close();
 *         }
 *     }
 * }
 *
 * public record KmsSupplierConfig(
 *     @PluginImplName(KeyManagementService.class) String kmsImplementation,
 *     @PluginImplConfig(implNameProperty = "kmsImplementation") Object kmsConfig,
 *     @JsonProperty(required = true) String keyId
 * ) {}
 * }</pre>
 *
 * <h2>Configuration Example</h2>
 * <p>Configure a TLS credential supplier in the proxy YAML configuration:</p>
 * <pre>
 * virtualClusters:
 *   demo:
 *     targetCluster:
 *       bootstrap_servers: kafka.example.com:9093
 *       tls:
 *         trust:
 *           storeFile: /path/to/truststore.p12
 *           storePassword:
 *             passwordFile: /path/to/password.txt
 *         # TLS credential supplier configuration
 *         tlsCredentialSupplier:
 *           type: FileBasedSupplier  # References @Plugin annotation's name
 *           config:
 *             keyPath: /path/to/client-key.pem
 *             certPath: /path/to/client-cert.pem
 * </pre>
 *
 * @param <C> The type of configuration. Use {@link Void} if the credential supplier is not configurable.
 * @param <I> The type of the initialization data passed between factory methods.
 *
 * @see ServerTlsCredentialSupplier
 * @see TlsCredentials
 */
public interface ServerTlsCredentialSupplierFactory<C, I> {

    /**
     * <p>Initializes the factory with the specified configuration.</p>
     *
     * <p>This method is guaranteed to be called at most once for each credential supplier
     * configuration and before any call to {@link #create(ServerTlsCredentialSupplierFactoryContext, Object)}.
     * This method may provide extra semantic validation of the config and returns some object
     * (which may be the config itself, or some other object) which will be passed to
     * {@link #create(ServerTlsCredentialSupplierFactoryContext, Object)} and {@link #close(Object)}.</p>
     *
     * <p>Use this method to:</p>
     * <ul>
     *     <li>Validate the configuration using {@link io.kroxylicious.proxy.plugin.Plugins#requireConfig(Object, Object)}</li>
     *     <li>Initialize expensive or shared resources (connection pools, caches, etc.)</li>
     *     <li>Retrieve nested plugin instances via {@link ServerTlsCredentialSupplierFactoryContext#pluginInstance(Class, String)}</li>
     *     <li>Verify external dependencies (files exist, services are reachable, etc.)</li>
     * </ul>
     *
     * @param context The factory context providing access to plugins and runtime resources
     * @param config The configuration (may be null if not configurable)
     * @return A configuration state object, specific to the given {@code config}, which will be
     *         passed to {@link #create(ServerTlsCredentialSupplierFactoryContext, Object)} and {@link #close(Object)}
     * @throws PluginConfigurationException when the configuration is invalid
     */
    @UnknownNullness
    I initialize(ServerTlsCredentialSupplierFactoryContext context, @UnknownNullness C config)
            throws PluginConfigurationException;

    /**
     * <p>Creates an instance of {@link ServerTlsCredentialSupplier}.</p>
     *
     * <p>This can be called on a different thread from {@link #initialize(ServerTlsCredentialSupplierFactoryContext, Object)}
     * and {@link #close(Object)}. Implementors should either use the {@code initializationData} to pass
     * state from {@link #initialize(ServerTlsCredentialSupplierFactoryContext, Object)} or use appropriate
     * synchronization.</p>
     *
     * <p>This method may be called multiple times to create multiple supplier instances. Each instance
     * should be independent, though they may share read-only resources passed via {@code initializationData}.</p>
     *
     * @param context The factory context providing access to plugins and runtime resources
     * @param initializationData The initialization data that was returned from
     *                          {@link #initialize(ServerTlsCredentialSupplierFactoryContext, Object)}
     * @return The {@link ServerTlsCredentialSupplier} instance
     */
    ServerTlsCredentialSupplier create(ServerTlsCredentialSupplierFactoryContext context, @UnknownNullness I initializationData);

    /**
     * <p>Called by the runtime to release any resources associated with the given {@code initializationData}.</p>
     *
     * <p>This is guaranteed to eventually be called for each successful call to
     * {@link #initialize(ServerTlsCredentialSupplierFactoryContext, Object)}.
     * Once this method has been called, {@link #create(ServerTlsCredentialSupplierFactoryContext, Object)}
     * won't be called again.</p>
     *
     * <p>Use this method to clean up resources such as:</p>
     * <ul>
     *     <li>Closing connection pools</li>
     *     <li>Shutting down executors</li>
     *     <li>Releasing file handles</li>
     *     <li>Closing nested plugin instances</li>
     * </ul>
     *
     * @param initializationData The initialization data that was returned from
     *                          {@link #initialize(ServerTlsCredentialSupplierFactoryContext, Object)}
     */
    default void close(@UnknownNullness I initializationData) {
    }
}
