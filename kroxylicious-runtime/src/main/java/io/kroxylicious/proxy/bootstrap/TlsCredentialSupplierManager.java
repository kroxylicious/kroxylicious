/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.bootstrap;

import java.io.InputStream;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.config.PluginFactory;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.config.tls.TlsCredentialSupplierDefinition;
import io.kroxylicious.proxy.filter.FilterDispatchExecutor;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.tls.ServerTlsCredentialSupplier;
import io.kroxylicious.proxy.tls.ServerTlsCredentialSupplierFactory;
import io.kroxylicious.proxy.tls.ServerTlsCredentialSupplierFactoryContext;
import io.kroxylicious.proxy.tls.TlsCredentials;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Manages the lifecycle of TLS credential supplier plugin instances.
 * <p>
 * This class handles ServiceLoader-based discovery, initialization, and cleanup
 * of {@link ServerTlsCredentialSupplierFactory} instances. It follows the same
 * pattern as {@link FilterChainFactory}, managing the factory lifecycle:
 * </p>
 * <ol>
 *     <li>{@link ServerTlsCredentialSupplierFactory#initialize} - validate config, create shared resources</li>
 *     <li>{@link ServerTlsCredentialSupplierFactory#create} - create supplier instances (may be called multiple times)</li>
 *     <li>{@link ServerTlsCredentialSupplierFactory#close} - release resources</li>
 * </ol>
 *
 * <p>One factory instance is created per target cluster during proxy startup.
 * The factory lifecycle is managed correctly (initialize → create → close), and
 * {@link PluginConfigurationException} is thrown for invalid plugin configurations at startup.</p>
 */
public class TlsCredentialSupplierManager implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(TlsCredentialSupplierManager.class);

    /**
     * Wrapper that manages the lifecycle of a single factory instance.
     */
    private static final class FactoryWrapper {
        private final ServerTlsCredentialSupplierFactory<? super Object, ? super Object> factory;
        private final TlsCredentialSupplierDefinition definition;
        private final Object initializationData;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private FactoryWrapper(ServerTlsCredentialSupplierFactoryContext context,
                               TlsCredentialSupplierDefinition definition,
                               ServerTlsCredentialSupplierFactory<? super Object, ? super Object> factory) {
            this.factory = Objects.requireNonNull(factory);
            this.definition = Objects.requireNonNull(definition);
            Object config = definition.config();
            try {
                this.initializationData = factory.initialize(context, config);
            }
            catch (Exception e) {
                throw new PluginConfigurationException(
                        "Exception initializing TLS credential supplier factory " + definition.type() + " with config " + config + ": " + e.getMessage(), e);
            }
        }

        public ServerTlsCredentialSupplier create(ServerTlsCredentialSupplierFactoryContext context) {
            if (closed.get()) {
                throw new IllegalStateException("TLS credential supplier factory " + definition.type() + " is closed");
            }
            try {
                ServerTlsCredentialSupplier supplier = factory.create(context, initializationData);
                LOGGER.debug("Created TLS credential supplier instance from factory {}", definition.type());
                return supplier;
            }
            catch (Exception e) {
                throw new PluginConfigurationException(
                        "Exception creating TLS credential supplier " + definition.type() + " using factory " + factory, e);
            }
        }

        public void close() {
            if (!this.closed.getAndSet(true)) {
                try {
                    factory.close(initializationData);
                    LOGGER.debug("Closed TLS credential supplier factory {}", definition.type());
                }
                catch (Exception e) {
                    LOGGER.warn("Exception closing TLS credential supplier factory {}", definition.type(), e);
                }
            }
        }

        @Override
        public String toString() {
            return "FactoryWrapper[" +
                    "factory=" + factory + ", " +
                    "definition=" + definition + ']';
        }
    }

    @Nullable
    private final FactoryWrapper factoryWrapper;

    /**
     * Creates a TlsCredentialSupplierManager for the given target cluster definition.
     *
     * @param pfr The plugin factory registry for ServiceLoader-based discovery
     * @param definition The TLS credential supplier definition from configuration (may be null)
     */
    public TlsCredentialSupplierManager(PluginFactoryRegistry pfr, @Nullable TlsCredentialSupplierDefinition definition) {
        Objects.requireNonNull(pfr);

        if (definition == null) {
            this.factoryWrapper = null;
            LOGGER.debug("No TLS credential supplier configured");
        }
        else {
            @SuppressWarnings({ "unchecked", "rawtypes" })
            Class<ServerTlsCredentialSupplierFactory<? super Object, ? super Object>> type = (Class) ServerTlsCredentialSupplierFactory.class;
            PluginFactory<ServerTlsCredentialSupplierFactory<? super Object, ? super Object>> pluginFactory = pfr.pluginFactory(type);

            ServerTlsCredentialSupplierFactoryContext context = new ServerTlsCredentialSupplierFactoryContext() {
                @Override
                public <P> P pluginInstance(Class<P> pluginClass, String implementationName) {
                    return pfr.pluginFactory(pluginClass).pluginInstance(implementationName);
                }

                @Override
                public <P> Set<String> pluginImplementationNames(Class<P> pluginClass) {
                    return pfr.pluginFactory(pluginClass).registeredInstanceNames();
                }

                @Override
                @NonNull
                public FilterDispatchExecutor filterDispatchExecutor() {
                    throw new IllegalStateException("FilterDispatchExecutor not available at factory initialization time");
                }

                @Override
                @NonNull
                public CompletionStage<TlsCredentials> tlsCredentials(@NonNull InputStream certificateChainPem, @NonNull InputStream privateKeyPem) {
                    throw new IllegalStateException("tlsCredentials() not available at factory initialization time");
                }
            };

            FactoryWrapper wrapper = null;
            try {
                ServerTlsCredentialSupplierFactory<? super Object, ? super Object> factory = pluginFactory.pluginInstance(definition.type());
                Class<?> configType = pluginFactory.configType(definition.type());

                if (definition.config() == null || configType.isInstance(definition.config())) {
                    wrapper = new FactoryWrapper(context, definition, factory);
                    LOGGER.info("Initialized TLS credential supplier factory: {}", definition.type());
                }
                else {
                    throw new PluginConfigurationException(
                            "TLS credential supplier " + definition.type() + " accepts config of type " +
                                    configType.getName() + " but provided with config of type " + definition.config().getClass().getName());
                }
            }
            catch (Exception e) {
                // If initialization fails, ensure we don't leave partial state
                if (wrapper != null) {
                    wrapper.close();
                }
                throw e;
            }
            this.factoryWrapper = wrapper;
        }
    }

    /**
     * Creates a new {@link ServerTlsCredentialSupplier} instance using the initialized factory.
     *
     * @param context The factory context for supplier creation
     * @return A new supplier instance, or null if no factory was configured
     */
    @Nullable
    public ServerTlsCredentialSupplier createSupplier(ServerTlsCredentialSupplierFactoryContext context) {
        if (factoryWrapper == null) {
            return null;
        }
        return factoryWrapper.create(context);
    }

    /**
     * Returns true if a TLS credential supplier factory is configured.
     *
     * @return true if a factory is configured, false otherwise
     */
    public boolean isConfigured() {
        return factoryWrapper != null;
    }

    @Override
    public void close() {
        if (factoryWrapper != null) {
            factoryWrapper.close();
        }
    }
}
