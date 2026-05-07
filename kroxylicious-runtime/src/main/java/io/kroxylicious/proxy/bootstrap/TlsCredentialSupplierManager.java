/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.bootstrap;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.config.PluginFactory;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.config.tls.TlsCredentialSupplierConfig;
import io.kroxylicious.proxy.filter.FilterDispatchExecutor;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.tls.ServerTlsCredentialSupplier;
import io.kroxylicious.proxy.tls.ServerTlsCredentialSupplierFactory;
import io.kroxylicious.proxy.tls.ServerTlsCredentialSupplierFactoryContext;

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
 *     <li>{@link ServerTlsCredentialSupplierFactory#create} - create a single shared supplier instance</li>
 *     <li>{@link ServerTlsCredentialSupplierFactory#close} - release resources</li>
 * </ol>
 *
 * <p>One manager instance is created per virtual cluster during proxy startup and stored
 * in the {@link io.kroxylicious.proxy.model.VirtualClusterModel}. The factory is initialized
 * once at cluster creation time, and a single shared supplier instance is created eagerly.
 * The supplier must be thread-safe as it is shared across all connections.
 * The manager follows the per-cluster lifecycle and is closed when the virtual cluster is shut down.
 * {@link PluginConfigurationException} is thrown for invalid plugin configurations at startup.</p>
 */
@SuppressWarnings("java:S1192") // ignore dupe string literals is due to logger keys
public class TlsCredentialSupplierManager implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(TlsCredentialSupplierManager.class);

    private static final TlsCredentialSupplierManager UNCONFIGURED = new TlsCredentialSupplierManager();

    /**
     * Wrapper that manages the lifecycle of a single factory instance and its shared supplier.
     */
    private static final class FactoryWrapper {
        private final ServerTlsCredentialSupplierFactory<? super Object, ? super Object> factory;
        private final TlsCredentialSupplierConfig definition;
        private final Object initializationData;
        private final ServerTlsCredentialSupplier supplier;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private FactoryWrapper(ServerTlsCredentialSupplierFactoryContext context,
                               TlsCredentialSupplierConfig definition,
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

            // Eagerly create the shared supplier instance
            try {
                this.supplier = factory.create(context, initializationData);
                LOGGER.atDebug()
                        .addKeyValue("factory", definition.type())
                        .log("Created shared TLS credential supplier instance from factory");
            }
            catch (Exception e) {
                // Clean up initialization data if supplier creation fails
                try {
                    factory.close(initializationData);
                }
                catch (Exception closeEx) {
                    LOGGER.atWarn()
                            .addKeyValue("error", closeEx.getMessage())
                            .setCause(LOGGER.isDebugEnabled() ? closeEx : null)
                            .log("Exception closing factory after supplier creation failure{}",
                                    LOGGER.isDebugEnabled() ? "" : " increase log level to DEBUG for stacktrace");
                }
                throw new PluginConfigurationException(
                        "Exception creating TLS credential supplier " + definition.type() + " using factory " + factory, e);
            }
        }

        private ServerTlsCredentialSupplier getSupplier() {
            if (closed.get()) {
                throw new IllegalStateException("TLS credential supplier factory " + definition.type() + " is closed");
            }
            return supplier;
        }

        private void close() {
            if (!this.closed.getAndSet(true)) {
                try {
                    factory.close(initializationData);
                    LOGGER.atDebug()
                            .addKeyValue("factory", definition.type())
                            .log("Closed TLS credential supplier factory");
                }
                catch (Exception e) {
                    LOGGER.atWarn()
                            .addKeyValue("factory", definition.type())
                            .addKeyValue("error", e.getMessage())
                            .setCause(LOGGER.isDebugEnabled() ? e : null)
                            .log("Exception closing TLS credential supplier factory{}",
                                    LOGGER.isDebugEnabled() ? "" : " increase log level to DEBUG for stacktrace");
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
     * Private no-arg constructor for the unconfigured singleton.
     */
    private TlsCredentialSupplierManager() {
        this.factoryWrapper = null;
    }

    /**
     * Returns an unconfigured manager singleton (null-object pattern).
     *
     * @return An unconfigured manager where {@link #isConfigured()} returns false
     */
    public static TlsCredentialSupplierManager unconfigured() {
        return UNCONFIGURED;
    }

    /**
     * Creates a TlsCredentialSupplierManager for the given target cluster definition.
     *
     * @param pfr The plugin factory registry for ServiceLoader-based discovery
     * @param definition The TLS credential supplier definition from configuration (may be null)
     */
    public TlsCredentialSupplierManager(PluginFactoryRegistry pfr, @Nullable TlsCredentialSupplierConfig definition) {
        Objects.requireNonNull(pfr);

        if (definition == null) {
            this.factoryWrapper = null;
            LOGGER.atDebug()
                    .log("No TLS credential supplier configured");
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
            };

            ServerTlsCredentialSupplierFactory<? super Object, ? super Object> factory = pluginFactory.pluginInstance(definition.type());
            Class<?> configType = pluginFactory.configType(definition.type());

            if (definition.config() != null && !configType.isInstance(definition.config())) {
                throw new PluginConfigurationException(
                        "TLS credential supplier " + definition.type() + " accepts config of type " +
                                configType.getName() + " but provided with config of type " + definition.config().getClass().getName());
            }

            this.factoryWrapper = new FactoryWrapper(context, definition, factory);
            LOGGER.atInfo()
                    .addKeyValue("factory", definition.type())
                    .log("Initialized TLS credential supplier");
        }
    }

    /**
     * Returns the shared {@link ServerTlsCredentialSupplier} instance, or null if no factory was configured.
     * The supplier is created once at initialization time and shared across all connections.
     * It must be thread-safe.
     *
     * @return The shared supplier instance, or null if unconfigured
     */
    @Nullable
    public ServerTlsCredentialSupplier getSupplier() {
        if (factoryWrapper == null) {
            return null;
        }
        return factoryWrapper.getSupplier();
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
