/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.bootstrap;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.PluginFactory;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.FilterDispatchExecutor;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.ResponseFilter;
import io.kroxylicious.proxy.internal.filter.FilterAndInvoker;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Builds per-connection filter instances for a single virtual cluster's filter chain.
 *
 * <h2>Scope</h2>
 * A {@code FilterChainFactory} is scoped to <em>one</em> {@link io.kroxylicious.proxy.model.VirtualClusterModel}.
 * Its lifetime is bound to that VCM — constructed when the VCM is built, closed when the VCM
 * is closed (driven by {@code VirtualClusterRegistry} when the lifecycle reaches
 * {@code Stopped}).
 *
 * <h2>What it holds</h2>
 * <ul>
 *   <li>An ordered list of {@link NamedFilterDefinition}s — the VC's filter chain, as
 *       resolved from {@code VirtualCluster.filters()} (or {@code defaultFilters} when the
 *       cluster opts in to the proxy-wide default chain).</li>
 *   <li>A per-name map of initialized {@link Wrapper}s. Each {@code Wrapper} owns the
 *       expensive {@code initResult} (KMS clients, caches, rule files, etc.) produced by
 *       {@code FilterFactory.initialize}. Wrappers are deduped by name — a chain that
 *       references {@code audit} twice (e.g. before-and-after positions) initializes the
 *       {@code audit} factory once and reuses the {@code initResult} across both positions.</li>
 * </ul>
 *
 * <h2>What it does</h2>
 * {@link #createFilters(FilterFactoryContext)} returns a fresh list of {@link Filter}
 * instances for one downstream channel. The chain order is the order this factory was
 * constructed with; instances are fresh per-call, but the underlying {@code initResult}
 * is shared across all calls to this factory (i.e. across all connections to the same VC).
 *
 */
public class FilterChainFactory implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FilterChainFactory.class);

    private record ClassDescription(boolean overridesDeprecatedOnRequest, boolean overridesDeprecatedOnResponse) {}

    private static final ConcurrentHashMap<Class<?>, ClassDescription> CLASS_DESCRIPTIONS = new ConcurrentHashMap<>();

    /**
     * Manages the lifesystem of a filter instance, initializing it on construction and closing it in {@link #close()}
     */
    private static final class Wrapper {

        private final FilterFactory<? super Object, ? super Object> filterFactory;
        private final NamedFilterDefinition filterDefinition;
        private final Object initResult;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private Wrapper(FilterFactoryContext context,
                        NamedFilterDefinition filterDefinition,
                        FilterFactory<? super Object, ? super Object> filterFactory) {
            this.filterFactory = filterFactory;
            this.filterDefinition = filterDefinition;
            Object config = filterDefinition.config();
            try {
                initResult = filterFactory.initialize(context, config);
            }
            catch (Exception e) {
                throw new PluginConfigurationException(
                        "Exception initializing filter factory " + filterDefinition.name() + " with config " + config + ": " + e.getMessage(), e);
            }
        }

        private Filter create(FilterFactoryContext context) {
            if (closed.get()) {
                throw new IllegalStateException("Filter factory " + filterDefinition.name() + " is closed");
            }
            try {
                Filter filter = filterFactory.createFilter(context, initResult);
                maybeWarnAboutDeprecations(filter);
                return filter;
            }
            catch (Exception e) {
                throw new PluginConfigurationException("Exception instantiating filter " + filterDefinition.name() + " using factory " + filterFactory, e);
            }
        }

        /**
         * Logging about deprecated method usage must be done reflectively as we do not want to add a Logger to the interface.
         */
        private void maybeWarnAboutDeprecations(Filter filter) {
            ClassDescription description = inspectFilterForDeprecations(filter);
            if (description.overridesDeprecatedOnRequest) {
                logDeprecation(filter.getClass(), "onRequest", RequestHeaderData.class);
            }
            if (description.overridesDeprecatedOnResponse) {
                logDeprecation(filter.getClass(), "onResponse", ResponseHeaderData.class);
            }
        }

        private ClassDescription inspectFilterForDeprecations(Filter filter) {
            return CLASS_DESCRIPTIONS.computeIfAbsent(filter.getClass(), clazz -> {
                try {
                    boolean isOnRequestDeprecated = filter instanceof RequestFilter && !clazz.getMethod("onRequest", ApiKeys.class,
                            RequestHeaderData.class,
                            ApiMessage.class,
                            FilterContext.class).isDefault();
                    boolean isOnResponseDeprecated = filter instanceof ResponseFilter && !clazz.getMethod("onResponse", ApiKeys.class,
                            ResponseHeaderData.class,
                            ApiMessage.class,
                            FilterContext.class).isDefault();
                    return new ClassDescription(isOnRequestDeprecated, isOnResponseDeprecated);
                }
                catch (Exception e) {
                    LOGGER.atWarn()
                            .setCause(e)
                            .addKeyValue("filterName", filterDefinition.name())
                            .log("Exception while inspecting Filter implementation for deprecations");
                    return new ClassDescription(false, false);
                }
            });
        }

        private void logDeprecation(Class<? extends Filter> filterClass, String methodName, Class<? extends ApiMessage> headerType) {
            LOGGER.atWarn()
                    .addKeyValue("filterName", filterDefinition.name())
                    .addKeyValue("filterDefinitionType", filterDefinition.type())
                    .addKeyValue("filterClass", filterClass)
                    .addKeyValue("method", methodName + "(ApiKeys, " + headerType.getSimpleName() + ", ApiMessage, FilterContext)")
                    .log("FilterDefinition created a Filter instance which implements a deprecated method. This Filter implementation must be updated as the method will be removed in a future release");
        }

        private void close() {
            if (!this.closed.getAndSet(true)) {
                filterFactory.close(initResult);
            }
        }

        @Override
        public String toString() {
            return "Wrapper[" +
                    "filterFactory=" + filterFactory + ", " +
                    "filterDefinition=" + filterDefinition + ']';
        }

    }

    /**
     * The VC's filter chain in invocation order. May contain duplicate names (e.g. an audit
     * filter applied before and after a transformation). Stored as the source of truth for
     * {@link #createFilters(FilterFactoryContext)} — every call iterates this list.
     */
    private final List<NamedFilterDefinition> filterChain;

    /**
     * Wrappers keyed by filter-definition name. Each wrapper owns one expensive
     * {@code initResult}; the map is deduped on name so a chain that references the same
     * definition twice initializes the factory once and shares the {@code initResult} across
     * both positions in the chain.
     */
    private final Map<String, Wrapper> initialized;

    public FilterChainFactory(@Nullable PluginFactoryRegistry pfr, @Nullable List<NamedFilterDefinition> filterChain) {
        if (filterChain == null || filterChain.isEmpty()) {
            // Empty chain — no plugin lookups needed, so a null pfr is tolerated. Empty
            // FCFs arise from VCs that opt out of the default filter chain (filters == [])
            // and from test-only VirtualClusterModel constructors that don't supply a pfr.
            this.filterChain = List.of();
            this.initialized = Map.of();
        }
        else {
            Objects.requireNonNull(pfr, "PluginFactoryRegistry must not be null when filterChain is non-empty");
            @SuppressWarnings({ "unchecked", "rawtypes" })
            Class<FilterFactory<? super Object, ? super Object>> type = (Class) FilterFactory.class;
            PluginFactory<FilterFactory<? super Object, ? super Object>> pluginFactory = pfr.pluginFactory(type);
            this.filterChain = List.copyOf(filterChain);
            FilterFactoryContext context = new FilterFactoryContext() {

                @Override
                public FilterDispatchExecutor filterDispatchExecutor() {
                    throw new IllegalStateException("no Filter Dispatch executor available at filter factory initialization time");
                }

                @Override
                public <P> P pluginInstance(Class<P> pluginClass, String implementationName) {
                    return pfr.pluginFactory(pluginClass).pluginInstance(implementationName);
                }

                @Override
                public <P> Set<String> pluginImplementationNames(Class<P> pluginClass) {
                    return pfr.pluginFactory(pluginClass).registeredInstanceNames();
                }
            };
            this.initialized = new LinkedHashMap<>(this.filterChain.size());
            try {
                for (var fd : this.filterChain) {
                    // A chain may reference the same definition twice (e.g. audit before/after).
                    // Initialize each unique definition only once — the duplicate-position case
                    // reuses the same Wrapper and its initResult.
                    if (this.initialized.containsKey(fd.name())) {
                        continue;
                    }
                    FilterFactory<? super Object, ? super Object> filterFactory = pluginFactory.pluginInstance(fd.type());
                    Class<?> configType = pluginFactory.configType(fd.type());
                    if (fd.config() == null || configType.isInstance(fd.config())) {
                        Wrapper uninitializedFilterFactory = new Wrapper(context, fd, filterFactory);
                        this.initialized.put(fd.name(), uninitializedFilterFactory);
                    }
                    else {
                        throw new PluginConfigurationException("Filter " + fd.name() + " accepts config of type " +
                                configType.getName() + " but provided with config of type " + fd.config().getClass().getName() + "]");
                    }
                }
            }
            catch (Exception e) {
                // close already initialized factories
                close();
                throw e;
            }
        }
    }

    @Override
    public void close() {
        RuntimeException firstThrown = null;
        // Close in reverse order of initialization
        var list = new ArrayList<>(initialized.values());
        for (int i = list.size() - 1; i >= 0; i--) {
            Wrapper wrapper = list.get(i);
            try {
                wrapper.close();
            }
            catch (RuntimeException e) {
                if (firstThrown == null) {
                    firstThrown = e;
                }
                else {
                    firstThrown.addSuppressed(e);
                }
            }
        }
        if (firstThrown != null) {
            throw firstThrown;
        }
    }

    /**
     * Creates a fresh list of {@link Filter} instances for the chain this factory was built
     * with. Returned instances are <strong>per-call</strong> — every connection gets its own
     * filter instances — but the underlying {@code initResult} is shared across all
     * connections through the {@link Wrapper}s held by this factory.
     *
     * @param context the filter factory context (typically per-connection)
     * @return the new chain, in the order the factory was constructed with; empty if this
     *         factory was constructed with a null or empty chain
     */
    public List<FilterAndInvoker> createFilters(FilterFactoryContext context) {
        return filterChain
                .stream()
                .flatMap(filterDefinition -> FilterAndInvoker.build(
                        filterDefinition.name(),
                        initialized.get(filterDefinition.name()).create(context))
                        .stream())
                .toList();
    }
}
