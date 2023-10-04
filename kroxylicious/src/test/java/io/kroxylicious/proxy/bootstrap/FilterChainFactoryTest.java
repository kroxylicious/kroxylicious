/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.bootstrap;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.assertj.core.api.ListAssert;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.FilterDefinition;
import io.kroxylicious.proxy.config.PluginFactory;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.internal.filter.ExampleConfig;
import io.kroxylicious.proxy.internal.filter.NettyFilterContext;
import io.kroxylicious.proxy.internal.filter.OptionalConfig;
import io.kroxylicious.proxy.internal.filter.RequiresConfig;
import io.kroxylicious.proxy.internal.filter.TestFilter;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FilterChainFactoryTest {

    private ScheduledExecutorService eventLoop;
    private ExampleConfig config;
    private PluginFactoryRegistry pfr;

    @BeforeEach
    void setUp() {
        eventLoop = Executors.newScheduledThreadPool(1);
        config = new ExampleConfig();
        pfr = new PluginFactoryRegistry() {
            @Override
            public <P> PluginFactory<P> pluginFactory(Class<P> pluginClass) {
                if (pluginClass == FilterFactory.class) {
                    return new PluginFactory() {
                        @NotNull
                        @Override
                        public FilterFactory pluginInstance(@NotNull String name) {
                            if (name.endsWith("TestFilter")) {
                                return new TestFilter();
                            }
                            else if (name.endsWith("RequiresConfig")) {
                                return new RequiresConfig();
                            }
                            else if (name.endsWith("OptionalConfig")) {
                                return new OptionalConfig();
                            }
                            throw new RuntimeException("Unknown FilterFactory" + name);
                        }

                        @NotNull
                        @Override
                        public Class<?> configType(@NotNull String name) {
                            return ExampleConfig.class;
                        }
                    };
                }
                else {
                    throw new RuntimeException();
                }
            }
        };

    }

    @Test
    void testNullFiltersInConfigResultsInEmptyList() {
        ScheduledExecutorService eventLoop = Executors.newScheduledThreadPool(1);
        FilterChainFactory filterChainFactory = new FilterChainFactory(pfr, null);
        List<FilterAndInvoker> filters = filterChainFactory.createFilters(new NettyFilterContext(eventLoop, pfr));
        assertNotNull(filters, "Filters list should not be null");
        assertTrue(filters.isEmpty(), "Filters list should be empty");
    }

    @Test
    void testEmptyFiltersInConfigResultsInEmptyList() {
        assertFiltersCreated(List.of());
    }

    @Test
    void testCreateFilter() {
        final ListAssert<FilterAndInvoker> listAssert = assertFiltersCreated(List.of(new FilterDefinition(TestFilter.class.getName(), config)));
        listAssert.first().extracting(FilterAndInvoker::filter).isInstanceOfSatisfying(TestFilter.TestFilterImpl.class, testFilterImpl -> {
            assertThat(testFilterImpl.getContributorClass()).isEqualTo(TestFilter.class);
            assertThat(testFilterImpl.getContext().eventLoop()).isSameAs(eventLoop);
            assertThat(testFilterImpl.getExampleConfig()).isSameAs(config);
        });
    }

    @Test
    void testCreateFilters() {
        final ListAssert<FilterAndInvoker> listAssert = assertFiltersCreated(List.of(new FilterDefinition(TestFilter.class.getName(), config),
                new FilterDefinition(TestFilter.class.getName(), config)));
        listAssert.element(0).extracting(FilterAndInvoker::filter).isInstanceOfSatisfying(TestFilter.TestFilterImpl.class, testFilterImpl -> {
            assertThat(testFilterImpl.getContributorClass()).isEqualTo(TestFilter.class);
            assertThat(testFilterImpl.getContext().eventLoop()).isSameAs(eventLoop);
            assertThat(testFilterImpl.getExampleConfig()).isSameAs(config);
        });
        listAssert.element(1).extracting(FilterAndInvoker::filter).isInstanceOfSatisfying(TestFilter.TestFilterImpl.class, testFilterImpl -> {
            assertThat(testFilterImpl.getContributorClass()).isEqualTo(TestFilter.class);
            assertThat(testFilterImpl.getContext().eventLoop()).isSameAs(eventLoop);
            assertThat(testFilterImpl.getExampleConfig()).isSameAs(config);
        });
    }

    @Test
    void shouldReturnInvalidFilterNameIfFilterRequiresConfigAndNoneIsSupplied() {
        // Given
        final List<FilterDefinition> filters = List.of(new FilterDefinition(TestFilter.class.getName(), config),
                new FilterDefinition(TestFilter.class.getName(), null));

        // When
        var ex = assertThrows(PluginConfigurationException.class, () -> new FilterChainFactory(pfr, filters));

        // Then
        assertThat(ex.getMessage()).contains(TestFilter.class.getName());
    }

    @Test
    void shouldReturnInvalidFilterNamesForAllFiltersWithoutRequiredConfig() {
        // Given
        final List<FilterDefinition> filters = List.of(new FilterDefinition(TestFilter.class.getName(), null),
                new FilterDefinition(TestFilter.class.getName(), null),
                new FilterDefinition(OptionalConfig.class.getName(), null));

        // When
        var ex = assertThrows(PluginConfigurationException.class, () -> new FilterChainFactory(pfr, filters));

        // Then
        assertThat(ex.getMessage()).contains(TestFilter.class.getName());
    }

    @Test
    void shouldPassValidationWhenAllFiltersHaveConfiguration() {
        // Given
        final List<FilterDefinition> filterDefinitions = List.of(new FilterDefinition(TestFilter.class.getName(), config),
                new FilterDefinition(TestFilter.class.getName(), config));

        // When

        // Then
        // no exception thrown;
        assertThat(new FilterChainFactory(pfr, filterDefinitions)).isNotNull();
    }

    @Test
    void shouldPassValidationWhenFiltersWithOptionalConfigurationAreMissingConfiguration() {
        // Given
        final List<FilterDefinition> filterDefinitions = List.of(new FilterDefinition(TestFilter.class.getName(), config),
                new FilterDefinition(TestFilter.class.getName(), config),
                new FilterDefinition(OptionalConfig.class.getName(), null));

        // When

        // Then
        assertThat(new FilterChainFactory(pfr, filterDefinitions)).isNotNull();
    }

    private ListAssert<FilterAndInvoker> assertFiltersCreated(List<FilterDefinition> filterDefinitions) {
        FilterChainFactory filterChainFactory = new FilterChainFactory(pfr, filterDefinitions);
        NettyFilterContext context = new NettyFilterContext(eventLoop, pfr);
        List<FilterAndInvoker> filters = filterChainFactory.createFilters(context);
        return assertThat(filters).isNotNull().hasSize(filterDefinitions.size());
    }

    @Test
    void shouldFailValidationIfRequireConfigMissing() {
        // Given
        final FilterDefinition requiredConfig = new FilterDefinition(RequiresConfig.class.getName(), null);
        List<FilterDefinition> list = List.of(requiredConfig);

        // When

        // Then
        assertThrows(PluginConfigurationException.class, () -> new FilterChainFactory(pfr, list));
    }

    @Test
    void shouldPassValidationIfRequireConfigSupplied() {
        // Given
        final FilterDefinition requiredConfig = new FilterDefinition(RequiresConfig.class.getName(), new ExampleConfig());

        // When

        // Then
        assertThat(new FilterChainFactory(pfr, List.of(requiredConfig))).isNotNull();
    }

    @Test
    void shouldPassValidationIfOptionalConfigSupplied() {
        // Given
        final FilterDefinition requiredConfig = new FilterDefinition(OptionalConfig.class.getName(), new ExampleConfig());

        // When

        // Then
        assertThat(new FilterChainFactory(pfr, List.of(requiredConfig))).isNotNull();
    }

    @Test
    void shouldPassValidationIfOptionalConfigIsMissing() {
        // Given
        final FilterDefinition requiredConfig = new FilterDefinition(OptionalConfig.class.getName(), null);

        // When

        // Then
    }

}
