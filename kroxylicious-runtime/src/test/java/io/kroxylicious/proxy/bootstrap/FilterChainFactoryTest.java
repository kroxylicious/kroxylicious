/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.bootstrap;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Stream;

import org.assertj.core.api.ListAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.proxy.config.FilterDefinition;
import io.kroxylicious.proxy.config.PluginFactory;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.internal.filter.ExampleConfig;
import io.kroxylicious.proxy.internal.filter.FlakyConfig;
import io.kroxylicious.proxy.internal.filter.FlakyFactory;
import io.kroxylicious.proxy.internal.filter.NettyFilterContext;
import io.kroxylicious.proxy.internal.filter.OptionalConfigFactory;
import io.kroxylicious.proxy.internal.filter.RequiresConfigFactory;
import io.kroxylicious.proxy.internal.filter.TestFilter;
import io.kroxylicious.proxy.internal.filter.TestFilterFactory;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
            @NonNull
            @SuppressWarnings({ "rawtypes", "unchecked" })
            @Override
            public <P> PluginFactory<P> pluginFactory(@NonNull Class<P> pluginClass) {
                if (pluginClass == FilterFactory.class) {
                    return new PluginFactory() {
                        @NonNull
                        @Override
                        public FilterFactory pluginInstance(@NonNull String instanceName) {
                            if (instanceName.endsWith(TestFilterFactory.class.getSimpleName())) {
                                return new TestFilterFactory();
                            }
                            else if (instanceName.endsWith(RequiresConfigFactory.class.getSimpleName())) {
                                return new RequiresConfigFactory();
                            }
                            else if (instanceName.endsWith(OptionalConfigFactory.class.getSimpleName())) {
                                return new OptionalConfigFactory();
                            }
                            else if (instanceName.endsWith(FlakyFactory.class.getSimpleName())) {
                                return new FlakyFactory();
                            }
                            throw new RuntimeException("Unknown FilterFactory: " + instanceName);
                        }

                        @NonNull
                        @Override
                        public Class<?> configType(@NonNull String instanceName) {
                            if (instanceName.endsWith(FlakyFactory.class.getSimpleName())) {
                                return FlakyConfig.class;
                            }
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
        final ListAssert<FilterAndInvoker> listAssert = assertFiltersCreated(List.of(new FilterDefinition(TestFilterFactory.class.getName(), config)));
        listAssert.first().extracting(FilterAndInvoker::filter).isInstanceOfSatisfying(TestFilterFactory.TestFilterImpl.class, testFilterImpl -> {
            assertThat(testFilterImpl.getContributorClass()).isEqualTo(TestFilterFactory.class);
            assertThat(testFilterImpl.getContext().eventLoop()).isSameAs(eventLoop);
            assertThat(testFilterImpl.getExampleConfig()).isSameAs(config);
        });
    }

    @ParameterizedTest
    @MethodSource(value = "filterTypes")
    void shouldCreateMultipleFilters(Class<FilterFactory<?, ?>> factoryClass, Class<? extends TestFilter> expectedFilterClass) {
        final ListAssert<FilterAndInvoker> listAssert = assertFiltersCreated(List.of(new FilterDefinition(factoryClass.getName(), config),
                new FilterDefinition(factoryClass.getName(), config)));
        listAssert.element(0).extracting(FilterAndInvoker::filter)
                .isInstanceOfSatisfying(expectedFilterClass, testFilterImpl -> {
                    assertThat(testFilterImpl.getContributorClass()).isEqualTo(factoryClass);
                    assertThat(testFilterImpl.getContext().eventLoop()).isSameAs(eventLoop);
                    assertThat(testFilterImpl.getExampleConfig()).isSameAs(config);
                });
        listAssert.element(1).extracting(FilterAndInvoker::filter).isInstanceOfSatisfying(expectedFilterClass, testFilterImpl -> {
            assertThat(testFilterImpl.getContributorClass()).isEqualTo(factoryClass);
            assertThat(testFilterImpl.getContext().eventLoop()).isSameAs(eventLoop);
            assertThat(testFilterImpl.getExampleConfig()).isSameAs(config);
        });
    }

    static Stream<Arguments> filterTypes() {
        return Stream.of(
                Arguments.of(TestFilterFactory.class, TestFilterFactory.TestFilterImpl.class),
                Arguments.of(OptionalConfigFactory.class, OptionalConfigFactory.Filter.class),
                Arguments.of(RequiresConfigFactory.class, RequiresConfigFactory.Filter.class));
    }

    @Test
    void shouldReturnInvalidFilterNameIfFilterRequiresConfigAndNoneIsSupplied() {
        // Given
        final List<FilterDefinition> filters = List.of(new FilterDefinition(TestFilterFactory.class.getName(), config),
                new FilterDefinition(TestFilterFactory.class.getName(), null));

        // When
        var ex = assertThrows(PluginConfigurationException.class, () -> new FilterChainFactory(pfr, filters));

        // Then
        assertThat(ex.getMessage()).contains(TestFilterFactory.class.getName());
    }

    @Test
    void shouldReturnInvalidFilterNamesForAllFiltersWithoutRequiredConfig() {
        // Given
        final List<FilterDefinition> filters = List.of(new FilterDefinition(TestFilterFactory.class.getName(), null),
                new FilterDefinition(TestFilterFactory.class.getName(), null),
                new FilterDefinition(OptionalConfigFactory.class.getName(), null));

        // When
        var ex = assertThrows(PluginConfigurationException.class, () -> new FilterChainFactory(pfr, filters));

        // Then
        assertThat(ex.getMessage()).contains(TestFilterFactory.class.getName());
    }

    @Test
    void shouldPassValidationWhenAllFiltersHaveConfiguration() {
        // Given
        final List<FilterDefinition> filterDefinitions = List.of(new FilterDefinition(TestFilterFactory.class.getName(), config),
                new FilterDefinition(TestFilterFactory.class.getName(), config));

        // When

        // Then
        // no exception thrown;
        assertThat(new FilterChainFactory(pfr, filterDefinitions)).isNotNull();
    }

    @Test
    void shouldPassValidationWhenFiltersWithOptionalConfigurationAreMissingConfiguration() {
        // Given
        final List<FilterDefinition> filterDefinitions = List.of(new FilterDefinition(TestFilterFactory.class.getName(), config),
                new FilterDefinition(TestFilterFactory.class.getName(), config),
                new FilterDefinition(OptionalConfigFactory.class.getName(), null));

        // When

        // Then
        assertThat(new FilterChainFactory(pfr, filterDefinitions)).isNotNull();
    }

    @Test
    void shouldFailValidationIfRequireConfigMissing() {
        // Given
        final FilterDefinition requiredConfig = new FilterDefinition(RequiresConfigFactory.class.getName(), null);
        List<FilterDefinition> list = List.of(requiredConfig);

        // When

        // Then
        assertThrows(PluginConfigurationException.class, () -> new FilterChainFactory(pfr, list));
    }

    @Test
    void shouldPassValidationIfRequireConfigSupplied() {
        // Given
        final FilterDefinition requiredConfig = new FilterDefinition(RequiresConfigFactory.class.getName(), new ExampleConfig());

        // When

        // Then
        assertThat(new FilterChainFactory(pfr, List.of(requiredConfig))).isNotNull();
    }

    @Test
    void shouldPassValidationIfOptionalConfigSupplied() {
        // Given
        final FilterDefinition requiredConfig = new FilterDefinition(OptionalConfigFactory.class.getName(), new ExampleConfig());

        // When

        // Then
        assertThat(new FilterChainFactory(pfr, List.of(requiredConfig))).isNotNull();
    }

    @Test
    void shouldPassValidationIfOptionalConfigIsMissing() {
        // Given
        final FilterDefinition missingConfig = new FilterDefinition(OptionalConfigFactory.class.getName(), null);

        // When

        // Then
        assertThat(new FilterChainFactory(pfr, List.of(missingConfig))).isNotNull();
    }

    private ListAssert<FilterAndInvoker> assertFiltersCreated(List<FilterDefinition> filterDefinitions) {
        FilterChainFactory filterChainFactory = new FilterChainFactory(pfr, filterDefinitions);
        NettyFilterContext context = new NettyFilterContext(eventLoop, pfr);
        List<FilterAndInvoker> filters = filterChainFactory.createFilters(context);
        return assertThat(filters).hasSameSizeAs(filterDefinitions);
    }

    @Test
    void shouldPropagateFilterInitializeException() {
        // Given
        FlakyFactory.INITIALIZED_AND_UNCLOSED = 0;
        final FilterDefinition flakyDefinition = new FilterDefinition(FlakyFactory.class.getName(),
                new FlakyConfig("foo", null, null));
        List<FilterDefinition> list = List.of(flakyDefinition, flakyDefinition);

        // When

        // Then
        assertThatThrownBy(() -> new FilterChainFactory(pfr, list))
                .isExactlyInstanceOf(PluginConfigurationException.class)
                .cause()
                .isExactlyInstanceOf(RuntimeException.class)
                .hasMessage("foo");
        assertThat(FlakyFactory.INITIALIZED_AND_UNCLOSED).isZero();
    }

    @Test
    void shouldPropagateFilterCreateException() {
        // Given
        FlakyFactory.INITIALIZED_AND_UNCLOSED = 0;
        final FilterDefinition flakyDefinition = new FilterDefinition(FlakyFactory.class.getName(),
                new FlakyConfig(null, "foo", null));
        List<FilterDefinition> list = List.of(flakyDefinition, flakyDefinition);
        NettyFilterContext context = new NettyFilterContext(eventLoop, pfr);

        try (var fcf = new FilterChainFactory(pfr, list)) {
            // When
            assertThat(FlakyFactory.INITIALIZED_AND_UNCLOSED).isEqualTo(2);

            // Then
            assertThatThrownBy(() -> fcf.createFilters(context))
                    .isExactlyInstanceOf(PluginConfigurationException.class)
                    .cause()
                    .isExactlyInstanceOf(RuntimeException.class)
                    .hasMessage("foo");
            assertThat(FlakyFactory.INITIALIZED_AND_UNCLOSED).isEqualTo(2);
        }
        assertThat(FlakyFactory.INITIALIZED_AND_UNCLOSED).isZero();
    }

    @Test
    void shouldPropagateFilterCloseException() {
        // Given
        FlakyFactory.INITIALIZED_AND_UNCLOSED = 0;
        final FilterDefinition flakyDefinition = new FilterDefinition(FlakyFactory.class.getName(),
                new FlakyConfig(null, null, "foo"));
        List<FilterDefinition> list = List.of(flakyDefinition, flakyDefinition);
        try (var fcf = new FilterChainFactory(pfr, list)) {
            // When
            assertThat(FlakyFactory.INITIALIZED_AND_UNCLOSED).isEqualTo(2);

            // Then
            assertThatThrownBy(fcf::close)
                    .isExactlyInstanceOf(RuntimeException.class)
                    .hasMessage("foo");
            assertThat(FlakyFactory.INITIALIZED_AND_UNCLOSED).isZero();
        }
        assertThat(FlakyFactory.INITIALIZED_AND_UNCLOSED).isZero();
    }
}
