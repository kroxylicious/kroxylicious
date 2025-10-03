/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.bootstrap;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.assertj.core.api.ListAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoop;

import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.PluginFactory;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.filter.FilterDispatchExecutor;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FilterChainFactoryTest {

    private EventLoop eventLoop;
    private ExampleConfig config;
    private PluginFactoryRegistry pfr;

    @BeforeEach
    void setUp() {
        eventLoop = new DefaultEventLoop();
        config = new ExampleConfig();
        pfr = new PluginFactoryRegistry() {
            @SuppressWarnings({ "rawtypes", "unchecked" })
            @Override
            public <P> PluginFactory<P> pluginFactory(Class<P> pluginClass) {
                if (pluginClass == FilterFactory.class) {
                    return new PluginFactory() {
                        @Override
                        public FilterFactory pluginInstance(String instanceName) {
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

                        @Override
                        public Class<?> configType(String instanceName) {
                            if (instanceName.endsWith(FlakyFactory.class.getSimpleName())) {
                                return FlakyConfig.class;
                            }
                            return ExampleConfig.class;
                        }

                        @Override
                        public Set<String> registeredInstanceNames() {
                            return Set.of(TestFilterFactory.class.getSimpleName(),
                                    RequiresConfigFactory.class.getSimpleName(),
                                    OptionalConfigFactory.class.getSimpleName(),
                                    FlakyFactory.class.getSimpleName());
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
        FilterChainFactory filterChainFactory = new FilterChainFactory(pfr, null);
        List<FilterAndInvoker> filters = filterChainFactory.createFilters(new NettyFilterContext(eventLoop, pfr), null);
        assertThat(filters)
                .isNotNull()
                .isEmpty();
    }

    @Test
    void testEmptyFiltersInConfigResultsInEmptyList() {
        assertFiltersCreated(List.of());
    }

    @Test
    void testCreateFilter() {
        final ListAssert<FilterAndInvoker> listAssert = assertFiltersCreated(
                List.of(new NamedFilterDefinition("myFilterDef", TestFilterFactory.class.getName(), config)));
        listAssert.first().extracting(FilterAndInvoker::filter).isInstanceOfSatisfying(TestFilterFactory.TestFilterImpl.class, testFilterImpl -> {
            assertThat(testFilterImpl.getContributorClass()).isEqualTo(TestFilterFactory.class);
            FilterDispatchExecutor filterDispatchExecutor = testFilterImpl.getContext().filterDispatchExecutor();
            assertThat(filterDispatchExecutor).isNotNull();
            assertThat(testFilterImpl.getContext().filterDispatchExecutor()).isSameAs(filterDispatchExecutor);
            assertThat(testFilterImpl.getExampleConfig()).isSameAs(config);
        });
    }

    @ParameterizedTest
    @MethodSource(value = "filterTypes")
    void shouldCreateMultipleFilters(Class<FilterFactory<?, ?>> factoryClass, Class<? extends TestFilter> expectedFilterClass) {
        final ListAssert<FilterAndInvoker> listAssert = assertFiltersCreated(List.of(new NamedFilterDefinition("filterDef1", factoryClass.getName(), config),
                new NamedFilterDefinition("filterDef2", factoryClass.getName(), config)));
        listAssert.element(0).extracting(FilterAndInvoker::filter)
                .isInstanceOfSatisfying(expectedFilterClass, testFilterImpl -> {
                    assertThat(testFilterImpl.getContributorClass()).isEqualTo(factoryClass);
                    FilterDispatchExecutor filterDispatchExecutor = testFilterImpl.getContext().filterDispatchExecutor();
                    assertThat(filterDispatchExecutor).isNotNull();
                    assertThat(testFilterImpl.getContext().filterDispatchExecutor()).isSameAs(filterDispatchExecutor);
                    assertThat(testFilterImpl.getExampleConfig()).isSameAs(config);
                });
        listAssert.element(1).extracting(FilterAndInvoker::filter).isInstanceOfSatisfying(expectedFilterClass, testFilterImpl -> {
            assertThat(testFilterImpl.getContributorClass()).isEqualTo(factoryClass);
            FilterDispatchExecutor filterDispatchExecutor = testFilterImpl.getContext().filterDispatchExecutor();
            assertThat(filterDispatchExecutor).isNotNull();
            assertThat(testFilterImpl.getContext().filterDispatchExecutor()).isSameAs(filterDispatchExecutor);
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
        final List<NamedFilterDefinition> filters = List.of(new NamedFilterDefinition("myValidFilterDef", TestFilterFactory.class.getName(), config),
                new NamedFilterDefinition("myInvalidFilterDef", TestFilterFactory.class.getName(), null));

        // When/Then
        assertThatThrownBy(() -> new FilterChainFactory(pfr, filters))
                .hasMessageContaining("Exception initializing filter factory myInvalidFilterDef with config null");
    }

    @Test
    void shouldReturnInvalidFilterNamesForAllFiltersWithoutRequiredConfig() {
        // Given
        final List<NamedFilterDefinition> filters = List.of(new NamedFilterDefinition("myInvalidFilterDef1", TestFilterFactory.class.getName(), null),
                new NamedFilterDefinition("myInvalidFilterDef2", TestFilterFactory.class.getName(), null),
                new NamedFilterDefinition("myValidFilterDef2", OptionalConfigFactory.class.getName(), null));

        // When/Then
        assertThatThrownBy(() -> new FilterChainFactory(pfr, filters))
                .hasMessageContaining("Exception initializing filter factory myInvalidFilterDef1 with config null");
    }

    @Test
    void shouldPassValidationWhenAllFiltersHaveConfiguration() {
        // Given
        final List<NamedFilterDefinition> filterDefinitions = List.of(new NamedFilterDefinition("myValidFilterDef1", TestFilterFactory.class.getName(), config),
                new NamedFilterDefinition("myValidFilterDef2", TestFilterFactory.class.getName(), config));

        // When

        // Then
        assertThat(new FilterChainFactory(pfr, filterDefinitions)).isNotNull();
    }

    @Test
    void shouldPassValidationWhenFiltersWithOptionalConfigurationAreMissingConfiguration() {
        // Given
        final List<NamedFilterDefinition> filterDefinitions = List.of(new NamedFilterDefinition("myValidFilterDef1", TestFilterFactory.class.getName(), config),
                new NamedFilterDefinition("myValidFilterDef2", TestFilterFactory.class.getName(), config),
                new NamedFilterDefinition("myValidFilterDef3", OptionalConfigFactory.class.getName(), null));

        // When

        // Then
        assertThat(new FilterChainFactory(pfr, filterDefinitions)).isNotNull();
    }

    @Test
    void shouldFailValidationIfRequireConfigMissing() {
        // Given
        List<NamedFilterDefinition> list = List.of(new NamedFilterDefinition("myInvalidFilterDef1", RequiresConfigFactory.class.getName(), null));

        // When

        // Then
        assertThatThrownBy(() -> new FilterChainFactory(pfr, list))
                .isInstanceOf(PluginConfigurationException.class);
    }

    @Test
    void shouldPassValidationIfRequireConfigSupplied() {
        // Given
        var list = List.of(new NamedFilterDefinition("myValidFilterDef", RequiresConfigFactory.class.getName(), new ExampleConfig()));

        // When

        // Then
        assertThat(new FilterChainFactory(pfr, list)).isNotNull();
    }

    @Test
    void shouldPassValidationIfOptionalConfigSupplied() {
        // Given
        List<NamedFilterDefinition> list = List.of(new NamedFilterDefinition("myValidFilterDef", OptionalConfigFactory.class.getName(), new ExampleConfig()));

        // When

        // Then
        assertThat(new FilterChainFactory(pfr, list)).isNotNull();
    }

    @Test
    void shouldPassValidationIfOptionalConfigIsMissing() {
        // Given
        var list = List.of(new NamedFilterDefinition("myValidFilterDef", OptionalConfigFactory.class.getName(), null));

        // When

        // Then
        assertThat(new FilterChainFactory(pfr, list)).isNotNull();
    }

    private ListAssert<FilterAndInvoker> assertFiltersCreated(List<NamedFilterDefinition> nameFilterDefinitions) {
        try (FilterChainFactory filterChainFactory = new FilterChainFactory(pfr, nameFilterDefinitions)) {
            NettyFilterContext context = new NettyFilterContext(eventLoop, pfr);
            List<FilterAndInvoker> filters = filterChainFactory.createFilters(context, nameFilterDefinitions);
            return assertThat(filters).hasSameSizeAs(nameFilterDefinitions);
        }
    }

    static class Counter {
        int count = 0;

        public void increment(FlakyConfig ignored) {
            count++;
        }
    }

    @Test
    void shouldPropagateFilterInitializeException() {
        // Given
        var onInitialize1 = new Counter();
        var onClose1 = new Counter();
        var onInitialize2 = new Counter();
        var onClose2 = new Counter();
        List<NamedFilterDefinition> list = List.of(
                new NamedFilterDefinition("flakyFilterDef1", FlakyFactory.class.getName(), new FlakyConfig(null, null, null,
                        onInitialize1::increment, onClose1::increment)),
                new NamedFilterDefinition("flakyFilterDef2", FlakyFactory.class.getName(), new FlakyConfig("foo", null, null,
                        onInitialize2::increment, onClose2::increment)));

        // When

        // Then
        assertThatThrownBy(() -> new FilterChainFactory(pfr, list))
                .isExactlyInstanceOf(PluginConfigurationException.class)
                .cause()
                .isExactlyInstanceOf(RuntimeException.class)
                .hasMessage("foo");
        assertThat(onInitialize1.count).isEqualTo(1);
        assertThat(onClose1.count).isEqualTo(1);
        assertThat(onInitialize2.count).isZero();
        assertThat(onClose2.count).isZero();
    }

    @Test
    void shouldPropagateFilterCreateException() {
        // Given
        var onInitialize1 = new Counter();
        var onClose1 = new Counter();
        var onInitialize2 = new Counter();
        var onClose2 = new Counter();
        List<NamedFilterDefinition> list = List.of(
                new NamedFilterDefinition("flakyFilterDef1", FlakyFactory.class.getName(), new FlakyConfig(null, null, null,
                        onInitialize1::increment, onClose1::increment)),
                new NamedFilterDefinition("flakyFilterDef2", FlakyFactory.class.getName(), new FlakyConfig(null, "foo", null,
                        onInitialize2::increment, onClose2::increment)));
        NettyFilterContext context = new NettyFilterContext(eventLoop, pfr);

        try (var fcf = new FilterChainFactory(pfr, list)) {
            assertThat(onInitialize1.count).isEqualTo(1);
            assertThat(onClose1.count).isZero();
            assertThat(onInitialize2.count).isEqualTo(1);
            assertThat(onClose2.count).isZero();
            // When

            // Then
            assertThatThrownBy(() -> fcf.createFilters(context, list))
                    .isExactlyInstanceOf(PluginConfigurationException.class)
                    .cause()
                    .isExactlyInstanceOf(RuntimeException.class)
                    .hasMessage("foo");

            assertThat(onInitialize1.count).isEqualTo(1);
            assertThat(onClose1.count).isZero();
            assertThat(onInitialize2.count).isEqualTo(1);
            assertThat(onClose2.count).isZero();
        }
        assertThat(onInitialize1.count).isEqualTo(1);
        assertThat(onClose1.count).isEqualTo(1);
        assertThat(onInitialize2.count).isEqualTo(1);
        assertThat(onClose2.count).isEqualTo(1);

    }

    @Test
    void shouldPropagateFilterCloseException() {
        // Given
        var onInitialize1 = new Counter();
        var onClose1 = new Counter();
        var onInitialize2 = new Counter();
        var onClose2 = new Counter();
        List<FlakyConfig> initializeOrder = new ArrayList<>();
        List<FlakyConfig> closeOrder = new ArrayList<>();
        FlakyConfig flakyConfig1 = new FlakyConfig(null, null, null,
                ((Consumer<FlakyConfig>) onInitialize1::increment).andThen(initializeOrder::add),
                ((Consumer<FlakyConfig>) onClose1::increment).andThen(closeOrder::add));
        FlakyConfig flakyConfig2 = new FlakyConfig(null, null, "foo",
                ((Consumer<FlakyConfig>) onInitialize2::increment).andThen(initializeOrder::add),
                ((Consumer<FlakyConfig>) onClose2::increment).andThen(closeOrder::add));
        List<NamedFilterDefinition> list = List.of(
                new NamedFilterDefinition("flakyFilterDef1", FlakyFactory.class.getName(), flakyConfig1),
                new NamedFilterDefinition("flakyFilterDef2", FlakyFactory.class.getName(), flakyConfig2));
        try (var fcf = new FilterChainFactory(pfr, list)) {
            // When
            assertThat(onInitialize1.count).isEqualTo(1);
            assertThat(onClose1.count).isZero();
            assertThat(onInitialize2.count).isEqualTo(1);
            assertThat(onClose2.count).isZero();
            assertThat(initializeOrder).isEqualTo(List.of(flakyConfig1, flakyConfig2));

            // Then
            assertThatThrownBy(fcf::close)
                    .isExactlyInstanceOf(RuntimeException.class)
                    .hasMessage("foo");
            assertThat(onInitialize1.count).isEqualTo(1);
            assertThat(onClose1.count).isEqualTo(1);
            assertThat(onInitialize2.count).isEqualTo(1);
            assertThat(onClose2.count).isEqualTo(1);
        }
        assertThat(onInitialize1.count).isEqualTo(1);
        assertThat(onClose1.count).isEqualTo(1);
        assertThat(onInitialize2.count).isEqualTo(1);
        assertThat(onClose2.count).isEqualTo(1);

        assertThat(closeOrder).isEqualTo(List.of(flakyConfig2, flakyConfig1));
    }
}
