/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.bootstrap;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.assertj.core.api.ListAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.FilterDefinition;
import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.internal.filter.ExampleConfig;
import io.kroxylicious.proxy.internal.filter.NettyFilterContext;
import io.kroxylicious.proxy.internal.filter.OptionalConfigFilter;
import io.kroxylicious.proxy.internal.filter.TestFilter;
import io.kroxylicious.proxy.internal.filter.TestFilterFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FilterChainFactoryTest {

    private ScheduledExecutorService eventLoop;
    private ExampleConfig config;

    @BeforeEach
    void setUp() {
        eventLoop = Executors.newScheduledThreadPool(1);
        config = new ExampleConfig();
    }

    @Test
    void testNullFiltersInConfigResultsInEmptyList() {
        ScheduledExecutorService eventLoop = Executors.newScheduledThreadPool(1);
        FilterChainFactory filterChainFactory = new FilterChainFactory(new Configuration(null, null, null, null, true));
        List<FilterAndInvoker> filters = filterChainFactory.createFilters(new NettyFilterContext(eventLoop));
        assertNotNull(filters, "Filters list should not be null");
        assertTrue(filters.isEmpty(), "Filters list should be empty");
    }

    @Test
    void testConfigurationNotNullable() {
        assertThatThrownBy(() -> new FilterChainFactory(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void testEmptyFiltersInConfigResultsInEmptyList() {
        assertFiltersCreated(List.of());
    }

    @Test
    void testCreateFilter() {
        final ListAssert<FilterAndInvoker> listAssert = assertFiltersCreated(List.of(new FilterDefinition(TestFilter.class.getName(), config)));
        listAssert.first().extracting(FilterAndInvoker::filter).isInstanceOfSatisfying(TestFilter.class, testFilter -> {
            assertThat(testFilter.getContributorClass()).isEqualTo(TestFilterFactory.class);
            assertThat(testFilter.getContext().eventLoop()).isSameAs(eventLoop);
            assertThat(testFilter.getExampleConfig()).isSameAs(config);
        });
    }

    @Test
    void testCreateFilters() {
        final ListAssert<FilterAndInvoker> listAssert = assertFiltersCreated(List.of(new FilterDefinition(TestFilter.class.getName(), config),
                new FilterDefinition(TestFilter.class.getName(), config)));
        listAssert.element(0).extracting(FilterAndInvoker::filter).isInstanceOfSatisfying(TestFilter.class, testFilter -> {
            assertThat(testFilter.getContributorClass()).isEqualTo(TestFilterFactory.class);
            assertThat(testFilter.getContext().eventLoop()).isSameAs(eventLoop);
            assertThat(testFilter.getExampleConfig()).isSameAs(config);
        });
        listAssert.element(1).extracting(FilterAndInvoker::filter).isInstanceOfSatisfying(TestFilter.class, testFilter -> {
            assertThat(testFilter.getContributorClass()).isEqualTo(TestFilterFactory.class);
            assertThat(testFilter.getContext().eventLoop()).isSameAs(eventLoop);
            assertThat(testFilter.getExampleConfig()).isSameAs(config);
        });
    }

    @Test
    void shouldReturnInvalidFilterNameIfFilterRequiresConfigAndNoneIsSupplied() {
        // Given
        final List<FilterDefinition> filters = List.of(new FilterDefinition(TestFilter.class.getName(), config),
                new FilterDefinition(TestFilter.class.getName(), null));

        // When
        final Set<String> invalidFilters = FilterChainFactory.validateFilterConfiguration(filters);

        // Then
        assertThat(invalidFilters).containsOnly(TestFilter.class.getName());
    }

    @Test
    void shouldReturnInvalidFilterNamesForAllFiltersWithoutRequiredConfig() {
        // Given
        final List<FilterDefinition> filters = List.of(new FilterDefinition(TestFilter.class.getName(), null),
                new FilterDefinition(TestFilter.class.getName(), null),
                new FilterDefinition(OptionalConfigFilter.class.getName(), null));

        // When
        final Set<String> invalidFilters = FilterChainFactory.validateFilterConfiguration(filters);

        // Then
        assertThat(invalidFilters).containsOnly(TestFilter.class.getName(),
                TestFilter.class.getName());
    }

    @Test
    void shouldPassValidationWhenAllFiltersHaveConfiguration() {
        // Given
        final List<FilterDefinition> filterDefinitions = List.of(new FilterDefinition(TestFilter.class.getName(), config),
                new FilterDefinition(TestFilter.class.getName(), config));

        // When
        final Set<String> invalidFilters = FilterChainFactory.validateFilterConfiguration(filterDefinitions);

        // Then
        assertThat(invalidFilters).isEmpty();
    }

    @Test
    void shouldPassValidationWhenFiltersWithOptionalConfigurationAreMissingConfiguration() {
        // Given
        final List<FilterDefinition> filterDefinitions = List.of(new FilterDefinition(TestFilter.class.getName(), config),
                new FilterDefinition(TestFilter.class.getName(), config),
                new FilterDefinition(OptionalConfigFilter.class.getName(), null));

        // When
        final Set<String> invalidFilters = FilterChainFactory.validateFilterConfiguration(filterDefinitions);

        // Then
        assertThat(invalidFilters).isEmpty();
    }

    private ListAssert<FilterAndInvoker> assertFiltersCreated(List<FilterDefinition> filterDefinitions) {
        FilterChainFactory filterChainFactory = new FilterChainFactory(new Configuration(null, null, filterDefinitions, null, true));
        NettyFilterContext context = new NettyFilterContext(eventLoop);
        List<FilterAndInvoker> filters = filterChainFactory.createFilters(context);
        return assertThat(filters).isNotNull().hasSize(filterDefinitions.size());
    }

}
