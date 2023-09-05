/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.bootstrap;

import java.util.List;
import java.util.Objects;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.FilterDefinition;
import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.filter.FilterContributor;
import io.kroxylicious.proxy.internal.filter.FilterContributorManager;
import io.kroxylicious.proxy.internal.filter.TestFilterContributor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FilterChainFactoryTest {

    private FilterContributorManager testFilterContributorManager;

    @BeforeEach
    void setUp() {
        final List<FilterContributor> testFilterContributors = List.of(new TestFilterContributor());
        testFilterContributorManager = new FilterContributorManager(testFilterContributors::iterator);
    }

    @Test
    void testNullFiltersInConfigResultsInEmptyList() {
        FilterChainFactory filterChainFactory = new FilterChainFactory(new Configuration(null, null, null, null, true), FilterContributorManager.getInstance());
        List<FilterAndInvoker> filters = filterChainFactory.createFilters();
        assertNotNull(filters, "Filters list should not be null");
        assertTrue(filters.isEmpty(), "Filters list should be empty");
    }

    @Test
    void testEmptyFiltersInConfigResultsInEmptyList() {
        FilterChainFactory filterChainFactory = new FilterChainFactory(new Configuration(null, null, List.of(), null, true), FilterContributorManager.getInstance());
        List<FilterAndInvoker> filters = filterChainFactory.createFilters();
        assertNotNull(filters, "Filters list should not be null");
        assertTrue(filters.isEmpty(), "Filters list should be empty");
    }

    @Test
    void shouldConstructFilterWithoutConfig() {
        // Given
        final FilterDefinition filterWithoutConfig = new FilterDefinition(TestFilterContributor.NO_CONFIG_REQUIRED_TYPE_NAME, null);
        FilterChainFactory filterChainFactory = new FilterChainFactory(new Configuration(null, null, List.of(filterWithoutConfig), null, true),
                testFilterContributorManager);

        // When
        final List<FilterAndInvoker> actualFilters = filterChainFactory.createFilters();

        // Then
        assertThat(actualFilters).hasSize(1)
                .allMatch(Objects::nonNull)
                .allSatisfy(filterAndInvoker -> assertThat(filterAndInvoker.filter()).isInstanceOf(TestFilterContributor.NoConfigFilter.class));
    }

    @Test
    void shouldConstructFilterWithConfig() {
        // Given
        final FilterDefinition filterWithoutConfig = new FilterDefinition(TestFilterContributor.CONFIG_REQUIRED_TYPE_NAME,
                new TestFilterContributor.ConfigRequiredFilter.Config("wibble"));
        FilterChainFactory filterChainFactory = new FilterChainFactory(new Configuration(null, null, List.of(filterWithoutConfig), null, true),
                testFilterContributorManager);

        // When
        final List<FilterAndInvoker> actualFilters = filterChainFactory.createFilters();

        // Then
        assertThat(actualFilters).hasSize(1)
                .allMatch(Objects::nonNull)
                .allSatisfy(filterAndInvoker -> assertThat(filterAndInvoker.filter()).isInstanceOf(TestFilterContributor.ConfigRequiredFilter.class));
    }

    @Test
    void shouldNotConstructFilterWithMissingConfig() {
        // Given
        final FilterDefinition filterWithoutConfig = new FilterDefinition(TestFilterContributor.CONFIG_REQUIRED_TYPE_NAME, null);
        FilterChainFactory filterChainFactory = new FilterChainFactory(new Configuration(null, null, List.of(filterWithoutConfig), null, true),
                testFilterContributorManager);

        // When
        final IllegalStateException actualException = assertThrows(IllegalStateException.class, filterChainFactory::createFilters);

        // Then
        assertThat(actualException).hasStackTraceContaining("Missing required config for [" + TestFilterContributor.CONFIG_REQUIRED_TYPE_NAME + "]");
    }

}