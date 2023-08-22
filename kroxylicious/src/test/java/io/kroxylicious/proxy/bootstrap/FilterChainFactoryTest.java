/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.bootstrap;

import java.util.List;

import org.assertj.core.api.ListAssert;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.FilterDefinition;
import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.filter.FilterContributorContext;
import io.kroxylicious.proxy.internal.filter.Config;
import io.kroxylicious.proxy.internal.filter.TestFilterContributor;
import io.kroxylicious.proxy.internal.filter.TestKrpcFilter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FilterChainFactoryTest {

    @Test
    void testNullFiltersInConfigResultsInEmptyList() {
        FilterContributorContext mockContext = Mockito.mock(FilterContributorContext.class);
        FilterChainFactory filterChainFactory = new FilterChainFactory(new Configuration(null, null, null, null, true), mockContext);
        List<FilterAndInvoker> filters = filterChainFactory.createFilters();
        assertNotNull(filters, "Filters list should not be null");
        assertTrue(filters.isEmpty(), "Filters list should be empty");
    }

    @Test
    void testEmptyFiltersInConfigResultsInEmptyList() {
        FilterContributorContext mockContext = Mockito.mock(FilterContributorContext.class);
        FilterChainFactory filterChainFactory = new FilterChainFactory(new Configuration(null, null, List.of(), null, true), mockContext);
        List<FilterAndInvoker> filters = filterChainFactory.createFilters();
        assertNotNull(filters, "Filters list should not be null");
        assertTrue(filters.isEmpty(), "Filters list should be empty");
    }

    @Test
    void testSingleFilterConstruction() {
        FilterContributorContext mockContext = Mockito.mock(FilterContributorContext.class);
        Config config = new Config();
        FilterChainFactory filterChainFactory = new FilterChainFactory(new Configuration(null, null, List.of(new FilterDefinition(TestFilterContributor.SHORT_NAME,
                config)), null, true), mockContext);
        List<FilterAndInvoker> filters = filterChainFactory.createFilters();
        assertThat(filters).hasSize(1).first().extracting(FilterAndInvoker::filter).isInstanceOfSatisfying(TestKrpcFilter.class, testKrpcFilter -> {
            assertThat(testKrpcFilter.context()).isSameAs(mockContext);
            assertThat(testKrpcFilter.config()).isSameAs(config);
        });
    }

    @Test
    void testMultipleFilterConstruction() {
        FilterContributorContext mockContext = Mockito.mock(FilterContributorContext.class);
        Config configA = new Config();
        FilterDefinition definitionA = new FilterDefinition(TestFilterContributor.SHORT_NAME, configA);
        Config configB = new Config();
        FilterDefinition definitionB = new FilterDefinition(TestFilterContributor.SHORT_NAME, configB);
        FilterChainFactory filterChainFactory = new FilterChainFactory(new Configuration(null, null, List.of(definitionA, definitionB), null, true), mockContext);
        List<FilterAndInvoker> filters = filterChainFactory.createFilters();
        ListAssert<FilterAndInvoker> filterAndInvokerListAssert = assertThat(filters).hasSize(2);
        filterAndInvokerListAssert.element(0).extracting(FilterAndInvoker::filter).isInstanceOfSatisfying(TestKrpcFilter.class, testKrpcFilter -> {
            assertThat(testKrpcFilter.context()).isSameAs(mockContext);
            assertThat(testKrpcFilter.config()).isSameAs(configA);
        });
        filterAndInvokerListAssert.element(1).extracting(FilterAndInvoker::filter).isInstanceOfSatisfying(TestKrpcFilter.class, testKrpcFilter -> {
            assertThat(testKrpcFilter.context()).isSameAs(mockContext);
            assertThat(testKrpcFilter.config()).isSameAs(configB);
        });
    }

}
