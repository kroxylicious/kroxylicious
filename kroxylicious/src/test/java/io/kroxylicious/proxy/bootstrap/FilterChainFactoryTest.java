/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.bootstrap;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.filter.FilterContributorContext;

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

}
