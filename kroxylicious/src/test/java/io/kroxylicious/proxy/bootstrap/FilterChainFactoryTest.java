/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.bootstrap;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.filter.FilterAndInvoker;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FilterChainFactoryTest {

    @Test
    public void testNullFiltersInConfigResultsInEmptyList() {
        FilterChainFactory filterChainFactory = new FilterChainFactory(new Configuration(null, null, null, null, true));
        List<FilterAndInvoker> filters = filterChainFactory.createFilters();
        assertNotNull(filters, "Filters list should not be null");
        assertTrue(filters.isEmpty(), "Filters list should be empty");
    }

    @Test
    public void testEmptyFiltersInConfigResultsInEmptyList() {
        FilterChainFactory filterChainFactory = new FilterChainFactory(new Configuration(null, null, List.of(), null, true));
        List<FilterAndInvoker> filters = filterChainFactory.createFilters();
        assertNotNull(filters, "Filters list should not be null");
        assertTrue(filters.isEmpty(), "Filters list should be empty");
    }

}