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
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.FilterDefinition;
import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.internal.filter.ExampleConfig;
import io.kroxylicious.proxy.internal.filter.NettyFilterContext;
import io.kroxylicious.proxy.internal.filter.TestFilter;
import io.kroxylicious.proxy.internal.filter.TestFilterContributor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FilterChainFactoryTest {

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
        ScheduledExecutorService eventLoop = Executors.newScheduledThreadPool(1);
        FilterChainFactory filterChainFactory = new FilterChainFactory(new Configuration(null, null, List.of(), null, true));
        List<FilterAndInvoker> filters = filterChainFactory.createFilters(new NettyFilterContext(eventLoop));
        assertNotNull(filters, "Filters list should not be null");
        assertTrue(filters.isEmpty(), "Filters list should be empty");
    }

    @Test
    void testCreateFilter() {
        ScheduledExecutorService eventLoop = Executors.newScheduledThreadPool(1);
        ExampleConfig config = new ExampleConfig();
        FilterChainFactory filterChainFactory = new FilterChainFactory(
                new Configuration(null, null, List.of(new FilterDefinition(TestFilterContributor.SHORT_NAME_A, config)), null, true));
        NettyFilterContext context = new NettyFilterContext(eventLoop);
        List<FilterAndInvoker> filters = filterChainFactory.createFilters(context);
        assertThat(filters).isNotNull().hasSize(1).first().extracting(FilterAndInvoker::filter).isInstanceOfSatisfying(TestFilter.class, testFilter -> {
            assertThat(testFilter.getShortName()).isEqualTo(TestFilterContributor.SHORT_NAME_A);
            assertThat(testFilter.getContext().getConfig()).isSameAs(config);
            assertThat(testFilter.getContext().executors().eventLoop()).isSameAs(eventLoop);
            assertThat(testFilter.getExampleConfig()).isSameAs(config);
        });
    }

    @Test
    void testCreateFilters() {
        ScheduledExecutorService eventLoop = Executors.newScheduledThreadPool(1);
        ExampleConfig config = new ExampleConfig();
        FilterDefinition definitionA = new FilterDefinition(TestFilterContributor.SHORT_NAME_A, config);
        FilterDefinition definitionB = new FilterDefinition(TestFilterContributor.SHORT_NAME_B, config);
        List<FilterDefinition> filterDefinitions = List.of(definitionA, definitionB);
        FilterChainFactory filterChainFactory = new FilterChainFactory(new Configuration(null, null, filterDefinitions, null, true));
        NettyFilterContext context = new NettyFilterContext(eventLoop);
        List<FilterAndInvoker> filters = filterChainFactory.createFilters(context);
        ListAssert<FilterAndInvoker> listAssert = assertThat(filters).isNotNull().hasSize(2);
        listAssert.element(0).extracting(FilterAndInvoker::filter).isInstanceOfSatisfying(TestFilter.class, testFilter -> {
            assertThat(testFilter.getShortName()).isEqualTo(TestFilterContributor.SHORT_NAME_A);
            assertThat(testFilter.getContext().getConfig()).isSameAs(config);
            assertThat(testFilter.getContext().executors().eventLoop()).isSameAs(eventLoop);
            assertThat(testFilter.getExampleConfig()).isSameAs(config);
        });
        listAssert.element(1).extracting(FilterAndInvoker::filter).isInstanceOfSatisfying(TestFilter.class, testFilter -> {
            assertThat(testFilter.getShortName()).isEqualTo(TestFilterContributor.SHORT_NAME_B);
            assertThat(testFilter.getContext().getConfig()).isSameAs(config);
            assertThat(testFilter.getContext().executors().eventLoop()).isSameAs(eventLoop);
            assertThat(testFilter.getExampleConfig()).isSameAs(config);
        });
    }

}
