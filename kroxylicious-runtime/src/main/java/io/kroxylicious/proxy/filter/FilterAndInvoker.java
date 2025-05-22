/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * A Filter and it's respective invoker
 * @param filterName The name of the filter (from the config)
 * @param filter filter The filter instance
 * @param invoker invoker The invoker
 */
public record FilterAndInvoker(String filterName, Filter filter, FilterInvoker invoker) {

    /**
     * A Filter and it's respective invoker
     * @param filter filter (non-nullable)
     * @param invoker invoker (non-nullable)
     */
    public FilterAndInvoker {
        requireNonNull(filter, "filter cannot be null");
        requireNonNull(invoker, "invoker cannot be null");
    }

    /**
     * Builds a list of invokers for a filter
     * @param filter filter
     * @return a filter and its respective invoker
     */
    public static List<FilterAndInvoker> build(String filterName, Filter filter) {
        return FilterInvokers.from(filterName, filter);
    }

    @Override
    public String toString() {
        return "FilterAndInvoker{" +
                "filter='" + filterName + '\'' +
                ", filterClass='" + filter.getClass().getSimpleName() + '\'' +
                ", invoker=" + invoker +
                '}';
    }
}
