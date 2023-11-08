/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.List;

/**
 * Filters can implement this when they are composed of multiple discrete
 * filters. If a Filter implements CompositeFilter it cannot implement
 * {@link RequestFilter}, {@link ResponseFilter} or any of the specific
 * message Filter interfaces like {@link ApiVersionsRequestFilter}.
 *<p>
 * This is useful if you want to create smaller component filters that are
 * individually testable, but only require a single entry in the
 * kroxylicious configuration to install your behaviour as a whole. An
 * example would be if you want to use a {@link RequestFilter} to modify
 * all clientId headers, but also want to modify some specific RPCs (like
 * mutate produce messages with an {@link ProduceRequestFilter} implementation).
 * The interfaces are incompatible so you must implement two different filter
 * classes. The CompositeFilter enables you to install both filters with a single
 * user configuration.
 *</p>
 * @deprecated additional complexity not worth it at this point
 */
@Deprecated(since = "0.3.0", forRemoval = true)
public interface CompositeFilter extends Filter {

    /**
     * Get composed Filters
     * @return filters
     */
    List<Filter> getFilters();

}
