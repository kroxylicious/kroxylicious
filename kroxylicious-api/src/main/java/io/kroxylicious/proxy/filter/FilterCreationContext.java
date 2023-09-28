/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

/**
 * Construction context for Filters. Used to pass the filter configuration and enviromental resources
 * to the FilterFactory when it is creating a new instance of the Filter. see {@link FilterFactory#createFilter(FilterCreationContext, Object)}
 */
public interface FilterCreationContext {

    /**
     * Offers Executors for doing asynchronous work
     * @return executors
     */
    FilterExecutors executors();

}
