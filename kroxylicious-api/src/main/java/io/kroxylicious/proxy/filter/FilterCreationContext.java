/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Construction context for Filters. Used to pass the filter configuration and environmental resources
 * to the FilterFactory when it is creating a new instance of the Filter. see {@link FilterFactory#createFilter(FilterCreationContext, Object)}
 */
public interface FilterCreationContext {

    /**
     * The event loop that this Filter's channel is assigned to. Should be safe
     * to mutate Filter members from this executor.
     * @return executor
     */
    ScheduledExecutorService eventLoop();

}
