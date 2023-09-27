/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Executors available to the Filter
 */
public interface FilterExecutors {

    /**
     * The event loop that this Filter's channel is assigned to. Should be safe
     * to mutate Filter members from this executor.
     * @return executor
     */
    ScheduledExecutorService eventLoop();
}
