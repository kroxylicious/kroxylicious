/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.util.concurrent.ScheduledExecutorService;

import io.kroxylicious.proxy.filter.FilterConstructContext;
import io.kroxylicious.proxy.filter.FilterExecutors;

public class NettyFilterContext implements FilterConstructContext {
    private final ScheduledExecutorService eventLoop;

    public NettyFilterContext(ScheduledExecutorService eventLoop) {
        this.eventLoop = eventLoop;
    }

    @Override
    public FilterExecutors executors() {
        return () -> eventLoop;
    }
}
