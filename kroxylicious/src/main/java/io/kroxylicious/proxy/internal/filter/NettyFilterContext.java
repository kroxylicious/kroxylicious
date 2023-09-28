/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.util.concurrent.ScheduledExecutorService;

import io.kroxylicious.proxy.filter.FilterCreationContext;

public class NettyFilterContext implements FilterCreationContext {
    private final ScheduledExecutorService eventLoop;

    public NettyFilterContext(ScheduledExecutorService eventLoop) {
        this.eventLoop = eventLoop;
    }

    @Override
    public ScheduledExecutorService eventLoop() {
        return eventLoop;
    }
}