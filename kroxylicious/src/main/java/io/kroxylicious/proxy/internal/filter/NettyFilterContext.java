/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.util.concurrent.ScheduledExecutorService;

import io.kroxylicious.proxy.filter.FilterConstructContext;
import io.kroxylicious.proxy.filter.FilterExecutors;

public class NettyFilterContext {
    private final ScheduledExecutorService eventLoop;

    public NettyFilterContext(ScheduledExecutorService eventLoop) {
        this.eventLoop = eventLoop;
    }

    public FilterConstructContext wrap(Object config) {
        return new NettyFilterConfigContext(config);
    }

    class NettyFilterConfigContext implements FilterConstructContext {
        private final Object filterConfig;

        NettyFilterConfigContext(Object filterConfig) {
            this.filterConfig = filterConfig;
        }

        @Override
        public FilterExecutors executors() {
            return () -> eventLoop;
        }

        @Override
        public Object getConfig() {
            return filterConfig;
        }
    }
}