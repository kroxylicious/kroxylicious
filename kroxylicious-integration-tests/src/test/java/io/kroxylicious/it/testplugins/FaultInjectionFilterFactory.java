/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it.testplugins;

import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;

/**
 * Factory for {@link FaultInjectionFilter}. The filter starts in pass-through
 * mode; tests control it via the {@link FaultInjectionFilter.Handle} returned
 * from {@link #latestHandle()}.
 */
@Plugin(configType = Void.class)
public class FaultInjectionFilterFactory
        implements FilterFactory<Void, Void> {

    private static volatile FaultInjectionFilter.Handle latestHandle;

    @Override
    public Void initialize(FilterFactoryContext context,
                           Void config) {
        return config;
    }

    @Override
    public FaultInjectionFilter createFilter(FilterFactoryContext context,
                                             Void configuration) {
        var filter = new FaultInjectionFilter();
        latestHandle = filter.handle();
        return filter;
    }

    /**
     * Returns the handle for the most recently created filter instance.
     * Call after the proxy has started and a client has connected.
     */
    public static FaultInjectionFilter.Handle latestHandle() {
        return latestHandle;
    }

    public static void reset() {
        latestHandle = null;
    }
}
