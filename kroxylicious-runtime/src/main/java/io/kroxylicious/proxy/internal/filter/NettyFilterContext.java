/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.util.concurrent.ScheduledExecutorService;

import io.kroxylicious.proxy.config.PluginFactory;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.filter.FilterFactoryContext;

import edu.umd.cs.findbugs.annotations.NonNull;

public class NettyFilterContext implements FilterFactoryContext {
    private final ScheduledExecutorService eventLoop;
    private final PluginFactoryRegistry pluginFactoryRegistry;

    public NettyFilterContext(ScheduledExecutorService eventLoop,
                              PluginFactoryRegistry pluginFactoryRegistry) {
        this.eventLoop = eventLoop;
        this.pluginFactoryRegistry = pluginFactoryRegistry;
    }

    @Override
    public ScheduledExecutorService eventLoop() {
        return eventLoop;
    }

    @NonNull
    @Override
    public <P> P pluginInstance(@NonNull Class<P> pluginClass, @NonNull String instanceName) {
        PluginFactory<P> pluginFactory = pluginFactoryRegistry.pluginFactory(pluginClass);
        return pluginFactory.pluginInstance(instanceName);
    }

}
