/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.util.Set;

import io.netty.channel.EventLoop;

import io.kroxylicious.proxy.config.PluginFactory;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.filter.FilterDispatchExecutor;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.internal.NettyFilterDispatchExecutor;

/**
 * A {@link FilterFactoryContext} backed by a Netty event loop, giving filter factories
 * access to a {@link FilterDispatchExecutor} for the connection's event loop and to
 * plugin instances via the {@link PluginFactoryRegistry}.
 */
public class NettyFilterContext implements FilterFactoryContext {
    private final FilterDispatchExecutor dispatchExecutor;
    private final PluginFactoryRegistry pluginFactoryRegistry;

    /**
     * Creates a filter factory context.
     *
     * @param eventLoop the Netty event loop backing the filter dispatch executor
     * @param pluginFactoryRegistry registry used to resolve plugin instances
     */
    public NettyFilterContext(EventLoop eventLoop,
                              PluginFactoryRegistry pluginFactoryRegistry) {
        this.dispatchExecutor = NettyFilterDispatchExecutor.eventLoopExecutor(eventLoop);
        this.pluginFactoryRegistry = pluginFactoryRegistry;
    }

    @Override
    public FilterDispatchExecutor filterDispatchExecutor() {
        return dispatchExecutor;
    }

    @Override
    public <P> P pluginInstance(Class<P> pluginClass, String implementationName) {
        PluginFactory<P> pluginFactory = pluginFactory(pluginClass);
        return pluginFactory.pluginInstance(implementationName);
    }

    @Override
    public <P> Set<String> pluginImplementationNames(Class<P> pluginClass) {
        return pluginFactory(pluginClass).registeredInstanceNames();
    }

    private <P> PluginFactory<P> pluginFactory(Class<P> pluginClass) {
        return pluginFactoryRegistry.pluginFactory(pluginClass);
    }

}
