/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.util.UUID;

import io.kroxylicious.proxy.filter.CreateTopicsRequestFilter;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.plugin.Plugins;

import edu.umd.cs.findbugs.annotations.NonNull;

@Plugin(configType = CreateTopicRequest.Config.class)
public class CreateTopicRequest implements FilterFactory<CreateTopicRequest.Config, CreateTopicRequest.Config> {

    @Override
    public Config initialize(FilterFactoryContext context, Config config) throws PluginConfigurationException {
        Plugins.requireConfig(this, config);
        return config;
    }

    @NonNull
    @Override
    public Filter createFilter(FilterFactoryContext context, Config initializationData) {
        return (CreateTopicsRequestFilter) (apiKey, header, request, requestFilterContext) -> requestFilterContext.forwardRequest(header, request);
    }

    public record Config(UUID configInstanceId) {

    }

}
