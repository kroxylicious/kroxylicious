/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.plugin.PluginImplConfig;

import edu.umd.cs.findbugs.annotations.NonNull;

@Plugin(configType = MissingPluginImplName.Config.class)
public class MissingPluginImplName implements FilterFactory<MissingPluginImplName.Config, Void> {

    record Config(
            String id, // This lacks the @PluginImplName annotation
            @PluginImplConfig(implNameProperty = "id")
            Object config
    ) {
    }

    @Override
    public Void initialize(FilterFactoryContext context, Config config) throws PluginConfigurationException {
        return null;
    }

    @NonNull
    @Override
    public Filter createFilter(FilterFactoryContext context, Void initializationData) {
        return null;
    }
}
