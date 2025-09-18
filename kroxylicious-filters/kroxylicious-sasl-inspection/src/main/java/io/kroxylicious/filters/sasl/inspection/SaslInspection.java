/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters.sasl.inspection;

import java.util.Objects;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import edu.umd.cs.findbugs.annotations.Nullable;

@Plugin(configType = Config.class)
public class SaslInspection implements FilterFactory<Config, Config> {

    @Override
    public Config initialize(FilterFactoryContext context,
                             @Nullable Config config)
            throws PluginConfigurationException {
        return config == null ? new Config(null) : config;
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, Config config) {
        Objects.requireNonNull(config);
        return new SaslInspectionFilter(config);
    }
}
