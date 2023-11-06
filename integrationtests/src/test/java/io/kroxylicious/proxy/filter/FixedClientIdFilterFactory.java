/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import io.kroxylicious.proxy.filter.FixedClientIdFilter.FixedClientIdFilterConfig;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.Plugins;

@Plugin(configType = FixedClientIdFilterConfig.class)
public class FixedClientIdFilterFactory implements FilterFactory<FixedClientIdFilterConfig, FixedClientIdFilterConfig> {

    @Override
    public FixedClientIdFilterConfig initialize(FilterFactoryContext context, FixedClientIdFilterConfig config) {
        return Plugins.requireConfig(this, config);
    }

    @Override
    public FixedClientIdFilter createFilter(FilterFactoryContext context, FixedClientIdFilterConfig configuration) {
        return new FixedClientIdFilter(configuration);
    }

}
