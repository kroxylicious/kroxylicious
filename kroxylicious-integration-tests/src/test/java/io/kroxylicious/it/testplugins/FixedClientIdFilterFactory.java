/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it.testplugins;

import io.kroxylicious.it.testplugins.FixedClientIdFilter.FixedClientIdFilterConfig;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.Plugins;

import edu.umd.cs.findbugs.annotations.NonNull;

@Plugin(configType = FixedClientIdFilterConfig.class)
public class FixedClientIdFilterFactory implements FilterFactory<FixedClientIdFilterConfig, FixedClientIdFilterConfig> {

    @Override
    public FixedClientIdFilterConfig initialize(FilterFactoryContext context, FixedClientIdFilterConfig config) {
        return Plugins.requireConfig(this, config);
    }

    @NonNull
    @Override
    public FixedClientIdFilter createFilter(FilterFactoryContext context, FixedClientIdFilterConfig configuration) {
        return new FixedClientIdFilter(configuration);
    }

}
