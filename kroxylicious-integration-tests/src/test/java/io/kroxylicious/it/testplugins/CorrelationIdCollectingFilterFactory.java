/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it.testplugins;

import io.kroxylicious.it.testplugins.CorrelationIdCollectingFilter.Config;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.Plugins;

import edu.umd.cs.findbugs.annotations.NonNull;

@Plugin(configType = Config.class)
public class CorrelationIdCollectingFilterFactory implements FilterFactory<Config, Config> {

    @Override
    public Config initialize(FilterFactoryContext context, Config config) {
        return Plugins.requireConfig(this, config);
    }

    @NonNull
    @Override
    public CorrelationIdCollectingFilter createFilter(FilterFactoryContext context, Config configuration) {
        return new CorrelationIdCollectingFilter(configuration);
    }
}
