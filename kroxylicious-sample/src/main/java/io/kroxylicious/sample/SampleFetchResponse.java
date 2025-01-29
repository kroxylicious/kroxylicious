/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sample;

import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.Plugins;
import io.kroxylicious.sample.config.SampleFilterConfig;

@Plugin(configType = SampleFilterConfig.class)
public class SampleFetchResponse implements FilterFactory<SampleFilterConfig, SampleFilterConfig> {

    @Override
    public SampleFilterConfig initialize(FilterFactoryContext context, SampleFilterConfig config) {
        return Plugins.requireConfig(this, config);
    }

    @Override
    public SampleFetchResponseFilter createFilter(FilterFactoryContext context, SampleFilterConfig configuration) {
        return new SampleFetchResponseFilter(configuration);
    }

}
