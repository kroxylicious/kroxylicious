/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import io.kroxylicious.proxy.filter.OutOfBandSendFilter.OutOfBandSendFilterConfig;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.Plugins;

@Plugin(configType = OutOfBandSendFilterConfig.class)
public class OutOfBandSendFilterFactory implements FilterFactory<OutOfBandSendFilterConfig, OutOfBandSendFilterConfig> {

    @Override
    public OutOfBandSendFilterConfig initialize(FilterFactoryContext context, OutOfBandSendFilterConfig config) {
        return Plugins.requireConfig(this, config);
    }

    @Override
    public OutOfBandSendFilter createFilter(FilterFactoryContext context, OutOfBandSendFilterConfig configuration) {
        return new OutOfBandSendFilter(configuration);
    }

}
