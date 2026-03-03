/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it.testplugins;

import io.kroxylicious.it.testplugins.OutOfBandSendFilter.OutOfBandSendFilterConfig;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.Plugins;

import edu.umd.cs.findbugs.annotations.NonNull;

@Plugin(configType = OutOfBandSendFilterConfig.class)
public class OutOfBandSendFilterFactory implements FilterFactory<OutOfBandSendFilterConfig, OutOfBandSendFilterConfig> {

    @Override
    public OutOfBandSendFilterConfig initialize(FilterFactoryContext context, OutOfBandSendFilterConfig config) {
        return Plugins.requireConfig(this, config);
    }

    @NonNull
    @Override
    public OutOfBandSendFilter createFilter(FilterFactoryContext context, OutOfBandSendFilterConfig configuration) {
        return new OutOfBandSendFilter(configuration);
    }

}
