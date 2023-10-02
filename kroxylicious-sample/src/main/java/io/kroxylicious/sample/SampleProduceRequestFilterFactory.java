/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sample;

import io.kroxylicious.proxy.filter.FilterCreationContext;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.sample.config.SampleFilterConfig;

public class SampleProduceRequestFilterFactory implements FilterFactory<SampleProduceRequestFilter, SampleFilterConfig> {

    @Override
    public Class<SampleProduceRequestFilter> filterType() {
        return SampleProduceRequestFilter.class;
    }

    @Override
    public Class<SampleFilterConfig> configType() {
        return SampleFilterConfig.class;
    }

    @Override
    public SampleProduceRequestFilter createFilter(FilterCreationContext context, SampleFilterConfig configuration) {
        return new SampleProduceRequestFilter(configuration);
    }
}
