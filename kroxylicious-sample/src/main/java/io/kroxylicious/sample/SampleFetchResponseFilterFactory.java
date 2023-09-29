/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sample;

import io.kroxylicious.proxy.filter.FilterCreationContext;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.sample.config.SampleFilterConfig;

public class SampleFetchResponseFilterFactory extends FilterFactory<SampleFetchResponseFilter, SampleFilterConfig> {

    public SampleFetchResponseFilterFactory() {
        super(SampleFilterConfig.class, SampleFetchResponseFilter.class);
    }

    @Override
    public SampleFetchResponseFilter createFilter(FilterCreationContext context, SampleFilterConfig configuration) {
        return new SampleFetchResponseFilter(configuration);
    }

}
