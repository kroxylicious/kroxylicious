/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import io.kroxylicious.proxy.filter.OutOfBandSendFilter.OutOfBandSendFilterConfig;

public class OutOfBandSendFilterFactory extends FilterFactory<OutOfBandSendFilter, OutOfBandSendFilterConfig> {

    public OutOfBandSendFilterFactory() {
        super(OutOfBandSendFilterConfig.class, OutOfBandSendFilter.class);
    }

    @Override
    public OutOfBandSendFilter createFilter(FilterCreationContext context, OutOfBandSendFilterConfig configuration) {
        return new OutOfBandSendFilter(configuration);
    }
}
