/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import io.kroxylicious.proxy.filter.OutOfBandSendFilter.OutOfBandSendFilterConfig;

public class OutOfBandSendFilterFactory implements FilterFactory<OutOfBandSendFilter, OutOfBandSendFilterConfig> {

    @Override
    public Class<OutOfBandSendFilter> filterType() {
        return OutOfBandSendFilter.class;
    }

    @Override
    public Class<OutOfBandSendFilterConfig> configType() {
        return OutOfBandSendFilterConfig.class;
    }

    @Override
    public OutOfBandSendFilter createFilter(FilterCreationContext context, OutOfBandSendFilterConfig configuration) {
        return new OutOfBandSendFilter(configuration);
    }
}
