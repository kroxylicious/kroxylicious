/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import io.kroxylicious.proxy.filter.RequestResponseMarkingFilter.RequestResponseMarkingFilterConfig;

public class RequestResponseMarkingFilterFactory implements FilterFactory<RequestResponseMarkingFilter, RequestResponseMarkingFilterConfig> {

    @Override
    public RequestResponseMarkingFilter createFilter(FilterCreationContext context,
                                                     RequestResponseMarkingFilterConfig configuration) {
        return new RequestResponseMarkingFilter(context, configuration);
    }

    @Override
    public Class<RequestResponseMarkingFilter> filterType() {
        return RequestResponseMarkingFilter.class;
    }

    @Override
    public Class<RequestResponseMarkingFilterConfig> configType() {
        return RequestResponseMarkingFilterConfig.class;
    }

}
