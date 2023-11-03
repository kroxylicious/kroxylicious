/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.Plugins;

@Plugin(configType = RequestResponseMarkingFilter.RequestResponseMarkingFilterConfig.class)
public class RequestResponseMarkingFilterFactory
        implements FilterFactory<RequestResponseMarkingFilter.RequestResponseMarkingFilterConfig, RequestResponseMarkingFilter.RequestResponseMarkingFilterConfig> {

    @Override
    public RequestResponseMarkingFilter.RequestResponseMarkingFilterConfig initialize(FilterFactoryContext context,
                                                                                      RequestResponseMarkingFilter.RequestResponseMarkingFilterConfig config) {
        return Plugins.requireConfig(this, config);
    }

    @Override
    public RequestResponseMarkingFilter createFilter(FilterFactoryContext context,
                                                     RequestResponseMarkingFilter.RequestResponseMarkingFilterConfig configuration) {
        return new RequestResponseMarkingFilter(context, configuration);
    }

    public enum Direction {
        REQUEST,
        RESPONSE;
    }

}
