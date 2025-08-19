/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.testplugins;

import org.apache.kafka.common.errors.UnknownServerException;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

/**
 * Responds to all requests with an unknown server exception
 */
@Plugin(configType = Void.class)
public class ShortCircuitErrorResponse implements FilterFactory<Void, Void> {
    @Override
    public Void initialize(FilterFactoryContext context, Void config) throws PluginConfigurationException {
        return null;
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, Void initializationData) {
        return (RequestFilter) (apiKey, header, request, context1) -> context1.requestFilterResultBuilder()
                .errorResponse(header, request, new UnknownServerException(ShortCircuitErrorResponse.class.getName() + ": responding error to all requests"))
                .completed();
    }
}
