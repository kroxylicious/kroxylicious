/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import io.kroxylicious.proxy.plugin.Plugin;

@Plugin(configType = Void.class)
public class ApiVersionsMarkingFilterFactory implements FilterFactory<Void, Void> {

    @Override
    public Void initialize(FilterFactoryContext context, Void config) {
        return null;
    }

    @Override
    public ApiVersionsMarkingFilter createFilter(FilterFactoryContext context, Void configuration) {
        return new ApiVersionsMarkingFilter();
    }

}
