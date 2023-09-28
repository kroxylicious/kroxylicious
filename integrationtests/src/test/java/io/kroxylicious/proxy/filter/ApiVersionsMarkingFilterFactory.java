/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

public class ApiVersionsMarkingFilterFactory implements FilterFactory<ApiVersionsMarkingFilter, Void> {

    @Override
    public Class<ApiVersionsMarkingFilter> filterType() {
        return ApiVersionsMarkingFilter.class;
    }

    @Override
    public Class<Void> configType() {
        return Void.class;
    }

    @Override
    public ApiVersionsMarkingFilter createFilter(FilterCreationContext context, Void configuration) {
        return new ApiVersionsMarkingFilter();
    }
}
