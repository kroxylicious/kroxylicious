/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

public class ApiVersionsMarkingFilterFactory extends FilterFactory<ApiVersionsMarkingFilter, Void> {

    public ApiVersionsMarkingFilterFactory() {
        super(Void.class, ApiVersionsMarkingFilter.class);
    }

    @Override
    public ApiVersionsMarkingFilter createFilter(FilterCreationContext context, Void configuration) {
        return new ApiVersionsMarkingFilter();
    }
}
