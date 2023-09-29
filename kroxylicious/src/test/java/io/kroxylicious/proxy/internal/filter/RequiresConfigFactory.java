/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import io.kroxylicious.proxy.filter.FilterCreationContext;
import io.kroxylicious.proxy.filter.FilterFactory;

public class RequiresConfigFactory extends FilterFactory<RequiresConfigFilter, ExampleConfig> {

    public RequiresConfigFactory() {
        super(ExampleConfig.class, RequiresConfigFilter.class);
    }

    @Override
    public RequiresConfigFilter createFilter(FilterCreationContext context, ExampleConfig configuration) {
        return new RequiresConfigFilter(context, configuration, this.getClass());
    }
}
