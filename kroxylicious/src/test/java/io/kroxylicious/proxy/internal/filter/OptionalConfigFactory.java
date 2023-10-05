/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import io.kroxylicious.proxy.filter.FilterCreationContext;
import io.kroxylicious.proxy.filter.FilterFactory;

import edu.umd.cs.findbugs.annotations.NonNull;

public class OptionalConfigFactory implements FilterFactory<OptionalConfigFilter, ExampleConfig> {

    @NonNull
    @Override
    public Class<OptionalConfigFilter> filterType() {
        return OptionalConfigFilter.class;
    }

    @NonNull
    @Override
    public Class<ExampleConfig> configType() {
        return ExampleConfig.class;
    }

    @Override
    public void validateConfiguration(ExampleConfig config) {
        // any config object is valid
    }

    @Override
    public OptionalConfigFilter createFilter(FilterCreationContext context, ExampleConfig configuration) {
        return new OptionalConfigFilter(context, configuration, this.getClass());
    }
}
