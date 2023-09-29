/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import io.kroxylicious.proxy.filter.FilterCreationContext;
import io.kroxylicious.proxy.filter.FilterFactory;

public class TestFilterFactory extends FilterFactory<TestFilter, ExampleConfig> {

    public TestFilterFactory() {
        super(ExampleConfig.class, TestFilter.class);
    }

    @Override
    public TestFilter createFilter(FilterCreationContext context, ExampleConfig configuration) {
        return new TestFilter(context, configuration, this.getClass());
    }
}
