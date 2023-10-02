/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import org.jetbrains.annotations.NotNull;

import io.kroxylicious.proxy.filter.FilterCreationContext;
import io.kroxylicious.proxy.filter.FilterFactory;

import edu.umd.cs.findbugs.annotations.NonNull;

public class TestFilterFactory implements FilterFactory<TestFilter, ExampleConfig> {

    @NotNull
    @Override
    public Class<TestFilter> filterType() {
        return TestFilter.class;
    }

    @NonNull
    @Override
    public Class<ExampleConfig> configType() {
        return ExampleConfig.class;
    }

    @Override
    public TestFilter createFilter(FilterCreationContext context, ExampleConfig configuration) {
        return new TestFilter(context, configuration, this.getClass());
    }
}
