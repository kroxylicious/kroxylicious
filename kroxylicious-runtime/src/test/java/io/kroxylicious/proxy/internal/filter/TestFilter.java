/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;

public interface TestFilter {
    FilterFactoryContext getContext();

    ExampleConfig getExampleConfig();

    Class<? extends FilterFactory> getContributorClass();
}
