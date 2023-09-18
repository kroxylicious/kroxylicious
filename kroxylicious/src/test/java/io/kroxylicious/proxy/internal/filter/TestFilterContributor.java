/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterConstructContext;
import io.kroxylicious.proxy.filter.FilterContributor;
import io.kroxylicious.proxy.service.BaseContributor;

public class TestFilterContributor extends BaseContributor<Filter, FilterConstructContext> implements FilterContributor {
    public static final String TYPE_NAME_A = "TEST1";
    public static final String TYPE_NAME_B = "TEST2";
    public static final String OPTIONAL_CONFIG_FILTER = "TEST3";
    public static final String REQUIRED_CONFIG_FILTER = "TEST4";
    public static final BaseContributorBuilder<Filter, FilterConstructContext> FILTERS = BaseContributor.<Filter, FilterConstructContext> builder()
            .add(TYPE_NAME_A, ExampleConfig.class, (context, exampleConfig) -> new TestFilter(TYPE_NAME_A, context, exampleConfig), true)
            .add(TYPE_NAME_B, ExampleConfig.class, (context, exampleConfig) -> new TestFilter(TYPE_NAME_B, context, exampleConfig), true)
            .add(REQUIRED_CONFIG_FILTER, ExampleConfig.class, (context, exampleConfig) -> new TestFilter(REQUIRED_CONFIG_FILTER, context, exampleConfig), true)
            .add(OPTIONAL_CONFIG_FILTER, ExampleConfig.class, (context, exampleConfig) -> new TestFilter(OPTIONAL_CONFIG_FILTER, context, exampleConfig), false);

    public TestFilterContributor() {
        super(FILTERS);
    }
}