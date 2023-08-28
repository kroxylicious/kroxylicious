/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.FilterContributor;
import io.kroxylicious.proxy.filter.KrpcFilter;
import io.kroxylicious.proxy.service.BaseContributor;

public class TestFilterContributor extends BaseContributor<KrpcFilter, FilterContext> implements FilterContributor {
    public static final String SHORT_NAME_A = "TEST1";
    public static final String SHORT_NAME_B = "TEST2";
    public static final BaseContributorBuilder<KrpcFilter, FilterContext> FILTERS = BaseContributor.<KrpcFilter, FilterContext> builder()
            .add(SHORT_NAME_A, ExampleConfig.class, (context, exampleConfig) -> new TestFilter(SHORT_NAME_A, context, exampleConfig))
            .add(SHORT_NAME_B, ExampleConfig.class, (context, exampleConfig) -> new TestFilter(SHORT_NAME_B, context, exampleConfig));

    public TestFilterContributor() {
        super(FILTERS);
    }
}
