/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter;

import io.kroxylicious.proxy.service.BaseContributor;

public class TestFilterContributor extends BaseContributor<KrpcFilter> implements FilterContributor {

    public static final BaseContributorBuilder<KrpcFilter> FILTERS = BaseContributor.<KrpcFilter> builder()
            .add("FixedClientId", FixedClientIdFilter.FixedClientIdFilterConfig.class, FixedClientIdFilter::new)
            .add("CreateTopicRejectFilter", CreateTopicRejectFilter::new)
            .add("RequestForwardDelaying", RequestForwardDelayingFilter::new)
            .add("ResponseForwardDelaying", ResponseForwardDelayingFilter::new);

    public TestFilterContributor() {
        super(FILTERS);
    }
}
