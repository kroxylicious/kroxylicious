/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter;

import io.kroxylicious.proxy.filter.CompositePrefixingFixedClientIdFilter.CompositePrefixingFixedClientIdFilterConfig;
import io.kroxylicious.proxy.service.BaseContributor;
import io.kroxylicious.proxy.service.ContributorContext;

public class TestFilterContributor extends BaseContributor<KrpcFilter, ContributorContext> implements FilterContributor {

    public static final BaseContributorBuilder<KrpcFilter, ContributorContext> FILTERS = BaseContributor.<KrpcFilter, ContributorContext> builder()
            .add("FixedClientId", FixedClientIdFilter.FixedClientIdFilterConfig.class, FixedClientIdFilter::new)
            .add("RequestResponseMarking", RequestResponseMarkingFilter.RequestResponseMarkingFilterConfig.class, RequestResponseMarkingFilter::new)
            .add("OutOfBandSend", OutOfBandSendFilter.OutOfBandSendFilterConfig.class, OutOfBandSendFilter::new)
            .add("CompositePrefixingFixedClientId", CompositePrefixingFixedClientIdFilterConfig.class, CompositePrefixingFixedClientIdFilter::new)
            .add("RejectingCreateTopic", RejectingCreateTopicFilter.RejectingCreateTopicFilterConfig.class, RejectingCreateTopicFilter::new);

    public TestFilterContributor() {
        super(FILTERS);
    }
}
