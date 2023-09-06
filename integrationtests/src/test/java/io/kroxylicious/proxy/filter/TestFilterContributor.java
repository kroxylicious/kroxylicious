/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter;

import io.kroxylicious.proxy.filter.CompositePrefixingFixedClientIdFilter.CompositePrefixingFixedClientIdFilterConfig;
import io.kroxylicious.proxy.service.BaseContributor;

public class TestFilterContributor extends BaseContributor<Filter> implements FilterContributor {

    public static final String REJECTING_CREATE_TOPIC = "RejectingCreateTopic";
    public static final BaseContributorBuilder<Filter> FILTERS = BaseContributor.<Filter> builder()
            .add("FixedClientId", FixedClientIdFilter.FixedClientIdFilterConfig.class, FixedClientIdFilter::new)
            .add("ApiVersionsMarkingFilter", ApiVersionsMarkingFilter::new)
            .add("RequestResponseMarking", RequestResponseMarkingFilter.RequestResponseMarkingFilterConfig.class, RequestResponseMarkingFilter::new)
            .add("OutOfBandSend", OutOfBandSendFilter.OutOfBandSendFilterConfig.class, OutOfBandSendFilter::new)
            .add("CompositePrefixingFixedClientId", CompositePrefixingFixedClientIdFilterConfig.class, CompositePrefixingFixedClientIdFilter::new)
            .add(REJECTING_CREATE_TOPIC, RejectingCreateTopicFilter.RejectingCreateTopicFilterConfig.class, RejectingCreateTopicFilter::new, false);

    public TestFilterContributor() {
        super(FILTERS);
    }
}
