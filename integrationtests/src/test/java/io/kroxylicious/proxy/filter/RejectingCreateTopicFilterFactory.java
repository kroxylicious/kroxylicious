/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import io.kroxylicious.proxy.filter.RejectingCreateTopicFilter.RejectingCreateTopicFilterConfig;

public class RejectingCreateTopicFilterFactory extends FilterFactory<RejectingCreateTopicFilter, RejectingCreateTopicFilterConfig> {

    public RejectingCreateTopicFilterFactory() {
        super(RejectingCreateTopicFilterConfig.class, RejectingCreateTopicFilter.class);
    }

    @Override
    public void validateConfiguration(RejectingCreateTopicFilterConfig config) {
        // null configuration is allowed, by default null config is invalid
    }

    @Override
    public RejectingCreateTopicFilter createFilter(FilterCreationContext context, RejectingCreateTopicFilterConfig configuration) {
        return new RejectingCreateTopicFilter(context, configuration);
    }
}
