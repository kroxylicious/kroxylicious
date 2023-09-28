/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import io.kroxylicious.proxy.filter.RejectingCreateTopicFilter.RejectingCreateTopicFilterConfig;

public class RejectingCreateTopicFilterFactory implements FilterFactory<RejectingCreateTopicFilter, RejectingCreateTopicFilterConfig> {

    @Override
    public Class<RejectingCreateTopicFilter> filterType() {
        return RejectingCreateTopicFilter.class;
    }

    @Override
    public Class<RejectingCreateTopicFilterConfig> configType() {
        return RejectingCreateTopicFilterConfig.class;
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
