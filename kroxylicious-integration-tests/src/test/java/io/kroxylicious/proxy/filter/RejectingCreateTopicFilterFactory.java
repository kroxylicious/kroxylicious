/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import io.kroxylicious.proxy.plugin.Plugin;

import edu.umd.cs.findbugs.annotations.NonNull;

@Plugin(configType = RejectingCreateTopicFilter.RejectingCreateTopicFilterConfig.class)
public class RejectingCreateTopicFilterFactory
                                               implements
                                               FilterFactory<RejectingCreateTopicFilter.RejectingCreateTopicFilterConfig, RejectingCreateTopicFilter.RejectingCreateTopicFilterConfig> {

    @Override
    public RejectingCreateTopicFilter.RejectingCreateTopicFilterConfig initialize(
            FilterFactoryContext context,
            RejectingCreateTopicFilter.RejectingCreateTopicFilterConfig config
    ) {
        // null configuration is allowed, by default null config is invalid
        return config;
    }

    @NonNull
    @Override
    public RejectingCreateTopicFilter createFilter(FilterFactoryContext context, RejectingCreateTopicFilter.RejectingCreateTopicFilterConfig configuration) {
        return new RejectingCreateTopicFilter(context, configuration);
    }

}
