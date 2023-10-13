/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.plugin.PluginConfigType;

@PluginConfigType(RejectingCreateTopicFilterFactory.RejectingCreateTopicFilterConfig.class)
public class RejectingCreateTopicFilterFactory
        implements FilterFactory<RejectingCreateTopicFilterFactory.RejectingCreateTopicFilterConfig, RejectingCreateTopicFilterFactory.RejectingCreateTopicFilterConfig> {

    @Override
    public RejectingCreateTopicFilterConfig initialize(FilterFactoryContext context, RejectingCreateTopicFilterConfig config) {
        // null configuration is allowed, by default null config is invalid
        return config;
    }

    @Override
    public RejectingCreateTopicFilter createFilter(FilterFactoryContext context, RejectingCreateTopicFilterConfig configuration) {
        return new RejectingCreateTopicFilter(context, configuration);
    }

    /**
     * @param withCloseConnection
     * If true, rejection will also close the connection */
    public record RejectingCreateTopicFilterConfig(boolean withCloseConnection, ForwardingStyle forwardingStyle) {

        @JsonCreator
        public RejectingCreateTopicFilterConfig(@JsonProperty(value = "withCloseConnection") boolean withCloseConnection,
                                                @JsonProperty(value = "forwardingStyle") ForwardingStyle forwardingStyle) {
            this.withCloseConnection = withCloseConnection;
            this.forwardingStyle = forwardingStyle == null ? ForwardingStyle.SYNCHRONOUS : forwardingStyle;
        }
    }
}
