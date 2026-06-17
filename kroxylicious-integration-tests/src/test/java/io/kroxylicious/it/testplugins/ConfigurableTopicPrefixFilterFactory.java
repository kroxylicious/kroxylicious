/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it.testplugins;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.filter.MetadataResponseFilter;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.plugin.Plugin;

/**
 * A filter that prefixes topic names in METADATA responses with a configurable prefix.
 * Used for testing route-scoped topic name resolution.
 */
@Plugin(configType = ConfigurableTopicPrefixFilterFactory.Config.class)
public class ConfigurableTopicPrefixFilterFactory implements FilterFactory<ConfigurableTopicPrefixFilterFactory.Config, ConfigurableTopicPrefixFilterFactory.Config> {

    public record Config(String prefix) {}

    @Override
    public Config initialize(FilterFactoryContext context, Config config) {
        return config;
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, Config config) {
        return new PrefixFilter(config.prefix());
    }

    static class PrefixFilter implements MetadataResponseFilter {
        private final String prefix;

        PrefixFilter(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public CompletionStage<ResponseFilterResult> onMetadataResponse(short apiVersion, ResponseHeaderData header, MetadataResponseData response,
                                                                        FilterContext context) {
            for (MetadataResponseData.MetadataResponseTopic topic : response.topics()) {
                if (topic.name() != null) {
                    topic.setName(prefix + topic.name());
                }
            }
            return context.forwardResponse(header, response);
        }
    }
}
