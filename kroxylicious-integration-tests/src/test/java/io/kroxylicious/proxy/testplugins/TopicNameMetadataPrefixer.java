/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.testplugins;

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
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

/**
 * This filter exists to test that {@link FilterContext#topicNames(java.util.Collection)} is composable with Filters that
 * manipulate topic names in the Metadata Response.
 */
@Plugin(configType = Void.class)
public class TopicNameMetadataPrefixer implements FilterFactory<Void, Void> {

    public static final String PREFIX = "prefixed_";

    @Override
    public Void initialize(FilterFactoryContext context, Void config) throws PluginConfigurationException {
        return null;
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, Void initializationData) {
        return new TopicNamePrefixerFilter();
    }

    static class TopicNamePrefixerFilter implements MetadataResponseFilter {

        @Override
        public CompletionStage<ResponseFilterResult> onMetadataResponse(short apiVersion, ResponseHeaderData header, MetadataResponseData response,
                                                                        FilterContext context) {
            for (MetadataResponseData.MetadataResponseTopic topic : response.topics()) {
                if (topic.name() != null) {
                    topic.setName(PREFIX + topic.name());
                }
            }
            return context.forwardResponse(header, response);
        }
    }
}
