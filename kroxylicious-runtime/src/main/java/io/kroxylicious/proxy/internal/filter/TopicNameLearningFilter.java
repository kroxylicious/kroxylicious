/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;

import io.kroxylicious.proxy.filter.CreateTopicsResponseFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.MetadataResponseFilter;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.model.VirtualClusterModel;

public record TopicNameLearningFilter(VirtualClusterModel model) implements MetadataResponseFilter, CreateTopicsResponseFilter {

    @Override
    public boolean shouldHandleMetadataResponse(short apiVersion) {
        // topic ids won't be set below version 12
        return apiVersion >= 12;
    }

    @Override
    public CompletionStage<ResponseFilterResult> onMetadataResponse(short apiVersion, ResponseHeaderData header, MetadataResponseData response, FilterContext context) {
        MetadataResponseData.MetadataResponseTopicCollection topics = response.topics();
        if (topics != null) {
            topics.forEach(topic -> rememberTopicName(topic::name, topic::topicId));
        }
        return context.forwardResponse(header, response);
    }

    @Override
    public boolean shouldHandleCreateTopicsResponse(short apiVersion) {
        // topic ids won't be set below version 7
        return apiVersion >= 7;
    }

    @Override
    public CompletionStage<ResponseFilterResult> onCreateTopicsResponse(short apiVersion, ResponseHeaderData header, CreateTopicsResponseData response,
                                                                        FilterContext context) {
        if (response.topics() != null) {
            response.topics().forEach(topic -> rememberTopicName(topic::name, topic::topicId));
        }
        return context.forwardResponse(header, response);
    }

    private void rememberTopicName(Supplier<String> topicNameSupplier, Supplier<Uuid> topicIdSupplier) {
        String topicName = topicNameSupplier.get();
        Uuid topicId = topicIdSupplier.get();
        if (topicId != null && !topicId.equals(Uuid.ZERO_UUID) && topicName != null && !topicName.isEmpty()) {
            model.rememberTopicIdMapping(topicId, topicName);
        }
    }

}
