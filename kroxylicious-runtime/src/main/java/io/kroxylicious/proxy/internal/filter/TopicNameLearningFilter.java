/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.MetadataResponseFilter;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.model.VirtualClusterModel;

public record TopicNameLearningFilter(VirtualClusterModel model) implements MetadataResponseFilter {
    @Override
    public CompletionStage<ResponseFilterResult> onMetadataResponse(short apiVersion, ResponseHeaderData header, MetadataResponseData response, FilterContext context) {
        MetadataResponseData.MetadataResponseTopicCollection topics = response.topics();
        if (topics != null) {
            topics.forEach(topic -> {
                String topicName = topic.name();
                Uuid topicId = topic.topicId();
                if (topicId != null && !topicId.equals(Uuid.ZERO_UUID) && topicName != null && !topicName.isEmpty()) {
                    model.rememberTopicIdMapping(topicId, topicName);
                }
            });
        }
        return context.forwardResponse(header, response);
    }
}
