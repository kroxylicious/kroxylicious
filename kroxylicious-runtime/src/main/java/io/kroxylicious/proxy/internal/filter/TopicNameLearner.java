/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.MetadataRequestFilter;
import io.kroxylicious.proxy.filter.MetadataResponseFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

public class TopicNameLearner implements MetadataRequestFilter, MetadataResponseFilter {

    public Map<Uuid, String> topicNameMap = new HashMap<>();
    public MetadataResponseData allTopics = new MetadataResponseData();

    @Override
    public CompletionStage<RequestFilterResult> onMetadataRequest(short apiVersion, RequestHeaderData header, MetadataRequestData request, FilterContext context) {
        List<Uuid> uuids = request.topics().stream().map(MetadataRequestData.MetadataRequestTopic::topicId)
                .flatMap(topicId -> Optional.ofNullable(topicId).stream())
                .toList();
        if (topicNameMap.keySet().containsAll(uuids)) {
            return context.requestFilterResultBuilder().shortCircuitResponse(allTopics).completed();
        }
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onMetadataResponse(short apiVersion, ResponseHeaderData header, MetadataResponseData response, FilterContext context) {
        response.topics().forEach(topic -> {
            if (topic.name() != null && topic.topicId() != null && !topicNameMap.containsKey(topic.topicId())) {
                topicNameMap.put(topic.topicId(), topic.name());
                allTopics.topics().add(topic.duplicate());
            }
        });
        return context.forwardResponse(header, response);
    }

}
