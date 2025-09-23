/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.KafkaErrorTopicNameLookupException;
import io.kroxylicious.proxy.filter.TopicNameLookupException;
import io.kroxylicious.proxy.filter.TopicNameResult;
import io.kroxylicious.proxy.tag.VisibleForTesting;

public class TopicNameRetriever {
    // Version 12 was the first version that uses topic ids.
    private static final short METADATA_API_VER_WITH_TOPIC_ID_SUPPORT = (short) 12;

    private final FilterContext filterContext;

    public TopicNameRetriever(FilterContext filterContext) {
        this.filterContext = filterContext;
    }

    public CompletionStage<Map<Uuid, TopicNameResult>> getTopicNames(Set<Uuid> topicIds) {
        Objects.requireNonNull(topicIds);
        return requestTopicMetadata(topicIds)
                .thenApply(f -> extractTopicNames(topicIds, f))
                .exceptionally(throwable -> topicIds.stream().collect(
                        Collectors.toMap(i -> i, i -> TopicNameResult.forException(new TopicNameLookupException("get topic names failed unexpectedly", throwable)))));
    }

    private CompletionStage<ApiMessage> requestTopicMetadata(Set<Uuid> topicIds) {
        MetadataRequestData request = new MetadataRequestData();
        request.setTopics(topicIds.stream().map(topicId -> new MetadataRequestData.MetadataRequestTopic().setTopicId(topicId)).toList());
        request.setAllowAutoTopicCreation(false);
        request.setIncludeClusterAuthorizedOperations(false);
        request.setIncludeTopicAuthorizedOperations(false);
        RequestHeaderData requestHeaderData = new RequestHeaderData();
        requestHeaderData.setRequestApiKey(ApiKeys.METADATA.id);
        requestHeaderData.setRequestApiVersion(METADATA_API_VER_WITH_TOPIC_ID_SUPPORT);
        return filterContext.sendRequest(requestHeaderData, request);
    }

    @VisibleForTesting
    static Map<Uuid, TopicNameResult> extractTopicNames(Set<Uuid> topicIds, ApiMessage metadataResponse) {
        if (metadataResponse instanceof MetadataResponseData d) {
            Errors errors = Errors.forCode(d.errorCode());
            if (errors != Errors.NONE) {
                return topicIds.stream().collect(Collectors.toMap(it -> it,
                        it -> TopicNameResult.forException(
                                new KafkaErrorTopicNameLookupException(errors, "MetadataResponse top level error: " + errors + ", " + errors.message()))));
            }
            else {
                return extractTopicNames(topicIds, d);
            }
        }
        else {
            return topicIds.stream().collect(Collectors.toMap(it -> it,
                    it -> TopicNameResult.forException(new TopicNameLookupException("Unexpected response type " + metadataResponse.getClass()))));
        }
    }

    private static Map<Uuid, TopicNameResult> extractTopicNames(Set<Uuid> topicIds, MetadataResponseData d) {
        Map<Uuid, TopicNameResult> results = d.topics().stream()
                .collect(Collectors.toMap(MetadataResponseData.MetadataResponseTopic::topicId, metadataResponseTopic -> {
                    Errors topicError = Errors.forCode(metadataResponseTopic.errorCode());
                    if (topicError != Errors.NONE) {
                        return TopicNameResult.forException(
                                new KafkaErrorTopicNameLookupException(topicError, "topic level error: " + topicError + ", " + topicError.message()));
                    }
                    else {
                        return TopicNameResult.forName(metadataResponseTopic.name());
                    }
                }));
        boolean allMapped = results.keySet().containsAll(topicIds);
        if (allMapped) {
            return results;
        }
        else {
            HashMap<Uuid, TopicNameResult> mutableResults = new HashMap<>(results);
            for (Uuid topicId : topicIds) {
                if (!mutableResults.containsKey(topicId)) {
                    mutableResults.put(topicId, TopicNameResult.forException(new TopicNameLookupException("Topic not found in Metadata response")));
                }
            }
            return mutableResults;
        }
    }
}
