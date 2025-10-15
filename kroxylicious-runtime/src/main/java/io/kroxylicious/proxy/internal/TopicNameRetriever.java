/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.TopicNameMapping;
import io.kroxylicious.proxy.filter.TopicNameMappingException;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

import static java.util.Collections.unmodifiableMap;

record TopicNameRetriever(FilterContext filterContext) {
    // Version 12 was the first version that uses topic ids.
    private static final short METADATA_API_VER_WITH_TOPIC_ID_SUPPORT = (short) 12;

    public CompletionStage<TopicNameMapping> getTopicNames(Collection<Uuid> topicIds) {
        Objects.requireNonNull(topicIds);
        return requestTopicMetadata(topicIds)
                .thenApply(f -> extractTopicNames(topicIds, f))
                .handle(TopicNameRetriever::wrapUnhandledException);
    }

    private CompletionStage<ApiMessage> requestTopicMetadata(Collection<Uuid> topicIds) {
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
    static TopicNameMapping extractTopicNames(Collection<Uuid> topicIds, ApiMessage response) {
        if (response instanceof MetadataResponseData metadataResponse) {
            Errors errors = Errors.forCode(metadataResponse.errorCode());
            if (errors != Errors.NONE) {
                throw new TopicNameMappingException("getTopicNames Metadata response contained a top level Error code: " + errors.name(), errors.exception());
            }
            else {
                return doExtractTopicNames(topicIds, metadataResponse);
            }
        }
        else {
            throw new TopicNameMappingException("unexpected response type: " + response.getClass().getSimpleName());
        }
    }

    private static TopicNameMapping doExtractTopicNames(Collection<Uuid> topicIds, MetadataResponseData d) {
        Map<Uuid, String> topicNames = new HashMap<>();
        Map<Uuid, Errors> failures = new HashMap<>();
        d.topics().forEach(metadataResponseTopic -> {
            Errors topicError = Errors.forCode(metadataResponseTopic.errorCode());
            if (topicError == Errors.NONE) {
                topicNames.put(metadataResponseTopic.topicId(), metadataResponseTopic.name());
            }
            else {
                failures.put(metadataResponseTopic.topicId(), topicError);
            }
        });
        List<Uuid> notFound = topicIds.stream().filter(uuid -> !topicNames.containsKey(uuid) && !failures.containsKey(uuid)).toList();
        if (!notFound.isEmpty()) {
            throw new TopicNameMappingException("Not all requested uuids present in Metadata, missing uuids: " + notFound);
        }
        return new MapTopicNameMapping(unmodifiableMap(topicNames), unmodifiableMap(failures));
    }

    private static TopicNameMapping wrapUnhandledException(@Nullable TopicNameMapping topicNameMapping, @Nullable Throwable throwable) {
        if (topicNameMapping != null) {
            return topicNameMapping;
        }
        if (throwable != null) {
            if (throwable instanceof CompletionException ex && ex.getCause() instanceof TopicNameMappingException) {
                throw ex;
            }
            else {
                throw new TopicNameMappingException("getTopicNames resulted in unhandled exception", throwable);
            }
        }
        else {
            // should never happen, but for completeness
            throw new TopicNameMappingException("getTopicNames resulted in null mapping and throwable");
        }
    }

    /**
     * The result of discovering the topic names for a collection of topic ids
     * @param topicNames successfully mapped topic names, non-null
     * @param failures failed topic name mappings, non-null
     */
    public record MapTopicNameMapping(Map<Uuid, String> topicNames, Map<Uuid, Errors> failures) implements TopicNameMapping {

        public MapTopicNameMapping {
            Objects.requireNonNull(topicNames);
            Objects.requireNonNull(failures);
        }

        @Override
        public boolean anyFailures() {
            return !failures.isEmpty();
        }

    }
}
