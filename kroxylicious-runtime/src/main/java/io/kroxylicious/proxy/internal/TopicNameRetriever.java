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
import io.kroxylicious.proxy.filter.metadata.TopLevelMetadataErrorException;
import io.kroxylicious.proxy.filter.metadata.TopicLevelMetadataErrorException;
import io.kroxylicious.proxy.filter.metadata.TopicNameMapping;
import io.kroxylicious.proxy.filter.metadata.TopicNameMappingException;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toMap;

final class TopicNameRetriever {
    // Version 12 was the first version that uses topic ids.
    private static final short METADATA_API_VER_WITH_TOPIC_ID_SUPPORT = (short) 12;
    private final FilterContext filterContext;

    TopicNameRetriever(FilterContext filterContext) {
        this.filterContext = filterContext;
    }

    CompletionStage<TopicNameMapping> topicNames(Collection<Uuid> topicIds) {
        Objects.requireNonNull(topicIds);
        CompletionStage<ApiMessage> apiMessageCompletionStage = requestTopicMetadata(topicIds);
        return apiMessageCompletionStage
                .thenApply(message -> extractTopicNames(topicIds, message))
                .exceptionally(throwable -> mapAllIdsToException(topicIds, throwable));
    }

    private static MapTopicNameMapping mapAllIdsToException(Collection<Uuid> topicIds, Throwable throwable) {
        TopicNameMappingException exception;
        if (throwable instanceof CompletionException ce && ce.getCause() instanceof TopicNameMappingException mappingException) {
            exception = mappingException;
        }
        else {
            exception = new TopicNameMappingException(Errors.UNKNOWN_SERVER_ERROR, throwable.getMessage(), throwable);
        }
        return new MapTopicNameMapping(Map.of(), topicIds.stream().collect(toMap(i -> i, i -> exception)));
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
            if (errors == Errors.NONE) {
                return doExtractTopicNames(topicIds, metadataResponse);
            }
            else {
                throw new TopLevelMetadataErrorException(errors);
            }
        }
        else {
            throw new TopicNameMappingException(Errors.UNKNOWN_SERVER_ERROR, "unexpected response type: " + response.getClass().getSimpleName());
        }
    }

    private static TopicNameMapping doExtractTopicNames(Collection<Uuid> topicIds, MetadataResponseData d) {
        Map<Uuid, String> topicNames = new HashMap<>(topicIds.size());
        Map<Uuid, TopicNameMappingException> failures = new HashMap<>();
        d.topics().forEach(metadataResponseTopic -> {
            Errors topicError = Errors.forCode(metadataResponseTopic.errorCode());
            if (topicError == Errors.NONE) {
                topicNames.put(metadataResponseTopic.topicId(), metadataResponseTopic.name());
            }
            else {
                failures.put(metadataResponseTopic.topicId(), new TopicLevelMetadataErrorException(topicError));
            }
        });
        List<Uuid> absentFromResponse = topicIds.stream().filter(uuid -> !topicNames.containsKey(uuid) && !failures.containsKey(uuid)).toList();
        if (!absentFromResponse.isEmpty()) {
            for (Uuid uuid : absentFromResponse) {
                failures.put(uuid, new TopicNameMappingException(Errors.UNKNOWN_SERVER_ERROR, "topic id metadata absent from response: " + uuid));
            }
        }
        return new MapTopicNameMapping(unmodifiableMap(topicNames), unmodifiableMap(failures));
    }

    /**
     * The result of discovering the topic names for a collection of topic ids
     * @param topicNames successfully mapped topic names, non-null
     * @param failures failed topic name mappings, non-null
     */
    private record MapTopicNameMapping(Map<Uuid, String> topicNames, Map<Uuid, TopicNameMappingException> failures) implements TopicNameMapping {

        private MapTopicNameMapping {
            Objects.requireNonNull(topicNames);
            Objects.requireNonNull(failures);
        }

        @Override
        public boolean anyFailures() {
            return !failures.isEmpty();
        }

    }
}
