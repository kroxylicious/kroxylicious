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

import io.netty.util.concurrent.ThreadAwareExecutor;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.metadata.TopLevelMetadataErrorException;
import io.kroxylicious.proxy.filter.metadata.TopicLevelMetadataErrorException;
import io.kroxylicious.proxy.filter.metadata.TopicNameMapping;
import io.kroxylicious.proxy.filter.metadata.TopicNameMappingException;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toMap;

/**
 * Retrieves topic names by sending an out-of-band Metadata request to the upstream cluster. Note that
 * this request/response traverses the Filter chain and so the observed topic names may be manipulated
 * by Filters upstream of the Filter context this is invoked from.
 */
final class MetadataRequestingTopicNameRetriever implements TopicNameRetriever {
    // Version 12 was the first version that uses topic ids.
    private static final short METADATA_API_VER_WITH_TOPIC_ID_SUPPORT = (short) 12;
    private static final MetadataRequestingTopicNameRetriever INSTANCE = new MetadataRequestingTopicNameRetriever();

    private MetadataRequestingTopicNameRetriever() {
    }

    public static TopicNameRetriever instance() {
        return INSTANCE;
    }

    @Override
    public CompletionStage<TopicNameMapping> topicNames(Collection<Uuid> topicIds, ThreadAwareExecutor filterDispatchExecutor, FilterContext filterContext) {
        Objects.requireNonNull(topicIds);
        if (topicIds.isEmpty()) {
            return InternalCompletableFuture.completedFuture(filterDispatchExecutor, MapTopicNameMapping.EMPTY).minimalCompletionStage();
        }
        CompletionStage<ApiMessage> apiMessageCompletionStage = requestTopicMetadata(topicIds, filterContext);
        return apiMessageCompletionStage
                .thenApply(message -> extractTopicNames(topicIds, message))
                .exceptionally(throwable -> mapAllIdsToException(topicIds, throwable));
    }

    private static TopicNameMapping mapAllIdsToException(Collection<Uuid> topicIds, Throwable throwable) {
        TopicNameMappingException exception;
        if (throwable instanceof CompletionException ce && ce.getCause() instanceof TopicNameMappingException mappingException) {
            exception = mappingException;
        }
        else {
            exception = new TopicNameMappingException(Errors.UNKNOWN_SERVER_ERROR, throwable.getMessage(), throwable);
        }
        return new MapTopicNameMapping(Map.of(), topicIds.stream().collect(toMap(i -> i, i -> exception)));
    }

    private CompletionStage<ApiMessage> requestTopicMetadata(Collection<Uuid> topicIds, FilterContext filterContext) {
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

}
