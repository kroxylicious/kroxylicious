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
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

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
import io.kroxylicious.proxy.filter.TopicNameResult;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import static java.util.Collections.unmodifiableMap;

final class TopicNameRetriever {
    // Version 12 was the first version that uses topic ids.
    private static final short METADATA_API_VER_WITH_TOPIC_ID_SUPPORT = (short) 12;
    private final FilterContext filterContext;
    private final ExecutorService executor;

    TopicNameRetriever(FilterContext filterContext, ExecutorService executor) {
        this.filterContext = filterContext;
        this.executor = executor;
    }

    TopicNameResult getTopicNamesResult(Collection<Uuid> topicIds) {
        Objects.requireNonNull(topicIds);
        CompletionStage<ApiMessage> apiMessageCompletionStage = requestTopicMetadata(topicIds);
        return new Result(executor, apiMessageCompletionStage, topicIds);
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
            for (Uuid uuid : notFound) {
                failures.put(uuid, Errors.UNKNOWN_SERVER_ERROR);
            }
        }
        return new MapTopicNameMapping(unmodifiableMap(topicNames), unmodifiableMap(failures));
    }

    /**
     * The result of discovering the topic names for a collection of topic ids
     * @param topicNames successfully mapped topic names, non-null
     * @param failures failed topic name mappings, non-null
     */
    // TODO, currently if there is an unhandled exception at the top level we map that to an UNKNOWN_SERVER_ERROR, maybe it should be an Exception class not Errors
    private record MapTopicNameMapping(Map<Uuid, String> topicNames, Map<Uuid, Errors> failures) implements TopicNameMapping {

        private MapTopicNameMapping {
            Objects.requireNonNull(topicNames);
            Objects.requireNonNull(failures);
        }

        @Override
        public boolean anyFailures() {
            return !failures.isEmpty();
        }

    }

    private static class Result implements TopicNameResult {

        private final CompletionStage<TopicNameMapping> topicNameMappingStage;
        private final ExecutorService executorService;
        private final Map<Uuid, CompletionStage<String>> individualFutures;

        private Result(ExecutorService executorService, CompletionStage<ApiMessage> apiMessageCompletionStage, Collection<Uuid> topicIds) {
            this.executorService = executorService;
            topicNameMappingStage = apiMessageCompletionStage
                    .thenApply(message -> extractTopicNames(topicIds, message))
                    .exceptionally(
                            throwable -> new MapTopicNameMapping(Map.of(), topicIds.stream().collect(Collectors.toMap(i -> i, i -> Errors.UNKNOWN_SERVER_ERROR))));
            individualFutures = topicIds.stream().collect(Collectors.toMap(uuid -> uuid, uuid -> topicNameMapping().thenApply(m -> {
                if (m.failures().containsKey(uuid)) {
                    Errors errors = m.failures().get(uuid);
                    throw new TopicNameMappingException(errors.message(), errors.exception());
                }
                else if (m.topicNames().containsKey(uuid)) {
                    return m.topicNames().get(uuid);
                }
                else {
                    throw new TopicNameMappingException("no result for uuid " + uuid);
                }
            }).thenApplyAsync(mapping -> mapping, executorService)));
        }

        @Override
        public CompletionStage<TopicNameMapping> topicNameMapping() {
            return topicNameMappingStage.thenApplyAsync(mapping -> mapping, executorService);
        }

        @Override
        public Map<Uuid, CompletionStage<String>> topicNames() {
            return individualFutures;
        }
    }
}
