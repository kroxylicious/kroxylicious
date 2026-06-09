/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.filter.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ResponseFilter;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.internal.util.RequestHeaderTagger;
import io.kroxylicious.proxy.tag.ThreadSafe;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Enriches backend responses with topic names resolved from topicIds.
 *
 * <p>On cache miss, sends an internal METADATA-by-topicId request
 * through the topology, waits for the response, caches the result,
 * then enriches and forwards the original response.</p>
 *
 * <p>The cache is shared across all connections (factory-scoped).
 * See {@link TopicIdRequestEnrichmentFilter} for cache poisoning
 * considerations with subject-dependent name transforms.</p>
 */
@ThreadSafe
public class TopicIdResponseEnrichmentFilter implements ResponseFilter {

    private final ConcurrentHashMap<Uuid, String> cache;

    /**
     * @param cache shared cache, typically factory-scoped
     */
    public TopicIdResponseEnrichmentFilter(ConcurrentHashMap<Uuid, String> cache) {
        this.cache = cache;
    }

    @Override
    public boolean shouldHandleResponse(ApiKeys apiKey, short apiVersion) {
        return switch (apiKey) {
            case METADATA -> true;
            case PRODUCE -> apiVersion >= 13;
            case FETCH -> apiVersion >= 13;
            case OFFSET_COMMIT -> apiVersion >= 10;
            case OFFSET_FETCH -> apiVersion >= 10;
            case DELETE_TOPICS -> apiVersion >= 6;
            default -> false;
        };
    }

    @Override
    public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey,
                                                            short apiVersion,
                                                            ResponseHeaderData header,
                                                            ApiMessage response,
                                                            FilterContext context) {
        if (apiKey == ApiKeys.METADATA) {
            learnFromMetadata((MetadataResponseData) response);
            return context.forwardResponse(header, response);
        }

        List<Uuid> uncached = collectUncachedTopicIds(apiKey, response);
        if (uncached.isEmpty()) {
            enrichResponse(apiKey, response);
            return context.forwardResponse(header, response);
        }

        return resolveTopicIds(uncached, context).thenCompose(v -> {
            enrichResponse(apiKey, response);
            return context.forwardResponse(header, response);
        });
    }

    private List<Uuid> collectUncachedTopicIds(ApiKeys apiKey, ApiMessage response) {
        var uncached = new ArrayList<Uuid>();
        switch (apiKey) {
            case PRODUCE -> {
                for (var topic : ((ProduceResponseData) response).responses()) {
                    addIfUncached(topic.topicId(), uncached);
                }
            }
            case FETCH -> {
                for (var topic : ((FetchResponseData) response).responses()) {
                    addIfUncached(topic.topicId(), uncached);
                }
            }
            case OFFSET_COMMIT -> {
                for (var topic : ((OffsetCommitResponseData) response).topics()) {
                    addIfUncached(topic.topicId(), uncached);
                }
            }
            case OFFSET_FETCH -> {
                for (var group : ((OffsetFetchResponseData) response).groups()) {
                    for (var topic : group.topics()) {
                        addIfUncached(topic.topicId(), uncached);
                    }
                }
            }
            case DELETE_TOPICS -> {
                for (var topic : ((DeleteTopicsResponseData) response).responses()) {
                    if (topic.name() == null || topic.name().isEmpty()) {
                        addIfUncached(topic.topicId(), uncached);
                    }
                }
            }
            default -> {
            }
        }
        return uncached;
    }

    private void addIfUncached(Uuid topicId, List<Uuid> uncached) {
        if (topicId != null && !Uuid.ZERO_UUID.equals(topicId) && !cache.containsKey(topicId)) {
            uncached.add(topicId);
        }
    }

    private CompletionStage<Void> resolveTopicIds(List<Uuid> topicIds,
                                                  FilterContext context) {
        var mdReq = new MetadataRequestData();
        mdReq.setAllowAutoTopicCreation(false);
        mdReq.setIncludeClusterAuthorizedOperations(false);
        mdReq.setIncludeTopicAuthorizedOperations(false);
        for (var id : topicIds) {
            mdReq.topics().add(new MetadataRequestData.MetadataRequestTopic().setTopicId(id));
        }
        var mdHeader = new RequestHeaderData();
        mdHeader.setRequestApiKey(ApiKeys.METADATA.id);
        mdHeader.setRequestApiVersion((short) 12);
        RequestHeaderTagger.tag(mdHeader, RequestHeaderTagger.Tag.LEARN_TOPIC_NAMES);
        return context.sendRequest(mdHeader, mdReq).thenAccept(response -> {
            if (response instanceof MetadataResponseData md) {
                learnFromMetadata(md);
            }
        });
    }

    private void enrichResponse(ApiKeys apiKey, ApiMessage response) {
        switch (apiKey) {
            case PRODUCE -> {
                for (var topic : ((ProduceResponseData) response).responses()) {
                    setNameFromCache(topic.topicId(), topic::setName);
                }
            }
            case FETCH -> {
                for (var topic : ((FetchResponseData) response).responses()) {
                    setNameFromCache(topic.topicId(), topic::setTopic);
                }
            }
            case OFFSET_COMMIT -> {
                for (var topic : ((OffsetCommitResponseData) response).topics()) {
                    setNameFromCache(topic.topicId(), topic::setName);
                }
            }
            case OFFSET_FETCH -> {
                for (var group : ((OffsetFetchResponseData) response).groups()) {
                    for (var topic : group.topics()) {
                        setNameFromCache(topic.topicId(), topic::setName);
                    }
                }
            }
            case DELETE_TOPICS -> {
                for (var topic : ((DeleteTopicsResponseData) response).responses()) {
                    if (topic.name() == null || topic.name().isEmpty()) {
                        setNameFromCache(topic.topicId(), topic::setName);
                    }
                }
            }
            default -> {
            }
        }
    }

    private void setNameFromCache(Uuid topicId, java.util.function.Consumer<String> nameSetter) {
        if (topicId != null && !Uuid.ZERO_UUID.equals(topicId)) {
            String name = cache.get(topicId);
            if (name != null) {
                nameSetter.accept(name);
            }
        }
    }

    private void learnFromMetadata(MetadataResponseData md) {
        if (md.topics() != null) {
            for (var topic : md.topics()) {
                Uuid topicId = topic.topicId();
                String name = topic.name();
                if (topicId != null && !Uuid.ZERO_UUID.equals(topicId)
                        && name != null && !name.isEmpty()) {
                    cache.put(topicId, name);
                }
            }
        }
    }

    @Nullable
    String resolve(Uuid topicId) {
        return cache.get(topicId);
    }
}
