/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.filter.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilter;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.internal.util.RequestHeaderTagger;

/**
 * Enriches client requests with topic names resolved from topicIds.
 * Per-connection instance (single-threaded on the event loop).
 * Works in tandem with {@link TopicIdResponseEnrichmentFilter}, which
 * enriches backend responses.
 *
 * <p>On the request path, resolves topicIds to names from a local cache.
 * On cache miss, sends an internal METADATA-by-topicId request through
 * the topology (which flows through the router and reaches backends),
 * waits for the response, caches the result, then continues.</p>
 *
 * <p>On the response path, learns topicId-to-name mappings from all
 * responses flowing back to the client. These are post-transform names
 * (per-route user filters have already run).</p>
 *
 * <p>The cache is per-connection to avoid poisoning from
 * subject-dependent name transforms in per-route filters.</p>
 */
public class TopicIdRequestEnrichmentFilter implements RequestFilter, ResponseFilter {

    private final Map<Uuid, String> cache;

    /**
     * @param cache shared cache that will also be read by
     *              {@link io.kroxylicious.proxy.router.RouterContext#topicName(Uuid)}
     */
    public TopicIdRequestEnrichmentFilter(Map<Uuid, String> cache) {
        this.cache = cache;
    }

    @Override
    public boolean shouldHandleRequest(ApiKeys apiKey, short apiVersion) {
        return switch (apiKey) {
            case PRODUCE -> apiVersion >= 13;
            case FETCH -> apiVersion >= 13;
            case OFFSET_COMMIT -> apiVersion >= 10;
            case OFFSET_FETCH -> apiVersion >= 10;
            case DELETE_TOPICS -> apiVersion >= 6;
            default -> false;
        };
    }

    @Override
    public boolean shouldHandleResponse(ApiKeys apiKey, short apiVersion) {
        return switch (apiKey) {
            case METADATA -> true;
            case PRODUCE -> apiVersion >= 13;
            case FETCH -> apiVersion >= 13;
            default -> false;
        };
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey,
                                                          short apiVersion,
                                                          RequestHeaderData header,
                                                          ApiMessage request,
                                                          FilterContext context) {
        List<Uuid> uncached = collectUncachedTopicIds(apiKey, request);
        if (uncached.isEmpty()) {
            enrichRequest(apiKey, request);
            return context.forwardRequest(header, request);
        }
        return resolveTopicIds(uncached, context).thenCompose(v -> {
            enrichRequest(apiKey, request);
            return context.forwardRequest(header, request);
        });
    }

    @Override
    public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey,
                                                            short apiVersion,
                                                            ResponseHeaderData header,
                                                            ApiMessage response,
                                                            FilterContext context) {
        learnFromResponse(apiKey, response);
        return context.forwardResponse(header, response);
    }

    private List<Uuid> collectUncachedTopicIds(ApiKeys apiKey, ApiMessage request) {
        var uncached = new ArrayList<Uuid>();
        switch (apiKey) {
            case PRODUCE -> {
                for (var td : ((ProduceRequestData) request).topicData()) {
                    addIfUncached(td.topicId(), uncached);
                }
            }
            case FETCH -> {
                var fr = (FetchRequestData) request;
                for (var topic : fr.topics()) {
                    addIfUncached(topic.topicId(), uncached);
                }
                for (var forgotten : fr.forgottenTopicsData()) {
                    addIfUncached(forgotten.topicId(), uncached);
                }
            }
            case OFFSET_COMMIT -> {
                for (var topic : ((OffsetCommitRequestData) request).topics()) {
                    addIfUncached(topic.topicId(), uncached);
                }
            }
            case OFFSET_FETCH -> {
                for (var group : ((OffsetFetchRequestData) request).groups()) {
                    if (group.topics() != null) {
                        for (var topic : group.topics()) {
                            addIfUncached(topic.topicId(), uncached);
                        }
                    }
                }
            }
            case DELETE_TOPICS -> {
                for (var topic : ((DeleteTopicsRequestData) request).topics()) {
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

    private void enrichRequest(ApiKeys apiKey, ApiMessage request) {
        switch (apiKey) {
            case PRODUCE -> {
                for (var td : ((ProduceRequestData) request).topicData()) {
                    setNameFromCache(td.topicId(), td::setName);
                }
            }
            case FETCH -> {
                var fr = (FetchRequestData) request;
                for (var topic : fr.topics()) {
                    setNameFromCache(topic.topicId(), topic::setTopic);
                }
                for (var forgotten : fr.forgottenTopicsData()) {
                    setNameFromCache(forgotten.topicId(), forgotten::setTopic);
                }
            }
            case OFFSET_COMMIT -> {
                for (var topic : ((OffsetCommitRequestData) request).topics()) {
                    setNameFromCache(topic.topicId(), topic::setName);
                }
            }
            case OFFSET_FETCH -> {
                for (var group : ((OffsetFetchRequestData) request).groups()) {
                    if (group.topics() != null) {
                        for (var topic : group.topics()) {
                            setNameFromCache(topic.topicId(), topic::setName);
                        }
                    }
                }
            }
            case DELETE_TOPICS -> {
                for (var topic : ((DeleteTopicsRequestData) request).topics()) {
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

    private void learnFromResponse(ApiKeys apiKey, ApiMessage response) {
        switch (apiKey) {
            case METADATA -> learnFromMetadata((MetadataResponseData) response);
            case PRODUCE -> {
                for (var topic : ((ProduceResponseData) response).responses()) {
                    learnMapping(topic.topicId(), topic.name());
                }
            }
            default -> {
            }
        }
    }

    private void learnFromMetadata(MetadataResponseData md) {
        if (md.topics() != null) {
            for (var topic : md.topics()) {
                learnMapping(topic.topicId(), topic.name());
            }
        }
    }

    private void learnMapping(Uuid topicId, String name) {
        if (topicId != null && !Uuid.ZERO_UUID.equals(topicId)
                && name != null && !name.isEmpty()) {
            cache.put(topicId, name);
        }
    }
}
