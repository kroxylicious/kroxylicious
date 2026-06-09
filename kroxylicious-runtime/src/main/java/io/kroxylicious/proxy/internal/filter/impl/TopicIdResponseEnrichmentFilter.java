/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.filter.impl;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ResponseFilter;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.tag.ThreadSafe;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Enriches backend responses with topic names resolved from topicIds.
 * Installed per cluster-targeting route, closest to the terminal handler,
 * so it processes responses before per-route user filters.
 *
 * <p>The cache is shared across all connections to the same cluster.
 * This is safe because the filter sees raw backend responses before
 * any subject-dependent name transforms.</p>
 */
@ThreadSafe
public class TopicIdResponseEnrichmentFilter implements ResponseFilter {

    private final ConcurrentHashMap<Uuid, String> cache = new ConcurrentHashMap<>();

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
        switch (apiKey) {
            case METADATA -> learnAndEnrichMetadata((MetadataResponseData) response);
            case PRODUCE -> enrichProduce((ProduceResponseData) response);
            case FETCH -> enrichFetch((FetchResponseData) response);
            case OFFSET_COMMIT -> enrichOffsetCommit((OffsetCommitResponseData) response);
            case OFFSET_FETCH -> enrichOffsetFetch((OffsetFetchResponseData) response);
            case DELETE_TOPICS -> enrichDeleteTopics((DeleteTopicsResponseData) response);
            default -> {
            }
        }
        return context.forwardResponse(header, response);
    }

    private void learnAndEnrichMetadata(MetadataResponseData response) {
        if (response.topics() == null) {
            return;
        }
        for (var topic : response.topics()) {
            Uuid topicId = topic.topicId();
            String name = topic.name();
            if (topicId != null && !Uuid.ZERO_UUID.equals(topicId)
                    && name != null && !name.isEmpty()) {
                cache.put(topicId, name);
            }
        }
    }

    private void enrichProduce(ProduceResponseData response) {
        for (var topic : response.responses()) {
            enrichName(topic.topicId(), topic::setName);
        }
    }

    private void enrichFetch(FetchResponseData response) {
        for (var topic : response.responses()) {
            enrichName(topic.topicId(), topic::setTopic);
        }
    }

    private void enrichOffsetCommit(OffsetCommitResponseData response) {
        for (var topic : response.topics()) {
            enrichName(topic.topicId(), topic::setName);
        }
    }

    private void enrichOffsetFetch(OffsetFetchResponseData response) {
        for (var group : response.groups()) {
            for (var topic : group.topics()) {
                enrichName(topic.topicId(), topic::setName);
            }
        }
    }

    private void enrichDeleteTopics(DeleteTopicsResponseData response) {
        for (var topic : response.responses()) {
            if (topic.name() == null || topic.name().isEmpty()) {
                enrichName(topic.topicId(), topic::setName);
            }
        }
    }

    private void enrichName(Uuid topicId, java.util.function.Consumer<String> nameSetter) {
        if (topicId != null && !Uuid.ZERO_UUID.equals(topicId)) {
            String name = resolve(topicId);
            if (name != null) {
                nameSetter.accept(name);
            }
        }
    }

    @Nullable
    String resolve(Uuid topicId) {
        return cache.get(topicId);
    }
}
