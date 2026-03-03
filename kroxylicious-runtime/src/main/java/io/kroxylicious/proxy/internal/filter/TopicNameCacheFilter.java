/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataRequestData.MetadataRequestTopic;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;

import io.kroxylicious.proxy.config.CacheConfiguration;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.MetadataRequestFilter;
import io.kroxylicious.proxy.filter.MetadataResponseFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.internal.util.RequestHeaderTagger;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import static io.kroxylicious.proxy.internal.util.Metrics.VIRTUAL_CLUSTER_LABEL;

/**
 * A Filter that learns and caches all topic names, it is responsible for short circuit responding to internal
 * topic name retrievals.
 * <p>
 * Note that this is a special Filter in that a single instance is shared across all connections for a VirtualCluster
 * rather than an instance per connection. This means it can be invoked by multiple threads concurrently.
 */
@ThreadSafe
public class TopicNameCacheFilter implements MetadataRequestFilter, MetadataResponseFilter {
    @VisibleForTesting
    final Cache<Uuid, String> topicNameCache;

    public TopicNameCacheFilter(CacheConfiguration cacheConfiguration,
                                String clusterName) {
        this(cacheConfiguration, Map.of(), clusterName);
    }

    /**
     * @param topicNames initial topic names to populate the cache with
     */
    @VisibleForTesting
    public TopicNameCacheFilter(CacheConfiguration cacheConfiguration,
                                Map<Uuid, String> topicNames,
                                String clusterName) {
        Objects.requireNonNull(cacheConfiguration, "cacheConfiguration must not be null");
        Objects.requireNonNull(topicNames, "topicNames must not be null");
        this.topicNameCache = buildCache(cacheConfiguration);
        new CaffeineCacheMetrics<>(this.topicNameCache, "topicNames", List.of(Tag.of(VIRTUAL_CLUSTER_LABEL, clusterName))).bindTo(Metrics.globalRegistry);
        this.topicNameCache.putAll(topicNames);
    }

    private static Cache<Uuid, String> buildCache(CacheConfiguration cacheConfiguration) {
        Caffeine<Object, Object> cacheBuilder = Caffeine.newBuilder();
        cacheBuilder.recordStats();
        if (cacheConfiguration.maxSize() != null) {
            cacheBuilder.maximumSize(cacheConfiguration.maxSize());
        }
        Duration expireAfterWrite = cacheConfiguration.expireAfterWrite();
        if (expireAfterWrite != null) {
            cacheBuilder.expireAfterWrite(expireAfterWrite);
        }
        cacheBuilder.expireAfterAccess(cacheConfiguration.expireAfterAccess());
        return cacheBuilder.build();
    }

    @Override
    public CompletionStage<RequestFilterResult> onMetadataRequest(short apiVersion,
                                                                  RequestHeaderData header,
                                                                  MetadataRequestData request,
                                                                  FilterContext context) {
        if (RequestHeaderTagger.containsTag(header, RequestHeaderTagger.Tag.LEARN_TOPIC_NAMES)) {
            if (request.topics() != null && !request.topics().isEmpty()) {
                Map<Uuid, String> names = topicNameCache.getAllPresent(
                        request.topics().stream().map(MetadataRequestTopic::topicId).toList());
                if (request.topics().stream().allMatch(metadataRequestTopic -> names.containsKey(metadataRequestTopic.topicId()))) {
                    MetadataResponseData metadataResponseData = new MetadataResponseData();
                    request.topics().stream().map(metadataRequestTopic -> {
                        MetadataResponseTopic responseTopic = new MetadataResponseTopic();
                        responseTopic.setTopicId(metadataRequestTopic.topicId());
                        responseTopic.setName(names.get(metadataRequestTopic.topicId()));
                        return responseTopic;
                    }).forEach(metadataResponseData.topics()::add);
                    return context.requestFilterResultBuilder().shortCircuitResponse(metadataResponseData).completed();
                }
                else {
                    // we don't know all the topics so forward
                    RequestHeaderTagger.removeTags(header);
                    return context.forwardRequest(header, request);
                }
            }
            else {
                UnknownServerException exception = new UnknownServerException("received an internal topic name request with no topics");
                return context.requestFilterResultBuilder().errorResponse(header, request, exception).completed();
            }
        }
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onMetadataResponse(short apiVersion,
                                                                    ResponseHeaderData header,
                                                                    MetadataResponseData response,
                                                                    FilterContext context) {
        if (response.topics() != null) {
            response.topics().forEach(topic -> {
                if (topic.name() != null && !topic.name().isEmpty() && topic.topicId() != null && topic.topicId() != Uuid.ZERO_UUID) {
                    topicNameCache.put(topic.topicId(), topic.name());
                }
            });
        }
        return context.forwardResponse(header, response);
    }

    @VisibleForTesting
    Optional<String> topicName(Uuid topicId) {
        return Optional.ofNullable(topicNameCache.getIfPresent(topicId));
    }
}
