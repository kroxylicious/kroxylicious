/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.apache.kafka.common.Uuid;

import io.netty.util.concurrent.ThreadAwareExecutor;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.metadata.TopicNameMapping;

import edu.umd.cs.findbugs.annotations.Nullable;

public class CachingTopicNameRetriever implements TopicNameRetriever {

    private static final Map<Uuid, String> EMPTY = Map.of();
    private final TopicNameRetriever delegate;
    private Map<Uuid, String> topicNameCache = EMPTY;

    private CachingTopicNameRetriever(TopicNameRetriever delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    public static TopicNameRetriever cachingRetriever(TopicNameRetriever delegate) {
        return new CachingTopicNameRetriever(delegate);
    }

    @Override
    public CompletionStage<TopicNameMapping> topicNames(Collection<Uuid> topicIds, ThreadAwareExecutor filterDispatchExecutor, FilterContext filterContext) {
        if (allIdsCached(topicIds)) {
            return buildResultFromCache(topicIds, filterDispatchExecutor);
        }
        else {
            return delegateAndCacheResult(topicIds, filterDispatchExecutor, filterContext);
        }
    }

    private boolean allIdsCached(Collection<Uuid> topicIds) {
        return topicIds.stream().allMatch(topicNameCache::containsKey);
    }

    private CompletionStage<TopicNameMapping> buildResultFromCache(Collection<Uuid> topicIds, ThreadAwareExecutor filterDispatchExecutor) {
        TopicNameMapping result;
        if (topicIds.isEmpty()) {
            result = MapTopicNameMapping.EMPTY;
        }
        else {
            Map<Uuid, String> names = topicIds.stream().collect(Collectors.toMap(topicId -> topicId, topicNameCache::get));
            result = new MapTopicNameMapping(names, Map.of());
        }
        return InternalCompletableFuture.completedFuture(filterDispatchExecutor, result).minimalCompletionStage();
    }

    private CompletionStage<TopicNameMapping> delegateAndCacheResult(Collection<Uuid> topicIds, ThreadAwareExecutor filterDispatchExecutor, FilterContext filterContext) {
        return delegate.topicNames(topicIds, filterDispatchExecutor, filterContext).whenComplete((topicNameMapping, throwable) -> {
            // this work is guaranteed to execute in the Filter Dispatch Thread, so can safely update a plain HashMap member
            maybeCacheTopicNames(topicNameMapping);
        });
    }

    private void maybeCacheTopicNames(@Nullable TopicNameMapping topicNameMapping) {
        if (topicNameMapping != null && !topicNameMapping.topicNames().isEmpty()) {
            if (topicNameCache == EMPTY) {
                topicNameCache = new HashMap<>();
            }
            topicNameCache.putAll(topicNameMapping.topicNames());
        }
    }
}
