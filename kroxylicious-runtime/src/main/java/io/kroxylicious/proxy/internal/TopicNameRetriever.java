/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Collection;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.Uuid;

import io.netty.util.concurrent.ThreadAwareExecutor;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.metadata.TopicNameMapping;

/**
 * Retrieves topic names for topic ids.
 */
public interface TopicNameRetriever {
    /**
     * Attempt to retrieve topic names for topic ids
     * <h4>Chained Computation stages</h4>
     * <p>Default and asynchronous default computation stages chained to the returned
     * {@link java.util.concurrent.CompletionStage} are guaranteed to be executed by the filterDispatchExecutor.
     * @param topicIds ids to retrieve
     * @param filterDispatchExecutor filter dispatch executor
     * @param filterContext
     * @return stage mapping from all topicIds to either an id or a failure exception.
     */
    CompletionStage<TopicNameMapping> topicNames(Collection<Uuid> topicIds, ThreadAwareExecutor filterDispatchExecutor, FilterContext filterContext);
}
