/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.Uuid;

/**
 * The result of an attempted topic id to topic name mapping. This is intended to enable different patterns for accessing the results.
 */
public interface TopicNameResult {
    /**
     * @return a CompletionStage that will be completed with a omplete mapping, with every requested topic id mapped to either an
     * {@link org.apache.kafka.common.protocol.Errors} or a name. The caller does not have to compose the CompletionStages, but it
     * will only be completed when all mappings are available.
     */
    CompletionStage<TopicNameMapping> topicNameMapping();

    /**
     * @return a Map from every requested topicId to a CompletionStage that will be completed with the topics name, else completed exceptionally.
     * The caller will likely need to compose the resulting Futures, but implementations will be free to complete the individual stages
     * when the name is available. If any stage is completed exceptionally, then the exception type will be a {@link TopicNameMappingException}
     */
    Map<Uuid, CompletionStage<String>> topicNames();
}
