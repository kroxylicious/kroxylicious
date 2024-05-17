/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.metadata;

import java.util.Collection;

/**
 * A request to describe the labels of the given topics.
 * @param topicNames
 */
public record DescribeTopicLabelsRequest(Collection<String> topicNames) implements ResourceMetadataRequest<DescribeTopicLabelsResponse> {}
