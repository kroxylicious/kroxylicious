/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.metadata;

import java.util.Map;

/**
 * A response to a {@link DescribeTopicLabelsRequest} detailing the labels of each of the topics.
 * @param topicLabels
 */
public record DescribeTopicLabelsResponse(Map<String, Map<String, String>> topicLabels) implements ResourceMetadataResponse<DescribeTopicLabelsRequest> {

}
